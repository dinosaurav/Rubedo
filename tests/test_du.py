"""rubedo du: ledger-derived storage report + reclaimable dry-run audit."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, run, step, pipeline
from rubedo.db import get_session
from rubedo.du import storage_report
from rubedo.models import Materialization
from rubedo.store import _get_object_path

TEST_FOLDER = ".test_du_data"
ENV_FOLDER = ".test_du_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    from rubedo.db import init_db

    init_db()

    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    from rubedo.store import init_store

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def make_shout_pipeline():
    @step(name="shout", version="1")
    def shout(path):
        return open(path).read().upper()

    return pipeline(id="du", name="du", folder=TEST_FOLDER, steps=[shout])


def test_sizes_and_counts_for_populated_store():
    # Outputs are text-serialized: "ALPHA" = 5 bytes, "HI" = 2 bytes.
    create_file("a.txt", "alpha")
    create_file("b.txt", "hi")
    summary = run(make_shout_pipeline(), workers=1)
    assert summary.created_count == 2

    report = storage_report()
    assert report.total_objects == 2
    assert report.total_bytes == 7  # hand-count: 5 + 2
    assert report.total_materializations == 2
    assert report.live_materializations == 2
    assert report.missing_objects == 0
    assert report.reclaimable_objects == 0
    assert report.reclaimable_bytes == 0

    (pipe,) = report.pipelines
    assert pipe.pipeline_id == "du"
    assert (pipe.objects, pipe.bytes, pipe.materializations) == (2, 7, 2)
    (usage,) = pipe.steps
    assert usage.step_name == "shout"
    assert (usage.objects, usage.bytes) == (2, 7)
    assert usage.live_materializations == 2

    # --json surface: plain dict, round-trippable
    d = report.to_dict()
    assert d["total_bytes"] == 7
    assert d["pipelines"][0]["steps"][0]["step_name"] == "shout"


def test_invalidated_only_object_is_reclaimable():
    create_file("a.txt", "alpha")
    run(make_shout_pipeline(), workers=1)

    res = invalidate(Selection(step="shout"), reason="test")
    assert res["invalidated_count"] == 1

    report = storage_report()
    assert report.total_objects == 1
    assert report.total_bytes == 5
    assert report.live_materializations == 0
    # The dry-run audit: nothing was deleted, the bytes are still there,
    # but zero live references means the object would be reclaimable.
    assert report.reclaimable_objects == 1
    assert report.reclaimable_bytes == 5


def test_shared_object_with_one_live_reference_is_not_reclaimable():
    """The 10b trap: the store dedupes identical bytes, so one physical
    object can back many materializations at different addresses.

    Cache identity is coordinate-free (address = hash(step, version,
    input_hash)), so two files with *identical* bytes collapse to a single
    materialization — no sharing there. Sharing needs different inputs that
    normalize to identical output bytes: "same" and "same\\n" strip to the
    same 4-byte object, giving two materializations (different addresses,
    different input hashes) over one physical object.
    """
    create_file("a.txt", "same")
    create_file("b.txt", "same\n")

    @step(name="norm", version="1")
    def norm(path):
        return open(path).read().strip()

    summary = run(pipeline(id="du", name="du", folder=TEST_FOLDER, steps=[norm]), workers=1)
    assert summary.created_count == 2

    report = storage_report()
    assert report.total_materializations == 2
    assert report.total_objects == 1  # deduped bytes: one physical object
    assert report.total_bytes == 4  # hand-count: len(b"same")

    with get_session() as session:
        addresses = {m.output_address for m in session.query(Materialization).all()}
        hashes = {m.output_content_hash for m in session.query(Materialization).all()}
    assert len(addresses) == 2  # distinct addresses...
    assert len(hashes) == 1  # ...sharing one object

    res = invalidate(Selection(coordinate_glob="a.txt"), reason="test")
    assert res["invalidated_count"] == 1

    report = storage_report()
    assert report.live_materializations == 1
    # One reference is dead, but the survivor keeps the object: NOT reclaimable.
    assert report.reclaimable_objects == 0
    assert report.reclaimable_bytes == 0
    assert report.total_bytes == 4

    # Kill the last live reference and the object becomes reclaimable.
    invalidate(Selection(coordinate_glob="b.txt"), reason="test")
    report = storage_report()
    assert report.reclaimable_objects == 1
    assert report.reclaimable_bytes == 4


def test_missing_object_file_is_reported_not_crashed():
    create_file("a.txt", "alpha")
    run(make_shout_pipeline(), workers=1)

    with get_session() as session:
        mat = session.query(Materialization).one()
        content_hash = str(mat.output_content_hash)
    os.remove(_get_object_path(content_hash))

    report = storage_report()  # must not raise
    assert report.missing_objects == 1
    assert report.total_objects == 1  # the ledger still names it
    assert report.total_bytes == 0
    # Absent bytes can't be reclaimed; the missing object stays out of the audit.
    assert report.reclaimable_objects == 0
    assert report.reclaimable_bytes == 0
    assert report.reclaimed_objects == 0  # not a deliberate deletion
    assert "missing" in str(report)


def test_reclaimed_object_reported_separately_from_missing():
    """A retention GC deletion is deliberate: du must call it *reclaimed*, not
    *missing* (corruption). The distinction is the object_reclamations log."""
    from rubedo.gc import gc

    # Three generations, no retention -> nothing auto-pruned.
    for content in ("alpha", "beta", "gamma"):
        create_file("a.txt", content)
        run(make_shout_pipeline(), workers=1)

    before = storage_report()
    assert before.total_objects == 3
    assert before.missing_objects == 0
    assert before.reclaimed_objects == 0

    # Budget-driven gc deletes the oldest object deliberately.
    done = gc(delete=True, max_bytes=before.total_bytes - 1)
    assert done.applied and len(done.reclaimed) >= 1

    after = storage_report()
    # The swept object is reclaimed, NOT missing (it was deleted on purpose).
    assert after.reclaimed_objects == len(done.reclaimed)
    assert after.reclaimed_bytes == done.reclaimed_bytes
    assert after.missing_objects == 0
    assert "reclaimed" in str(after)

    # A genuinely missing object still reads as missing, alongside the reclaimed.
    with get_session() as session:
        live = (
            session.query(Materialization).filter_by(is_live=True).first()
        )
        os.remove(_get_object_path(str(live.output_content_hash)))
    mixed = storage_report()
    assert mixed.missing_objects == 1
    assert mixed.reclaimed_objects == len(done.reclaimed)
