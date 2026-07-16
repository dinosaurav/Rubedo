"""rubedo du: ledger-derived storage report + reclaimable dry-run audit."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline
from rubedo.db import get_session
from rubedo.du import storage_report
from rubedo.models import InputHashUsage
from rubedo import lane_store
from rubedo.store import _get_object_path, read_materialization_output

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
    # Single-step expand root that reads and transforms in the same
    # generator — this keeps the step's own output content-address exactly
    # hand-countable (no extra scan-step materialization inflating the byte
    # totals below).
    @step
    def shout():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield open(path).read().upper()

    return pipeline(name="du", steps=[shout])


def test_sizes_and_counts_for_populated_store():
    # Outputs are text-serialized: "ALPHA" = 5 bytes, "HI" = 2 bytes.
    create_file("a.txt", "alpha")
    create_file("b.txt", "hi")
    summary = make_shout_pipeline().run(workers=1)
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
    make_shout_pipeline().run(workers=1)

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

    This needs a two-step scan -> norm chain (not the single-step shout
    root above): the scan step's own payload includes "path", so its two
    lanes stay distinct even though their post-strip content converges —
    exactly the same-object-different-address case under test.
    """
    create_file("a.txt", "same")
    create_file("b.txt", "same\n")

    @step(index=["path"])
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def norm(scan):
        return scan["text"].strip()

    summary = pipeline(name="du", steps=[scan, norm]).run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 4  # 2 files x (scan + norm)

    report = storage_report()
    assert report.total_materializations == 4  # 2 scan + 2 norm
    # norm's two materializations dedupe to one physical object; scan's two
    # (different "path", so different content, JSON-serialized) add two
    # more distinct objects.
    assert report.total_objects == 3
    (scan_usage,) = [
        s for s in report.pipelines[0].steps if s.step_name == "scan"
    ]
    (norm_usage,) = [
        s for s in report.pipelines[0].steps if s.step_name == "norm"
    ]
    assert norm_usage.objects == 1  # deduped: one shared "same" object
    assert norm_usage.bytes == len("same")
    assert report.total_bytes == scan_usage.bytes + norm_usage.bytes

    with get_session():
        norm_rows = [r for r in lane_store.all_filled_rows() if r.get("step_name") == "norm"]
        addresses = {r.get("address") for r in norm_rows}
        hashes = {r.get("content_hash") for r in norm_rows}
    assert len(addresses) == 2  # distinct addresses...
    assert len(hashes) == 1  # ...sharing one object

    def coordinate_for_path(path_value):
        from rubedo.planning import _ArrowRowRef

        with get_session() as session:
            for r in [row for row in lane_store.all_filled_rows() if row.get("step_name") == "scan"]:
                if read_materialization_output(_ArrowRowRef(r)).get("path") == path_value:
                    from rubedo.models import RunCoordinateStatus

                    rc = (
                        session.query(RunCoordinateStatus)
                        .filter_by(step_name="scan", output_address=r.get("address"))
                        .first()
                    )
                    return rc.coordinate
        return None

    coord_a = coordinate_for_path("a.txt")
    res = invalidate(
        Selection(coordinate_glob=coord_a, step="norm"), reason="test"
    )
    assert res["invalidated_count"] == 1

    report = storage_report()
    norm_live = [
        m
        for m in report.pipelines[0].steps
        if m.step_name == "norm"
    ][0]
    assert norm_live.live_materializations == 1
    # One reference is dead, but the survivor keeps the object: NOT reclaimable.
    assert report.reclaimable_objects == 0
    assert report.reclaimable_bytes == 0

    # Kill the last live reference and the object becomes reclaimable.
    coord_b = coordinate_for_path("b.txt")
    invalidate(Selection(coordinate_glob=coord_b, step="norm"), reason="test")
    report = storage_report()
    assert report.reclaimable_objects == 1
    assert report.reclaimable_bytes == 4


def test_missing_object_file_is_reported_not_crashed():
    create_file("a.txt", "alpha")
    make_shout_pipeline().run(workers=1)

    with get_session():
        rows = lane_store.all_filled_rows()
        assert len(rows) == 1
        content_hash = str(rows[0].get("content_hash"))
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

    # Three distinct-content runs -> three distinct content-addressed
    # lanes/materializations (an edited file is removed+added, not a
    # generation of a stable coordinate) -> no retention configured, so
    # nothing auto-pruned.
    for content in ("alpha", "beta", "gamma"):
        create_file("a.txt", content)
        make_shout_pipeline().run(workers=1)

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
        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True)).all()
        }
    idx = lane_store.address_row_index()
    live_row = next(idx[a] for a in fulfilled_addrs if a in idx)
    os.remove(_get_object_path(str(live_row.get("content_hash"))))
    mixed = storage_report()
    assert mixed.missing_objects == 1
    assert mixed.reclaimed_objects == len(done.reclaimed)
