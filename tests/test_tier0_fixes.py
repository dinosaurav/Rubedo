"""Regression tests for the Tier 0 code-review fixes (notes/TODO.md B1..B7, H1).

One file on purpose: each test pins the acceptance criterion of one review
commit. Redistribute into the per-feature test files if they grow.
"""

import os
import shutil
import threading
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Filtered, Selection, invalidate, pipeline, step
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, Run, RunCoordinateStatus
from rubedo.selection import get_selection_materialization_ids
from rubedo.store import init_store, read_materialization_output

TEST_FOLDER = ".test_tier0_data"
ENV_FOLDER = ".test_tier0_env"


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

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


# --- B1: multi-parent map over disjoint parent lanes -----------------------


def test_disjoint_parent_lanes_raise_clear_error():
    @step
    def a():
        yield {"x": 1}

    @step
    def b():
        yield {"y": 2}

    @step
    def combine(a, b):
        return {"a": a, "b": b}

    pipe = pipeline(name="dj", steps=[a, b, combine])
    with pytest.raises(ValueError, match="disjoint lane sets"):
        pipe.run(workers=1)


def test_diamond_parents_still_run():
    create_file("a.txt", "Hello")

    @step
    def upper(scan):
        return scan["text"].upper()

    @step
    def lower(scan):
        return scan["text"].lower()

    @step
    def both(upper, lower):
        return {"u": upper, "l": lower}

    pipe = pipeline(name="dm", steps=[scan, upper, lower, both])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 4  # scan + upper + lower + both


# --- B3: a failed invalidation must not commit partial flips ---------------


def test_invalidate_failure_leaves_no_partial_flips(monkeypatch):
    create_file("a.txt", "1")
    create_file("b.txt", "2")

    @step
    def read(scan):
        return scan["text"]

    pipe = pipeline(name="inv", steps=[scan, read])
    pipe.run(workers=1)

    # Make the second session.get(Materialization, mat_id) inside _flip
    # raise — simulates a crash mid-invalidation.  The rollback must undo
    # the first flip (is_live=False + fulfilled=False).
    from rubedo.models import Materialization
    from sqlalchemy.orm import Session as ORMSession
    real_get = ORMSession.get
    calls = {"n": 0}

    def flaky_get(self, entity, primary_key, *args, **kwargs):
        if entity is Materialization:
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("boom mid-invalidation")
        return real_get(self, entity, primary_key, *args, **kwargs)

    monkeypatch.setattr(ORMSession, "get", flaky_get)
    with pytest.raises(RuntimeError, match="boom"):
        invalidate(Selection(step="read"), reason="partial-failure test")

    with get_session() as session:
        # The first flip happened before the failure; rollback must undo it
        assert session.query(Materialization).filter_by(is_live=False).count() == 0
        failed_run = session.query(Run).filter_by(kind="invalidate").one()
        assert failed_run.status == "failed"


# --- B4: selection returns unique ids; pipeline: scopes the query ----------


def test_selection_ids_unique_across_runs():
    create_file("a.txt", "1")
    create_file("b.txt", "2")

    @step
    def read(scan):
        return scan["text"]

    pipe = pipeline(name="uniq", steps=[scan, read])
    pipe.run(workers=1)
    pipe.run(workers=1)  # reuse: a second status row per materialization

    with get_session() as session:
        # Scoped to "read" (not scan too): the point under test is
        # uniqueness across the two runs' status rows, not the raw count.
        ids = get_selection_materialization_ids(
            session, Selection(coordinate_glob="*", step="read")
        )
    assert len(ids) == len(set(ids)) == 2


def test_selection_parse_pipeline_term():
    sel = Selection.parse("pipeline:px step:read")
    assert sel.pipeline_id == "px"
    assert sel.step == "read"


def test_invalidate_scoped_to_pipeline():
    create_file("a.txt", "1")

    @step(name="read", version="1")
    def read_v1(scan):
        return scan["text"]

    @step(name="read", version="2")
    def read_v2(scan):
        return scan["text"]

    pipeline(name="p1", steps=[scan, read_v1]).run(workers=1)
    pipeline(name="p2", steps=[scan, read_v2]).run(workers=1)

    # Scoped to step="read" too (not just pipeline): each pipeline now has
    # two steps (scan + read), so pipeline-only scoping would catch both.
    res = invalidate(
        Selection.parse("pipeline:p2 step:read"), reason="scope test"
    )
    assert res["invalidated_count"] == 1

    with get_session() as session:
        dead = session.query(Materialization).filter_by(is_live=False).one()
        assert dead.pipeline_id == "p2"


# --- B5: skip_cache parents of join/group_key are rejected (validated
# lazily on first `.spec` access) ---


def test_join_rejects_skip_cache_parent():
    @step(index=["k"])
    def left():
        return {"k": "x"}

    @step(skip_cache=True)
    def right(left):
        return left

    @step(
        depends_on=["left", "right"],
        join_on={"left": "k", "right": "k"},
    )
    def j(left, right):
        return {}

    with pytest.raises(ValueError, match="skip_cache parent"):
        pipeline(name="jz", steps=[left, right, j]).spec


def test_group_key_rejects_skip_cache_parent():
    @step
    def src():
        return {"g": "a"}

    @step(skip_cache=True)
    def u(src):
        return src

    @step(depends_on=["u"], group_key="g")
    def r(u):
        return {}

    with pytest.raises(ValueError, match="materialized parents"):
        pipeline(name="gz", steps=[src, u, r]).spec


# --- B6: expand may yield bytes payloads ------------------------------------


def test_expand_yields_bytes_and_reuses():
    path = create_file("a.txt", "alpha\nbeta")

    def make_pipe():
        # A headless param-fed root: this test is about a downstream expand
        # yielding bytes, not about folder scanning, so a single param-fed
        # lane keeps it simple.
        @step
        def read(params):
            return open(params["path"]).read()

        @step
        def chunks(read):
            for line in read.splitlines():
                yield line.encode("utf-8")

        @step
        def size(chunks):
            assert isinstance(chunks, bytes)
            return len(chunks)

        return pipeline(name="bx", steps=[read, chunks, size])

    params = {"path": path}
    s1 = make_pipe().run(params=params, workers=1)
    assert s1.failed_count == 0
    # read (1) + two bytes children + two size outputs; the anchor is not a lane
    assert s1.created_count == 5

    with get_session() as session:
        sizes = {
            read_materialization_output(
                session.get(Materialization, r.materialization_id)
            )
            for r in session.query(RunCoordinateStatus)
            .filter_by(step_name="size", status="created")
            .all()
        }
    assert sizes == {5, 4}  # alpha, beta

    s2 = make_pipe().run(params=params, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 5)


# --- B7: a filter-heavy run with one failure is not a failed run ------------


def test_failed_plus_filtered_run_is_completed_with_failures():
    create_file("good.txt", "keep")
    create_file("bad.txt", "explode")

    @step
    def gate(scan):
        if scan["text"] == "explode":
            raise RuntimeError("boom")
        return Filtered(reason="not wanted")

    pipe = pipeline(name="st", steps=[scan, gate])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    assert summary.filtered_count == 1
    assert summary.status == "completed_with_failures"


# --- H1: different ephemeral coordinates compute in parallel ----------------


def test_ephemeral_coords_compute_in_parallel():
    create_file("f1.txt", "a")
    create_file("f2.txt", "b")

    # Both lanes must be inside the skip_cache producer at the same time:
    # the run memo's lock guards only the per-key state, not producer()
    # itself, so different coordinates' producers must run concurrently.
    barrier = threading.Barrier(2, timeout=5)

    @step
    def read(scan):
        return scan["text"]

    @step(skip_cache=True)
    def util(read):
        barrier.wait()
        return read

    @step
    def out(util):
        return util

    pipe = pipeline(name="par", steps=[scan, read, util, out])
    summary = pipe.run()
    assert summary.failed_count == 0
    assert summary.created_count == 6  # 2 scan + 2 read + 2 out
