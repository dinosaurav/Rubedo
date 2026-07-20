"""Regression tests for the Tier 0 code-review fixes (notes/TODO.md B1..B7, H1).

One file on purpose: each test pins the acceptance criterion of one review
commit. Redistribute into the per-feature test files if they grow.
"""

import os
import shutil
import threading

import pytest

from rubedo import Filtered, Selection, invalidate, pipeline, step
from rubedo.models import InputHashUsage, Run, RunCoordinateStatus
from rubedo.planning import _ArrowRowRef
from conftest import make_home

TEST_FOLDER = ".test_tier0_data"
ENV_FOLDER = ".test_tier0_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    TEST_HOME = make_home(ENV_FOLDER)
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

    pipe = pipeline(name="dj", steps=[a, b, combine], home=TEST_HOME)
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

    pipe = pipeline(name="dm", steps=[scan, upper, lower, both], home=TEST_HOME)
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

    pipe = pipeline(name="inv", steps=[scan, read], home=TEST_HOME)
    pipe.run(workers=1)

    # Make the second InputHashUsage query inside _flip raise — simulates
    # a crash mid-invalidation.  The rollback must undo the first flip
    # (fulfilled=False).
    from rubedo.models import InputHashUsage
    from sqlalchemy.orm import Session as ORMSession
    real_query = ORMSession.query
    calls = {"n": 0}

    def flaky_query(self, entity, *args, **kwargs):
        if entity is InputHashUsage:
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("boom mid-invalidation")
        return real_query(self, entity, *args, **kwargs)

    monkeypatch.setattr(ORMSession, "query", flaky_query)
    with pytest.raises(RuntimeError, match="boom"):
        invalidate(Selection(step="read"), reason="partial-failure test", home=TEST_HOME)

    # Undo the flaky patch before assertions query InputHashUsage
    monkeypatch.undo()

    with TEST_HOME.session() as session:
        # The first flip happened before the failure; rollback must undo it
        assert session.query(InputHashUsage).filter(InputHashUsage.fulfilled.is_(False)).count() == 0
        failed_run = session.query(Run).filter_by(kind="invalidate").one()
        assert failed_run.status == "failed"


# --- B4: selection returns unique ids; pipeline: scopes the query ----------


def test_selection_ids_unique_across_runs():
    create_file("a.txt", "1")
    create_file("b.txt", "2")

    @step
    def read(scan):
        return scan["text"]

    pipe = pipeline(name="uniq", steps=[scan, read], home=TEST_HOME)
    pipe.run(workers=1)
    pipe.run(workers=1)  # reuse: a second status row per materialization

    with TEST_HOME.session() as session:
        # Scoped to "read" (not scan too): the point under test is
        # uniqueness across the two runs' status rows, not the raw count.
        from rubedo.selection import get_selection_addresses
        addrs = get_selection_addresses(
            session, Selection(coordinate_glob="*", step="read"), home=TEST_HOME
        )
    assert len(addrs) == len(set(addrs)) == 2


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

    pipeline(name="p1", steps=[scan, read_v1], home=TEST_HOME).run(workers=1)
    pipeline(name="p2", steps=[scan, read_v2], home=TEST_HOME).run(workers=1)

    # Scoped to step="read" too (not just pipeline): each pipeline now has
    # two steps (scan + read), so pipeline-only scoping would catch both.
    res = invalidate(
        Selection.parse("pipeline:p2 step:read"), reason="scope test"
    ,
        home=TEST_HOME,
    )
    assert res["invalidated_count"] == 1

    with TEST_HOME.session() as session:
        dead_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(False)).all()
        }
        assert len(dead_addrs) == 1
        dead_row = TEST_HOME.lanes.address_row_index().get(next(iter(dead_addrs)))
        assert dead_row is not None
        assert dead_row.get("pipeline_id") == "p2"


# --- B5: skip_cache parents of join/group_key are rejected (validated
# lazily on first `.spec` access) ---


def test_join_rejects_skip_cache_parent():
    @step
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
        pipeline(name="jz", steps=[left, right, j], home=TEST_HOME).spec


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
        pipeline(name="gz", steps=[src, u, r], home=TEST_HOME).spec


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

        return pipeline(name="bx", steps=[read, chunks, size], home=TEST_HOME)

    params = {"path": path}
    s1 = make_pipe().run(params=params, workers=1)
    assert s1.failed_count == 0
    # read (1) + two bytes children + two size outputs; the anchor is not a lane
    assert s1.created_count == 5

    with TEST_HOME.session() as session:
        addr_index = TEST_HOME.lanes.address_row_index()
        sizes = {
            TEST_HOME.store.read_materialization_output(_ArrowRowRef(row))
            for r in session.query(RunCoordinateStatus)
            .filter_by(step_name="size", status="created")
            .all()
            if (row := addr_index.get(str(r.output_address)))
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

    pipe = pipeline(name="st", steps=[scan, gate], home=TEST_HOME)
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

    pipe = pipeline(name="par", steps=[scan, read, util, out], home=TEST_HOME)
    summary = pipe.run()
    assert summary.failed_count == 0
    assert summary.created_count == 6  # 2 scan + 2 read + 2 out
