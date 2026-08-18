"""Regression tests for the Tier 0 code-review fixes (notes/TODO.md B1..B7, H1).

One file on purpose: each test pins the acceptance criterion of one review
commit. Redistribute into the per-feature test files if they grow.
"""

import os
import threading

import pytest

from rubedo import Filtered, Selection, invalidate, pipeline, step
from rubedo.models import InputHashUsage, Run
from conftest import isolated_test_env

TEST_FOLDER = ".test_tier0_data"
ENV_FOLDER = ".test_tier0_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("tier0") as env:
        TEST_HOME = env.home
        yield

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


# --- TODO 37: real per-row dep mixed with a singleton/broadcast dep --------


def test_broadcast_dep_mixed_with_per_row_dep_runs():
    @step
    def root():
        return {"threshold": 10}

    @step
    def source():
        for i in range(5):
            yield {"i": i}

    @step
    def combine(source: dict, root: dict):
        return source["i"] + root["threshold"]

    pipe = pipeline(name="broadcast", steps=[root, source, combine], home=TEST_HOME)
    summary = pipe.run(workers=2)
    assert summary.failed_count == 0
    assert summary.created_count == 11  # root + 5 source + 5 combine

    cells = TEST_HOME.current(resolve_output=True)
    outputs = sorted(c.output for c in cells if c.step_name == "combine")
    assert outputs == [10, 11, 12, 13, 14]

    # Second run: everything reused, no re-raise, no re-materialization.
    summary2 = pipe.run(workers=2)
    assert summary2.failed_count == 0
    assert summary2.created_count == 0
    assert summary2.reused_count == 11


def test_broadcast_deps_no_cross_root_bleed():
    @step
    def root_a():
        return {"v": "A"}

    @step
    def root_b():
        return {"v": "B"}

    @step
    def source():
        for i in range(3):
            yield {"i": i}

    @step
    def combine(source: dict, root_a: dict, root_b: dict):
        return f"{source['i']}-{root_a['v']}-{root_b['v']}"

    pipe = pipeline(name="multiroot", steps=[root_a, root_b, source, combine], home=TEST_HOME)
    summary = pipe.run(workers=2)
    assert summary.failed_count == 0

    cells = TEST_HOME.current(resolve_output=True)
    outputs = sorted(c.output for c in cells if c.step_name == "combine")
    assert outputs == ["0-A-B", "1-A-B", "2-A-B"]


def test_broadcast_dep_transitive_chain():
    """singleton_coordinate_steps must propagate past a direct root — a
    map step two hops from the root is still singleton."""

    @step
    def root():
        return {"threshold": 10}

    @step
    def scaled(root: dict):
        return {"threshold": root["threshold"] * 2}

    @step
    def source():
        for i in range(3):
            yield {"i": i}

    @step
    def combine(source: dict, scaled: dict):
        return source["i"] + scaled["threshold"]

    pipe = pipeline(name="broadcast-chain", steps=[root, scaled, source, combine], home=TEST_HOME)
    summary = pipe.run(workers=2)
    assert summary.failed_count == 0

    cells = TEST_HOME.current(resolve_output=True)
    outputs = sorted(c.output for c in cells if c.step_name == "combine")
    assert outputs == [20, 21, 22]


def test_broadcast_dep_ungrouped_aggregate():
    """An aggregate with no group_key is singleton too (always one "@all"
    group) — must be usable as a broadcast dep, same as a root."""

    @step
    def source():
        for i in range(4):
            yield {"i": i}

    @step(name="total", depends_on=["source"], shape="aggregate")
    def total_step(source):
        return sum(v["i"] for v in source.values())

    @step
    def combine(source: dict, total: int):
        return source["i"] - total

    pipe = pipeline(name="broadcast-agg", steps=[source, total_step, combine], home=TEST_HOME)
    summary = pipe.run(workers=2)
    assert summary.failed_count == 0

    cells = TEST_HOME.current(resolve_output=True)
    outputs = sorted(c.output for c in cells if c.step_name == "combine")
    assert outputs == [-6, -5, -4, -3]  # total = 0+1+2+3 = 6


def test_broadcast_dep_dry_plan_no_raise():
    """plan()'s own copy of singleton_steps (runner.py, separate from
    _RunContext) must resolve the broadcast dep the same way run() does."""

    @step
    def root():
        return {"threshold": 10}

    @step
    def source():
        for i in range(3):
            yield {"i": i}

    @step
    def combine(source: dict, root: dict):
        return source["i"] + root["threshold"]

    pipe = pipeline(name="broadcast-plan", steps=[root, source, combine], home=TEST_HOME)
    pipe.run(workers=2)
    plan = pipe.plan()
    actions = {i.step_name for i in plan.items}
    assert "combine" in actions
    assert plan.counts.get("blocked", 0) == 0


def test_broadcast_dep_survives_scoped_partial_run():
    """combine is a coordinate-preserving descendant of the real per-row
    anchor (scope.coordinate_preserving_scope_steps) — it plans with
    lanes=[...] instead of lanes=None, exercising the branch of _plan_step
    the whole-run tests above never touch."""
    from rubedo import RunScope

    @step
    def root():
        return {"threshold": 10}

    @step
    def source():
        for i in range(5):
            yield {"i": i}

    @step
    def combine(source: dict, root: dict):
        return source["i"] + root["threshold"]

    pipe = pipeline(name="broadcast-scope", steps=[root, source, combine], home=TEST_HOME)
    baseline = pipe.run(workers=1)
    source_cells = baseline.cells("source", resolve_output=True)
    assert len(source_cells) == 5

    scope = RunScope.explicit(
        anchor="combine", lanes=[c.coordinate for c in source_cells[:2]]
    )
    trial = pipe.run(scope=scope, targets=["combine"], workers=1)
    assert trial.kind == "partial"
    combine_cells = trial.cells("combine", resolve_output=True)
    assert len(combine_cells) == 2
    scoped_i = {c.output - 10 for c in combine_cells}
    expected_i = {
        next(sc for sc in source_cells if sc.coordinate == c.coordinate).output["i"]
        for c in combine_cells
    }
    assert scoped_i == expected_i


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
        dead_cells = TEST_HOME.select(f"address:{next(iter(dead_addrs))} live:false")
        assert len(dead_cells) == 1
        assert dead_cells[0].pipeline_id == "p2"


# --- B5: skip_cache parents of join/group_key are rejected (validated
# lazily on first `.spec` access) ---


def test_join_rejects_skip_cache_parent():
    @step
    def left():
        return {"k": "x"}

    @step(use_cache=False)
    def right(left):
        return left

    @step(
        depends_on=["left", "right"],
        join_on={"left": "k", "right": "k"},
    )
    def j(left, right):
        return {}

    with pytest.raises(ValueError, match="use_cache=False parent"):
        pipeline(name="jz", steps=[left, right, j], home=TEST_HOME).spec


def test_group_key_rejects_skip_cache_parent():
    @step
    def src():
        return {"g": "a"}

    @step(use_cache=False)
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

    sizes = set(s1.output_for("size", home=TEST_HOME).values())
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

    @step(use_cache=False)
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
