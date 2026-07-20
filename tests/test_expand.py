import os
import shutil

import pytest

from rubedo import step, pipeline
from rubedo.models import MaterializationEdge, RunCoordinateStatus, RunEvent
from rubedo.planning import _ArrowRowRef
from conftest import make_home

TEST_FOLDER = ".test_expand_data"
ENV_FOLDER = ".test_expand_env"

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
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def assert_run(pipe):
    summary = pipe.run(workers=1)
    if summary.failed_count > 0:
        with TEST_HOME.session() as session:
            events = (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            )
            for e in events:
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


def _scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""

    @step(check_cache=False)
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    return scan


# scan (root expand) -> read (map) -> split (expand, one lane per line) -> shout (map)
def _read():
    @step
    def read(scan):
        return scan["text"]

    return read


def _split():
    @step
    def split(read):
        for line in read.splitlines():
            yield {"line": line}  # yield payloads; content-addressed lanes

    return split


def _shout():
    @step
    def shout(split):
        return split["line"].upper()

    return shout


def make_pipe():
    return pipeline(
        name="x",
        steps=[_scan(), _read(), _split(), _shout()],
    
        home=TEST_HOME,
    )


def test_expand_mints_child_lanes_and_chains_downstream():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    s1 = assert_run(pipe)
    # scan: 2 files, read: 2 files, split: 3 minted lines, shout: 3 lines
    assert (s1.created_count, s1.reused_count) == (10, 0)

    with TEST_HOME.session() as session:
        shout = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="shout", status="created")
            .all()
        )
        assert len(shout) == 3  # alpha, beta, gamma → 3 content-addressed lanes
        addr_index = TEST_HOME.lanes.address_row_index()
        values = {
            TEST_HOME.store.read_materialization_output(_ArrowRowRef(row))
            for r in shout
            if (row := addr_index.get(str(r.output_address)))
        }
        assert values == {"ALPHA", "BETA", "GAMMA"}

        # Lineage: 2 scan->read edges + 3 read->split edges + 3 split->shout edges
        assert session.query(MaterializationEdge).count() == 8


def test_expand_reruns_but_reuses_identical_children():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    assert_run(pipe)

    # Nothing changed: expand re-executes (no MVP caching) but yields identical
    # bytes, so every materialization is reused.
    s2 = assert_run(pipe)
    assert (s2.created_count, s2.reused_count) == (0, 10)


def test_expand_caches_anchor_and_skips_fn_on_rerun():
    create_file("a.txt", "alpha\nbeta")
    calls = []

    @step
    def split(read):
        calls.append(1)  # side effect proves whether the fn re-runs
        for line in read.splitlines():
            yield {"line": line}

    @step
    def shout(split):
        return split["line"].upper()

    pipe = pipeline(
        name="c", steps=[_scan(), _read(), split, shout]
    ,
        home=TEST_HOME,
    )
    assert_run(pipe)
    assert len(calls) == 1  # scraped once

    # Unchanged parent: the anchor cache-hits, children replay, fn NOT re-run.
    s2 = assert_run(pipe)
    assert len(calls) == 1
    # scan(1) + read(1) + 2 split children + 2 shout children, all reused
    assert (s2.created_count, s2.reused_count) == (0, 6)

    # Change the parent: anchor address moves, so the expand re-runs.
    create_file("a.txt", "alpha\nGAMMA")
    assert_run(pipe)
    assert len(calls) == 2


def test_expand_reacts_to_a_changed_source_lane():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    assert_run(pipe)

    # Edit b.txt: only b's lane recomputes (scan(b) + read(b) + its one
    # minted line + shout); a's lanes stay reused.
    create_file("b.txt", "GAMMA")
    s = assert_run(pipe)
    assert s.created_count == 4  # scan(b), read(b), split child b/0, shout b/0
    assert s.reused_count == 6  # scan(a), read(a), a/0, a/1 for split and shout


def test_expand_plan_marks_downstream_pending():
    create_file("a.txt", "alpha\nbeta")
    pipe = make_pipe()
    rp = pipe.plan()
    # Downstream of an unexecuted expand can't be enumerated: it's pending.
    actions = {it.step_name: it.action for it in rp.items}
    assert actions.get("shout") == "pending"


def test_expand_identical_payloads_collapse():
    create_file("a.txt", "x")

    @step
    def dup(read):
        yield {"v": 1}
        yield {"v": 1}  # identical payload — collapses to one lane
        yield {"v": 2}

    pipe = pipeline(
        name="d", steps=[_scan(), _read(), dup]
    ,
        home=TEST_HOME,
    )
    assert_run(pipe)
    with TEST_HOME.session() as session:
        lanes = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="dup")
            .filter(RunCoordinateStatus.status.in_(["created", "reused"]))
            .all()
        )
    assert len(lanes) == 2  # {v:1} and {v:2}; the duplicate collapsed


def test_source_decorator():
    calls = []

    @step
    def things():
        calls.append(1)  # a source-shaped root; reuses on re-run
        for x in ["a", "b", "c"]:
            yield {"x": x}

    @step
    def up(things):
        return things["x"].upper()

    pipe = pipeline(name="s", steps=[things, up], home=TEST_HOME)  # a bare @step root
    s = assert_run(pipe)
    assert (s.created_count, s.reused_count) == (6, 0)  # 3 things + 3 up
    assert calls == [1]

    s2 = assert_run(pipe)
    assert (s2.created_count, s2.reused_count) == (0, 6)
    assert calls == [1]  # reused via anchor — generator not re-run

    with TEST_HOME.session() as session:
        addr_index = TEST_HOME.lanes.address_row_index()
        vals = {
            TEST_HOME.store.read_materialization_output(_ArrowRowRef(row))
            for r in session.query(RunCoordinateStatus)
            .filter_by(step_name="up")
            .filter(RunCoordinateStatus.status.in_(["created", "reused"]))
            .all()
            if (row := addr_index.get(str(r.output_address)))
        }
    assert vals == {"A", "B", "C"}


def test_expand_at_most_one_parent():
    # A parentless expand is valid now — it's a root (a source).
    step(name="root", shape="expand")(lambda: None)
    # Two or more parents would be a join, not an expand.
    with pytest.raises(ValueError, match="at most one parent"):
        step(name="bad", depends_on=["a", "b"], shape="expand")(
            lambda a, b: None
        )


def test_root_expand_is_a_source():
    # A root expand and no source= at all: the expand yields the initial lanes.
    @step
    def rows():
        for i in range(3):
            yield {"n": i}

    @step
    def double(rows):
        return rows["n"] * 2

    pipe = pipeline(name="r", steps=[rows, double], home=TEST_HOME)  # no source
    s = assert_run(pipe)
    assert (s.created_count, s.reused_count) == (6, 0)  # 3 rows + 3 double

    # Re-run: the root re-executes (re-scan) but yields identical payloads, so
    # every lane reuses — a source's behavior.
    s2 = assert_run(pipe)
    assert (s2.created_count, s2.reused_count) == (0, 6)

    with TEST_HOME.session() as session:
        addr_index = TEST_HOME.lanes.address_row_index()
        vals = {
            TEST_HOME.store.read_materialization_output(_ArrowRowRef(row))
            for r in session.query(RunCoordinateStatus)
            .filter_by(step_name="double")
            .filter(RunCoordinateStatus.status.in_(["created", "reused"]))
            .all()
            if (row := addr_index.get(str(r.output_address)))
        }
    assert vals == {0, 2, 4}


def test_expand_rejects_skip_cache():
    with pytest.raises(ValueError, match="skip_cache is not supported"):
        step(name="bad", depends_on=["a"], shape="expand", skip_cache=True)(
            lambda a: None
        )
