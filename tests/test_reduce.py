import os
import json

import pytest

from conftest import isolated_test_env
from rubedo import step, pipeline, Filtered
from rubedo.models import (
    InputHashUsage,
    MaterializationEdge,
    RunCoordinateStatus,
    RunEvent,
)

TEST_FOLDER = ".test_reduce_data"
ENV_FOLDER = ".test_reduce_env"

TEST_HOME = None

@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("reduce") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)

def assert_run(pipe):
    summary = pipe.run(workers=1)
    if summary.failed_count > 0:
        with TEST_HOME.session() as session:
            events = session.query(RunEvent).filter_by(run_id=summary.run_id, level="error").all()
            for e in events:
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


@step(check_cache=False)
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def coord_for_path(filename):
    """The coordinate scan minted for `filename` — coordinates are
    row-<hash>, not the filename. A dependent 1:1 map step (parse) shares
    its ancestor's coordinate unchanged."""
    cells = TEST_HOME.select(f"step:scan path:{filename}", resolve_output=True)
    assert cells, f"no lane for path={filename}"
    return cells[0].coordinate


def _latest_live_row(session, step_name):
    """The latest fulfilled Arrow row for a step (the is_live=True
    equivalent).  Mirrors the old
    ``session.query(Materialization).filter_by(step_name=…, is_live=True)
    .order_by(id.desc()).first()`` lookup."""
    rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == step_name]
    live_addrs = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True)).all()
    }
    live_rows = [r for r in rows if r.get("address") in live_addrs]
    if not live_rows:
        return None
    return max(live_rows, key=lambda r: r.get("ts"))


def test_reduce_basic_and_lineage():
    create_file("a.txt", "10")
    create_file("b.txt", "20")
    create_file("c.txt", "30")

    @step
    def parse(scan):
        return int(scan["text"].strip())

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce1", steps=[scan, parse, sum_values], home=TEST_HOME)
    summary = assert_run(pipe)

    assert summary.created_count == 7  # 3 scan + 3 parse + 1 reduce

    with TEST_HOME.session() as session:
        sum_row = _latest_live_row(session, "sum")
        assert sum_row is not None
        assert sum_row["address"] is not None

        edges = session.query(MaterializationEdge).filter_by(child_address=sum_row["address"]).all()
        assert len(edges) == 3

def test_reduce_caching():
    create_file("a.txt", "10")
    create_file("b.txt", "20")

    @step
    def parse(scan):
        return int(scan["text"].strip())

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce2", steps=[scan, parse, sum_values], home=TEST_HOME)

    # Run 1: Create
    s1 = assert_run(pipe)
    assert s1.created_count == 5  # 2 scan + 2 parse + 1 reduce

    # Run 2: Reused
    s2 = assert_run(pipe)
    assert s2.reused_count == 5
    assert s2.created_count == 0

    # Change one file -> a new content-addressed lane for a.txt (scan +
    # parse created for it), b.txt's lane is untouched (reused), and the
    # reduce recomputes because its input membership changed.
    create_file("a.txt", "15")
    s3 = assert_run(pipe)
    assert s3.reused_count == 2  # scan(b), parse(b)
    assert s3.created_count == 3  # scan(a-new), parse(a-new), sum

    # Add a file -> a new lane computes, sum recomputes
    create_file("c.txt", "30")
    s4 = assert_run(pipe)
    assert s4.reused_count == 4  # scan(a), parse(a), scan(b), parse(b)
    assert s4.created_count == 3  # scan(c), parse(c), sum

def test_reduce_filtered_lane():
    create_file("a.txt", "keep:10")
    create_file("b.txt", "drop:20")

    @step
    def parse(scan):
        text = scan["text"].strip()
        if text.startswith("drop"):
            return Filtered("dropped")
        return int(text.split(":")[1])

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        # a.txt (10) is always present; b.txt (20) only when un-filtered.
        # Coordinates are content-addressed (row-<hash>), not "a.txt"/
        # "b.txt", so this checks membership by value, not by filename key.
        assert 10 in parse.values()
        if len(parse) == 2:
            assert 20 in parse.values()
        return sum(parse.values())

    pipe = pipeline(name="reduce3", steps=[scan, parse, sum_values], home=TEST_HOME)
    assert_run(pipe)

    with TEST_HOME.session() as session:
        sum_row = _latest_live_row(session, "sum")
        edges = session.query(MaterializationEdge).filter_by(child_address=sum_row["address"]).all()
        # Edge only from the survived lane
        assert len(edges) == 1

    # Un-filter b.txt
    create_file("b.txt", "keep:20")
    s2 = assert_run(pipe)
    # b.txt's content changed -> new scan+parse lane, sum recomputes
    assert s2.created_count == 3  # scan(b-new), parse(b-new), sum
    assert s2.reused_count == 2  # scan(a), parse(a)

    with TEST_HOME.session() as session:
        sum_row = _latest_live_row(session, "sum")
        edges2 = session.query(MaterializationEdge).filter_by(child_address=sum_row["address"]).all()
        assert len(edges2) == 2

def test_reduce_failed_parent_lane():
    create_file("a.txt", "10")
    create_file("b.txt", "fail")

    @step
    def parse(scan):
        text = scan["text"].strip()
        if text == "fail":
            raise ValueError("bad data")
        return int(text)

    @step(name="sum", depends_on=["parse"], shape="reduce", on_failed="block")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce4", steps=[scan, parse, sum_values], home=TEST_HOME)
    s1 = pipe.run(workers=1)

    assert s1.failed_count == 1
    assert s1.blocked_count == 1

    coord_b = coord_for_path("b.txt")
    with TEST_HOME.session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="sum").one()
        assert status.status == "blocked"
        meta = json.loads(status.metadata_json)
        assert f"parse:{coord_b}" in meta["failed_parents"]

def test_reduce_failed_parent_lane_use_passed():
    create_file("a.txt", "10")
    create_file("b.txt", "fail")
    create_file("c.txt", "20")

    @step
    def parse(scan):
        text = scan["text"].strip()
        if text == "fail":
            raise ValueError("bad data")
        return int(text)

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce4_use_passed", steps=[scan, parse, sum_values], home=TEST_HOME)
    s1 = pipe.run(workers=1)

    assert s1.failed_count == 1
    assert s1.blocked_count == 0
    assert s1.created_count == 6  # 3 scan + 2 parse successes + 1 sum

    coord_b = coord_for_path("b.txt")
    with TEST_HOME.session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="sum").one()
        assert status.status == "created"
        meta = json.loads(status.metadata_json)
        assert f"parse:{coord_b}" in meta["failed_parents"]

def test_reduce_downstream_map():
    create_file("a.txt", "10")

    @step
    def parse(scan):
        return int(scan["text"].strip())

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    @step(name="format")
    def format_val(sum):
        return f"Total: {sum}"

    pipe = pipeline(name="reduce5", steps=[scan, parse, sum_values, format_val], home=TEST_HOME)
    s1 = assert_run(pipe)

    assert s1.created_count == 4  # scan + parse + sum + format

    with TEST_HOME.session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="format").one()
        assert status.coordinate == "@all"
        assert status.status == "created"

def test_reduce_plan():
    create_file("a.txt", "10")

    @step
    def parse(scan):
        return int(scan["text"].strip())

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce6", steps=[scan, parse, sum_values], home=TEST_HOME)

    p1 = pipe.plan()
    # scan (a root expand) always plans as "execute"; everything downstream
    # of it — including the reduce — is unknowable without running the
    # generator, so it plans "pending".
    sum_items = [i for i in p1.items if i.step_name == "sum"]
    assert any(i.action == "pending" for i in sum_items)

    pipe.run(workers=1)

    p2 = pipe.plan()
    # A second plan() still can't see past the root expand: it always
    # re-plans "execute" for scan and "pending" downstream, never "reuse" —
    # see test_plan.py's test_second_plan_still_shows_execute_and_pending_not_reuse.
    sum_items2 = [i for i in p2.items if i.step_name == "sum"]
    assert any(i.action == "pending" for i in sum_items2)

def test_registration_errors():
    with pytest.raises(ValueError, match="skip_cache is meaningless with in_shape='aggregate'"):
        @step(name="sum", depends_on=["x"], shape="reduce", skip_cache=True)
        def sum_v1(x):
            pass

    with pytest.raises(ValueError, match=r"shape must be one of \['expand', 'join', 'map', 'reduce'\]"):
        @step(name="sum", shape="banana")
        def sum_v2(x):
            pass

    # A parentless reduce (no params, no depends_on) can't be caught at
    # decoration time — the parent-count check moved to build time so that
    # a reduce omitting depends_on= can get its parent from signature
    # inference. It raises when the pipeline is built:
    @step(name="sum", shape="reduce")
    def sum_v3():
        pass
    with pytest.raises(ValueError, match="requires at least one parent"):
        pipeline(name="pe", steps=[sum_v3], home=TEST_HOME).spec

def test_reduce_all_filtered():
    create_file("a.txt", "10")
    create_file("b.txt", "20")

    @step
    def parse(scan):
        from rubedo import Filtered
        return Filtered("reason")

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(name="reduce_empty", steps=[scan, parse, sum_values], home=TEST_HOME)
    s1 = pipe.run(workers=1)

    assert s1.failed_count == 0
    assert s1.blocked_count == 0
    assert s1.filtered_count == 2
    assert s1.created_count == 3  # 2 scan + 1 sum

    with TEST_HOME.session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="sum").one()
        assert status.status == "created"
