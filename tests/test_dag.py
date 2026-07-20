import pytest
import os
from rubedo import step, pipeline
from rubedo.planning import topological_sort
from rubedo.models import RunCoordinateStatus, MaterializationEdge, InputHashUsage
from conftest import isolated_test_env

TEST_FOLDER = ".test_dag_data"
ENV_FOLDER = ".test_dag_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def setup_teardown():
    global TEST_HOME
    with isolated_test_env("dag") as env:
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


def coordinate_for_path(step_name, path_value):
    """The migrated coordinate is a content hash (row-<hash>), not the
    literal filename. Recover it by scanning that step's live
    materializations for the one whose payload carries this path."""
    cells = TEST_HOME.select(f"step:{step_name} path:{path_value}", resolve_output=True)
    assert cells, f"no lane for path={path_value}"
    return cells[0].coordinate


def test_topological_sort():
    @step
    def a(scan):
        pass

    @step
    def b(a):
        pass

    @step
    def c(b):
        pass

    p = pipeline(name="p1", steps=[scan, a, b, c], home=TEST_HOME)
    topo = topological_sort(p.spec)
    assert [s.name for s in topo] == ["scan", "a", "b", "c"]


def test_linear_dag():
    @step
    def read(scan):
        return scan["text"].strip()

    @step
    def upper(read):
        return read.upper()

    pipe = pipeline(name="p1", steps=[scan, read, upper], home=TEST_HOME)

    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")

    summary = pipe.run(workers=1)

    with TEST_HOME.session() as session:
        # Check coordinates created
        statuses = session.query(RunCoordinateStatus).all()
        # 2 files * 3 steps (scan, read, upper) — scan's own lanes now count
        assert len(statuses) == 6

        # Check outputs. Coordinates are content hashes, not "f1.txt" —
        # recover f1.txt's coordinate via its scan payload, then reuse it: a
        # simple map chain propagates the parent's coordinate unchanged (see
        # planning.py's `_plan_step`).
        coord_f1 = coordinate_for_path("scan", "f1.txt")
        assert coord_f1 is not None

        read_cell = next(c for c in summary.cells("read") if c.coordinate == coord_f1)
        upper_cell = next(c for c in summary.cells("upper") if c.coordinate == coord_f1)
        assert read_cell.output_address is not None
        assert upper_cell.output_address is not None

        # Check edges
        edge = (
            session.query(MaterializationEdge)
            .filter_by(parent_address=read_cell.output_address, child_address=upper_cell.output_address)
            .first()
        )
        assert edge is not None


def test_cache_hit():
    @step
    def read(scan):
        return scan["text"].strip()

    pipe = pipeline(name="p2", steps=[scan, read], home=TEST_HOME)

    create_file("f1.txt", "hello")
    pipe.run(workers=1)

    with TEST_HOME.session() as session:
        statuses = session.query(RunCoordinateStatus).all()
        assert len(statuses) == 2  # scan's lane + read's lane
        assert {s.status for s in statuses} == {"created"}

    # Run again, should be reused
    pipe.run(workers=1)

    with TEST_HOME.session() as session:
        # 2 from first run, 2 from second run
        statuses = (
            session.query(RunCoordinateStatus)
            .order_by(RunCoordinateStatus.id.desc())
            .limit(1)
            .all()
        )
        assert statuses[0].status == "reused"


def test_invalidate_downstream_then_rerun():
    from rubedo import Selection
    from rubedo.invalidation import invalidate

    @step
    def read(scan):
        return scan["text"].strip()

    @step
    def upper(read):
        return read.upper()

    pipe = pipeline(name="p4", steps=[scan, read, upper], home=TEST_HOME)

    create_file("f1.txt", "hello")
    pipe.run(workers=1)

    res = invalidate(Selection(step="upper"), reason="bad output", home=TEST_HOME)
    assert res["invalidated_count"] == 1

    # Recompute resurrects the tombstoned materialization; its lineage
    # edges already exist and must not be inserted twice
    summary = pipe.run(workers=1)
    assert summary.created_count == 1
    assert summary.failed_count == 0

    with TEST_HOME.session() as session:
        upper_rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "upper"]
        # 2 rows: old (invalidated, history) + new (live).  The latest
        # one (by ts) is the live one.
        assert len(upper_rows) == 2
        latest = max(upper_rows, key=lambda r: r.get("ts", ""))
        addr = latest.get("address")
        usage = session.query(InputHashUsage).filter_by(address=addr).first()
        assert usage.fulfilled is True
        edges = (
            session.query(MaterializationEdge).filter_by(child_address=addr).all()
        )
        assert len(edges) == 1  # not duplicated


def test_duplicate_content_files_share_materialization():
    # This scan recipe yields only the file's text — no "path" field — so
    # two files with identical bytes yield byte-identical payloads and
    # collapse into a single content-addressed lane (row-<hash>), per TODO
    # 14 ("identical rows collapse"). The module-level `scan` above folds
    # "path" into the payload precisely so lanes stay distinguishable; this
    # test wants the opposite to exercise collapse.
    @step
    def scan_nopath():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"text": open(path).read()}

    @step
    def upper(scan_nopath):
        return scan_nopath["text"].strip().upper()

    pipe = pipeline(name="p5", steps=[scan_nopath, upper], home=TEST_HOME)

    # Same content -> same lane -> one materialization and one lineage edge,
    # without a unique-constraint crash, even though the generator yields it
    # twice.
    create_file("f1.txt", "hello")
    create_file("f2.txt", "hello")

    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    with TEST_HOME.session() as session:
        upper_rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "upper"]
        assert len(upper_rows) == 1
        edges = (
            session.query(MaterializationEdge).filter_by(child_address=upper_rows[0].get("address")).all()
        )
        assert len(edges) == 1


def test_dag_blocked_on_failure():
    @step
    def read(scan):
        raise ValueError("Boom")

    @step
    def upper(read):
        return read.upper()

    pipe = pipeline(name="p3", steps=[scan, read, upper], home=TEST_HOME)

    create_file("f1.txt", "hello")

    pipe.run(workers=1)

    with TEST_HOME.session() as session:
        rc_read = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="read")
            .order_by(RunCoordinateStatus.id.desc())
            .first()
        )
        assert rc_read.status == "failed"

        rc_upper = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="upper")
            .order_by(RunCoordinateStatus.id.desc())
            .first()
        )
        assert rc_upper.status == "blocked"
