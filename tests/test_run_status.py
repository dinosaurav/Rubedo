import os
import pytest
import json
from rubedo.models import (
    Run,
    RunCoordinateStatus,
    RunEvent,
)
from rubedo import step, pipeline
from conftest import isolated_test_env

TEST_FOLDER = ".test_run_status_data"

TEST_HOME = None


@pytest.fixture(autouse=True)
def setup_teardown():
    global TEST_HOME
    with isolated_test_env("run_status") as env:
        TEST_HOME = env.home
        with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
            f.write('one')
        with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
            f.write('two')
        with open(os.path.join(TEST_FOLDER, "c.txt"), "w") as f:
            f.write('three')
        yield env.data_dir

@step(check_cache=False)
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def coord_for_path(filename, run_id=None):
    """The coordinate scan minted for `filename` — coordinates are
    row-<hash>, not the filename. A dependent 1:1 map step shares its
    ancestor's coordinate unchanged.

    An edited file mints a brand new lane: the old lane's materialization
    stays live (a different address, not superseded), so without a run_id
    filter this could resolve to a stale generation's coordinate — scope to
    a specific run when that matters.
    """
    kwargs = {"run_id": run_id} if run_id is not None else {}
    cells = TEST_HOME.select(
        f"step:scan path:{filename}", resolve_output=True, **kwargs
    )
    assert cells, f"no lane for path={filename}"
    return cells[0].coordinate


@step(name="dummy")
def dummy_processor(scan: dict) -> str:
    return f"processed_{scan['path']}"


def dummy_pipeline():
    return pipeline(name="p-dummy", steps=[scan, dummy_processor], home=TEST_HOME)


@step(name="failing")
def failing_processor(scan: dict) -> str:
    if scan["path"] == "b.txt":
        raise Exception("Failed on b.txt")
    return f"processed_{scan['path']}"


def failing_pipeline():
    return pipeline(name="p-fail", steps=[scan, failing_processor], home=TEST_HOME)


def test_first_run_creates_statuses(setup_teardown):
    res = dummy_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 6  # 3 files x (scan + dummy)
        for c in coords:
            assert c.status == "created"
            assert c.pipeline_id == "p-dummy"
            assert c.output_address is not None

        run_row = session.query(Run).filter_by(id=res.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["created"] == 6
        assert summary["reused"] == 0
        assert summary["failed"] == 0


def test_second_run_reuses_statuses(setup_teardown):
    dummy_pipeline().run(workers=1)
    res2 = dummy_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 6
        for c in coords:
            assert c.status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["reused"] == 6
        assert summary["created"] == 0

        rows = TEST_HOME.lanes.all_filled_rows()
        assert len(rows) == 7  # 6 lanes + 1 root-anchor (no new materializations)


def test_changed_file_creates_one(setup_teardown):
    input_dir = setup_teardown
    dummy_pipeline().run(workers=1)

    # modify one file
    with open(os.path.join(input_dir, "a.txt"), "w") as f:
        f.write("one_modified")

    res2 = dummy_pipeline().run(workers=1)

    coord_a = coord_for_path("a.txt", run_id=res2.run_id)
    coord_b = coord_for_path("b.txt", run_id=res2.run_id)
    coord_c = coord_for_path("c.txt", run_id=res2.run_id)

    with TEST_HOME.session() as session:
        statuses = (
            session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        )
        by_cell = {(s.step_name, s.coordinate): s for s in statuses}
        assert by_cell[("scan", coord_a)].status == "created"
        assert by_cell[("dummy", coord_a)].status == "created"
        assert by_cell[("scan", coord_b)].status == "reused"
        assert by_cell[("dummy", coord_b)].status == "reused"
        assert by_cell[("scan", coord_c)].status == "reused"
        assert by_cell[("dummy", coord_c)].status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["created"] == 2  # scan(a) + dummy(a)
        assert summary["reused"] == 4  # scan/dummy for b, c

        rows = TEST_HOME.lanes.all_filled_rows()
        assert len(rows) == 10  # 6 original + 2 anchors + 2 new (scan-a, dummy-a)


def test_deleted_file_absent_from_next_run(setup_teardown):
    input_dir = setup_teardown
    res1 = dummy_pipeline().run(workers=1)

    # get old address (the "dummy" step's)
    coord_a = coord_for_path("a.txt")
    with TEST_HOME.session() as session:
        old_mat_a = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res1.run_id, coordinate=coord_a, step_name="dummy")
            .first()
        )
        old_address = old_mat_a.output_address

    os.remove(os.path.join(input_dir, "a.txt"))

    res2 = dummy_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        coords = {
            c.coordinate
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id)
            .all()
        }
        # a.txt simply isn't scanned this run — no "removed" status, no removal tracking.
        assert coord_a not in coords

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert "removed" not in summary
        assert summary["reused"] == 4  # scan/dummy for b, c

        # Output bytes still exist logically (the old run can still resolve it).
        assert TEST_HOME.select(f"address:{old_address}", run_id=res1.run_id)


def test_failed_coordinate_records_failed(setup_teardown):
    res = failing_pipeline().run(workers=1)

    coord_a = coord_for_path("a.txt")
    coord_b = coord_for_path("b.txt")
    coord_c = coord_for_path("c.txt")

    with TEST_HOME.session() as session:
        statuses = (
            session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        )
        by_cell = {(s.step_name, s.coordinate): s for s in statuses}
        assert by_cell[("scan", coord_a)].status == "created"
        assert by_cell[("scan", coord_b)].status == "created"
        assert by_cell[("scan", coord_c)].status == "created"
        assert by_cell[("failing", coord_a)].status == "created"
        assert by_cell[("failing", coord_b)].status == "failed"
        assert by_cell[("failing", coord_c)].status == "created"
        assert "Failed on b.txt" in by_cell[("failing", coord_b)].error_message

        run_row = session.query(Run).filter_by(id=res.run_id).first()
        assert run_row.status == "completed_with_failures"

        summary = json.loads(run_row.summary_json)
        assert summary["failed"] == 1
        assert summary["created"] == 5  # 3 scan + 2 failing (a, c)

        # Ensure no materialization created for b.txt's failing step
        mats = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("run_id") == res.run_id]
        assert len(mats) == 6  # 5 lanes + 1 root-anchor


def test_event_log_populated(setup_teardown):
    res = dummy_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        events = session.query(RunEvent).filter_by(run_id=res.run_id).all()
        types = [e.event_type for e in events]
        assert "run_started" in types
        assert "step_processing_started" in types
        assert "materialization_created" in types
        assert "run_completed" in types
