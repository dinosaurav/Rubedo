import os
import tempfile
import pytest
import json
from rubedo.db import init_db, get_session
import rubedo.db as db
from rubedo.models import (
    Run,
    RunCoordinateStatus,
    Materialization,
    MaterializationIndexEntry,
    RunEvent,
)
from rubedo import step, pipeline
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine

TEST_FOLDER = "input"


@pytest.fixture(autouse=True)
def setup_teardown():
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)

    os.makedirs(".rubedo/objects", exist_ok=True)

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    if db.engine is not None:
        db.engine.dispose()

    db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    db.Base.metadata.create_all(bind=db.engine)
    from sqlalchemy.orm import sessionmaker

    db.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)

    input_dir = os.path.join(temp_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    with open(os.path.join(input_dir, "a.txt"), "w") as f:
        f.write("one")
    with open(os.path.join(input_dir, "b.txt"), "w") as f:
        f.write("two")
    with open(os.path.join(input_dir, "c.txt"), "w") as f:
        f.write("three")

    yield input_dir

    db.Base.metadata.drop_all(db.engine)
    db.engine.dispose()
    db.engine = None
    db.SessionLocal = None
    os.chdir(orig_dir)


@step(name="scan", version="1", shape="expand", index=["path"])
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content. Indexed on
    `path` so tests can find "the lane for x.txt" without the coordinate
    being that literal string."""
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
    with get_session() as session:
        q = session.query(RunCoordinateStatus).filter_by(step_name="scan")
        if run_id is not None:
            q = q.filter_by(run_id=run_id)
        rows = q.filter(RunCoordinateStatus.materialization_id.isnot(None)).all()
        for rc in rows:
            hit = (
                session.query(MaterializationIndexEntry)
                .filter_by(
                    materialization_id=rc.materialization_id,
                    field="path",
                    value=filename,
                )
                .first()
            )
            if hit:
                return rc.coordinate
    return None


@step(name="dummy", version="v1", depends_on=["scan"])
def dummy_processor(scan: dict) -> str:
    return f"processed_{scan['path']}"


p_dummy = pipeline(name="p-dummy", steps=[scan, dummy_processor])


@step(name="failing", version="v1", depends_on=["scan"])
def failing_processor(scan: dict) -> str:
    if scan["path"] == "b.txt":
        raise Exception("Failed on b.txt")
    return f"processed_{scan['path']}"


p_fail = pipeline(name="p-fail", steps=[scan, failing_processor])


def test_first_run_creates_statuses(setup_teardown):
    res = p_dummy.run(workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 6  # 3 files x (scan + dummy)
        for c in coords:
            assert c.status == "created"
            assert c.pipeline_id == "p-dummy"
            assert c.materialization_id is not None

        run_row = session.query(Run).filter_by(id=res.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["created"] == 6
        assert summary["reused"] == 0
        assert summary["failed"] == 0


def test_second_run_reuses_statuses(setup_teardown):
    p_dummy.run(workers=1)
    res2 = p_dummy.run(workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 6
        for c in coords:
            assert c.status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["reused"] == 6
        assert summary["created"] == 0

        mats = session.query(Materialization).all()
        assert len(mats) == 6  # No new materializations


def test_changed_file_creates_one(setup_teardown):
    input_dir = setup_teardown
    p_dummy.run(workers=1)

    # modify one file
    with open(os.path.join(input_dir, "a.txt"), "w") as f:
        f.write("one_modified")

    res2 = p_dummy.run(workers=1)

    coord_a = coord_for_path("a.txt", run_id=res2.run_id)
    coord_b = coord_for_path("b.txt", run_id=res2.run_id)
    coord_c = coord_for_path("c.txt", run_id=res2.run_id)

    with get_session() as session:
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

        mats = session.query(Materialization).all()
        assert len(mats) == 8  # 6 original + 2 new (scan-a, dummy-a)


def test_deleted_file_absent_from_next_run(setup_teardown):
    input_dir = setup_teardown
    res1 = p_dummy.run(workers=1)

    # get old address (the "dummy" step's)
    coord_a = coord_for_path("a.txt")
    with get_session() as session:
        old_mat_a = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res1.run_id, coordinate=coord_a, step_name="dummy")
            .first()
        )
        old_address = old_mat_a.output_address

    os.remove(os.path.join(input_dir, "a.txt"))

    res2 = p_dummy.run(workers=1)

    with get_session() as session:
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

        # Output bytes still exist logically (materialization row still there)
        mat = (
            session.query(Materialization).filter_by(output_address=old_address).first()
        )
        assert mat is not None


def test_failed_coordinate_records_failed(setup_teardown):
    res = p_fail.run(workers=1)

    coord_a = coord_for_path("a.txt")
    coord_b = coord_for_path("b.txt")
    coord_c = coord_for_path("c.txt")

    with get_session() as session:
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
        mats = (
            session.query(Materialization).filter_by(created_by_run_id=res.run_id).all()
        )
        assert len(mats) == 5


def test_event_log_populated(setup_teardown):
    res = p_dummy.run(workers=1)

    with get_session() as session:
        events = session.query(RunEvent).filter_by(run_id=res.run_id).all()
        types = [e.event_type for e in events]
        assert "run_started" in types
        assert "step_processing_started" in types
        assert "materialization_created" in types
        assert "run_completed" in types
