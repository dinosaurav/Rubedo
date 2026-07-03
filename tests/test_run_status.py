import os
import tempfile
import pytest
import json
from batchbrain.db import init_db, get_session
import batchbrain.db as db
from batchbrain.models import (
    Run,
    RunCoordinateStatus,
    Materialization,
    Manifest,
    ManifestEntry,
    RunEvent,
)
from batchbrain.runner import run
from batchbrain import step, pipeline
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


@pytest.fixture(autouse=True)
def setup_teardown():
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)

    os.makedirs(".batchbrain/objects", exist_ok=True)

    os.environ["BATCHBRAIN_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    if db.engine is not None:
        db.engine.dispose()

    db.engine = create_engine(
        os.environ["BATCHBRAIN_DB_PATH"],
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


@step(name="dummy", version="v1")
def dummy_processor(path: str) -> str:
    return f"processed_{os.path.basename(path)}"


p_dummy = pipeline(id="p-dummy", name="Dummy", folder="input", steps=[dummy_processor])


@step(name="failing", version="v1")
def failing_processor(path: str) -> str:
    if "b.txt" in path:
        raise Exception("Failed on b.txt")
    return f"processed_{os.path.basename(path)}"


p_fail = pipeline(id="p-fail", name="Fail", folder="input", steps=[failing_processor])


def test_first_run_creates_statuses(setup_teardown):
    input_dir = setup_teardown
    res = run(p_dummy, input_dir, workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 3
        for c in coords:
            assert c.status == "created"
            assert c.pipeline_id == "p-dummy"
            assert c.materialization_id is not None

        run_row = session.query(Run).filter_by(id=res.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["created"] == 3
        assert summary["reused"] == 0
        assert summary["failed"] == 0
        assert summary["removed"] == 0


def test_second_run_reuses_statuses(setup_teardown):
    input_dir = setup_teardown
    run(p_dummy, input_dir, workers=1)
    res2 = run(p_dummy, input_dir, workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 3
        for c in coords:
            assert c.status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["reused"] == 3
        assert summary["created"] == 0

        mats = session.query(Materialization).all()
        assert len(mats) == 3  # No new materializations


def test_changed_file_creates_one(setup_teardown):
    input_dir = setup_teardown
    run(p_dummy, input_dir, workers=1)

    # modify one file
    with open(os.path.join(input_dir, "a.txt"), "w") as f:
        f.write("one_modified")

    res2 = run(p_dummy, input_dir, workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id)
            .all()
        }
        assert coords["a.txt"].status == "created"
        assert coords["b.txt"].status == "reused"
        assert coords["c.txt"].status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["created"] == 1
        assert summary["reused"] == 2

        mats = session.query(Materialization).all()
        assert len(mats) == 4  # 3 original + 1 new


def test_deleted_file_records_removed(setup_teardown):
    input_dir = setup_teardown
    res1 = run(p_dummy, input_dir, workers=1)

    # get old address
    with get_session() as session:
        old_mat_a = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res1.run_id, coordinate="a.txt")
            .first()
        )
        old_address = old_mat_a.output_address

    os.remove(os.path.join(input_dir, "a.txt"))

    res2 = run(p_dummy, input_dir, workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id)
            .all()
        }
        assert coords["a.txt"].status == "removed"
        assert coords["b.txt"].status == "reused"
        assert coords["c.txt"].status == "reused"

        run_row = session.query(Run).filter_by(id=res2.run_id).first()
        summary = json.loads(run_row.summary_json)
        assert summary["removed"] == 1
        assert summary["reused"] == 2

        # Check manifest
        manifest = session.query(Manifest).filter_by(run_id=res2.run_id).first()
        entries = session.query(ManifestEntry).filter_by(manifest_id=manifest.id).all()
        assert len(entries) == 2
        assert "a.txt" not in [e.coordinate for e in entries]

        # Output bytes still exist logically (materialization row still there)
        mat = (
            session.query(Materialization).filter_by(output_address=old_address).first()
        )
        assert mat is not None


def test_failed_coordinate_records_failed(setup_teardown):
    input_dir = setup_teardown
    res = run(p_fail, input_dir, workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res.run_id)
            .all()
        }
        assert coords["a.txt"].status == "created"
        assert coords["b.txt"].status == "failed"
        assert coords["c.txt"].status == "created"
        assert "Failed on b.txt" in coords["b.txt"].error_message

        run_row = session.query(Run).filter_by(id=res.run_id).first()
        assert run_row.status == "completed_with_failures"

        summary = json.loads(run_row.summary_json)
        assert summary["failed"] == 1
        assert summary["created"] == 2

        # Ensure no materialization created for b.txt
        mats = (
            session.query(Materialization).filter_by(created_by_run_id=res.run_id).all()
        )
        assert len(mats) == 2


def test_event_log_populated(setup_teardown):
    input_dir = setup_teardown
    res = run(p_dummy, input_dir, workers=1)

    with get_session() as session:
        events = session.query(RunEvent).filter_by(run_id=res.run_id).all()
        types = [e.event_type for e in events]
        assert "run_started" in types
        assert "manifest_created" in types
        assert "step_processing_started" in types
        assert "materialization_created" in types
        assert "run_completed" in types
