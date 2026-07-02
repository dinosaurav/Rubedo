import os
import tempfile
import pytest
from unittest.mock import patch
from batchbrain.db import init_db, get_session
from batchbrain.models import Materialization
from batchbrain.runner import run
from batchbrain.store import stage_and_commit
from batchbrain import step, pipeline
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


@pytest.fixture(autouse=True)
def setup_teardown():
    import batchbrain.db as db

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

    # Create some dummy files
    input_dir = os.path.join(temp_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    with open(os.path.join(input_dir, "a.txt"), "w") as f:
        f.write("hello")
    with open(os.path.join(input_dir, "b.txt"), "w") as f:
        f.write("world")

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


def test_crash_before_processing(setup_teardown):
    temp_workspace = setup_teardown

    # Simulate a crash during the actual processing function
    @step(name="crashing", version="v1")
    def crashing_processor(path: str) -> str:
        raise Exception("Crash before processing completes!")

    p_crashing = pipeline(
        id="p-crash", name="Crash", folder="input", steps=[crashing_processor]
    )

    summary = run(p_crashing, str(temp_workspace), workers=1)
    assert summary.status == "failed"
    assert summary.failed_count == 2
    assert summary.created_count == 0

    # Rerun should attempt again (and still fail if we use the crashing one,
    # but let's use the normal one to show it recovers)
    summary2 = run(p_dummy, str(temp_workspace), workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 2
    assert summary2.reused_count == 0


def test_crash_during_staging(setup_teardown):
    temp_workspace = setup_teardown
    # Simulate crash inside stage_and_commit

    def crashing_stage(*args, **kwargs):
        raise Exception("Disk full or worker killed during write")

    with patch("batchbrain.runner.stage_and_commit", side_effect=crashing_stage):
        summary = run(p_dummy, str(temp_workspace), workers=1)
        assert summary.status == "failed"
        assert summary.created_count == 0

    # Check that no materialization rows exist
    with get_session() as session:
        assert session.query(Materialization).count() == 0

    # Rerun normally
    summary2 = run(p_dummy, str(temp_workspace), workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 2


def test_crash_after_staging_before_db_commit(setup_teardown):
    temp_workspace = setup_teardown
    original_stage = stage_and_commit

    def crashing_stage_but_write_succeeds(*args, **kwargs):
        # We actually do the write
        original_stage(*args, **kwargs)
        # But we throw before the DB row can be inserted
        raise Exception("Worker killed right after disk write but before DB commit")

    with patch(
        "batchbrain.runner.stage_and_commit",
        side_effect=crashing_stage_but_write_succeeds,
    ):
        summary = run(p_dummy, str(temp_workspace), workers=1)
        assert summary.status == "failed"

    # Verify no materialization row
    with get_session() as session:
        assert session.query(Materialization).count() == 0

    # Rerun normally
    # The output address will be exactly the same.
    # Because stage_and_commit does an atomic os.replace, it will harmlessly overwrite the orphaned file.
    summary2 = run(p_dummy, str(temp_workspace), workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 2


def test_success_and_reuse(setup_teardown):
    temp_workspace = setup_teardown
    summary1 = run(p_dummy, str(temp_workspace), workers=1)
    assert summary1.status == "completed"
    assert summary1.created_count == 2

    # Rerun should skip
    summary2 = run(p_dummy, str(temp_workspace), workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 0
    assert summary2.reused_count == 2
