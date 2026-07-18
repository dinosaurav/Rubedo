import os
import tempfile
import pytest
from unittest.mock import patch
from rubedo.db import init_db
from rubedo import lane_store
from rubedo.store import serialize_output
from rubedo import step, pipeline
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine

TEST_FOLDER = "input"


@pytest.fixture(autouse=True)
def setup_teardown():
    import rubedo.db as db

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


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


@step(name="dummy")
def dummy_processor(scan: dict) -> str:
    return f"processed_{scan['path']}"


p_dummy = pipeline(name="p-dummy", steps=[scan, dummy_processor])


def test_crash_before_processing(setup_teardown):
    # Simulate a crash during the actual processing function
    @step(name="crashing")
    def crashing_processor(scan: dict) -> str:
        raise Exception("Crash before processing completes!")

    # Same pipeline name as p_dummy: this is a crash-recovery re-run of the
    # *same* pipeline (TODO 33 scopes addresses per pipeline, so a
    # differently-named pipeline would legitimately not share scan's
    # cached output — that's the bug being fixed, not what this test is
    # about).
    p_crashing = pipeline(name="p-dummy", steps=[scan, crashing_processor])

    summary = p_crashing.run(workers=1)
    # scan(a)/(b) succeed; both crashing(a)/(b) fail -> partial success.
    assert summary.status == "completed_with_failures"
    assert summary.failed_count == 2
    assert summary.created_count == 2  # scan(a) + scan(b) succeed

    # Rerun should attempt again (and still fail if we use the crashing one,
    # but let's use the normal one to show it recovers)
    summary2 = p_dummy.run(workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 2  # dummy(a) + dummy(b); scan reused
    assert summary2.reused_count == 2  # scan(a) + scan(b)


def test_crash_during_staging(setup_teardown):
    # Simulate crash inside serialize_output

    def crashing_stage(*args, **kwargs):
        raise Exception("Disk full or worker killed during write")

    with patch("rubedo.ledger.serialize_output", side_effect=crashing_stage):
        summary = p_dummy.run(workers=1)
        assert summary.status == "failed"
        assert summary.created_count == 0

    # Check that no materialization rows exist (the anchor is a cache
    # entry, not a lane — it's in a separate file and doesn't count)
    assert len([r for r in lane_store.all_filled_rows() if r.get("lane_key") != "@root"]) == 0

    # Rerun normally
    summary2 = p_dummy.run(workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 4  # scan(a,b) + dummy(a,b)


def test_crash_after_staging_before_db_commit(setup_teardown):
    original_serialize = serialize_output

    def crashing_serialize_but_write_succeeds(*args, **kwargs):
        # We actually do the write
        original_serialize(*args, **kwargs)
        # But we throw before the DB row can be inserted
        raise Exception("Worker killed right after disk write but before DB commit")

    with patch(
        "rubedo.ledger.serialize_output",
        side_effect=crashing_serialize_but_write_succeeds,
    ):
        summary = p_dummy.run(workers=1)
        assert summary.status == "failed"

    # Verify no materialization row (anchor is a cache entry, not a lane)
    assert len([r for r in lane_store.all_filled_rows() if r.get("lane_key") != "@root"]) == 0

    # Rerun normally
    # The output address will be exactly the same.
    summary2 = p_dummy.run(workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 4  # scan(a,b) + dummy(a,b)


def test_success_and_reuse(setup_teardown):
    summary1 = p_dummy.run(workers=1)
    assert summary1.status == "completed"
    assert summary1.created_count == 4  # scan(a,b) + dummy(a,b)

    # Rerun should skip
    summary2 = p_dummy.run(workers=1)
    assert summary2.status == "completed"
    assert summary2.created_count == 0
    assert summary2.reused_count == 4


def test_per_segment_flush_preserves_earlier_steps(setup_teardown):
    """A crash in a later segment must not lose earlier segments' outputs.

    scan (expand, segment 1) -> step_a (map, segment 2) -> step_b (map, segment 2)
    -> crash_step (map, segment 3)

    If crash_step raises, scan and step_a/step_b should be on disk and
    reused on the next run — not lost with the in-memory buffers.
    """
    call_count = {"crash": 0}

    @step
    def step_a(scan: dict):
        return {"path": scan["path"], "upper": scan["text"].upper()}

    @step
    def step_b(step_a: dict):
        return {"path": step_a["path"], "len": len(step_a["upper"])}

    @step
    def crash_step(step_b: dict):
        call_count["crash"] += 1
        raise RuntimeError("crash_step failed!")

    p = pipeline(name="seg-flush", steps=[scan, step_a, step_b, crash_step])

    summary = p.run(workers=1)
    # scan + step_a + step_b succeed (2 lanes each = 6), crash_step fails (2)
    assert summary.status == "completed_with_failures"
    assert summary.failed_count == 2
    assert summary.created_count == 6

    # The key assertion: scan, step_a, step_b rows are on disk.
    # If per-segment flush didn't work, clear_run_buffers on the error
    # path would have wiped them and this would be 0.
    rows = lane_store.all_filled_rows()
    step_names = {r["step_name"] for r in rows}
    assert "scan" in step_names
    assert "step_a" in step_names
    assert "step_b" in step_names
    assert "crash_step" not in step_names

    # Rerun with a non-crashing step — earlier steps should reuse. Same
    # pipeline name as `p`: a crash-recovery re-run of the *same* pipeline
    # (TODO 33 scopes addresses per pipeline, so a differently-named
    # pipeline would legitimately miss scan/step_a/step_b's cache — that's
    # the bug being fixed, not what this test is about).
    @step
    def ok_step(step_b: dict):
        return {"result": step_b["len"] * 2}

    p2 = pipeline(name="seg-flush", steps=[scan, step_a, step_b, ok_step])
    summary2 = p2.run(workers=1)
    assert summary2.status == "completed"
    # scan, step_a, step_b all reused (2 lanes each = 6), ok_step created (2)
    assert summary2.reused_count == 6
    assert summary2.created_count == 2
