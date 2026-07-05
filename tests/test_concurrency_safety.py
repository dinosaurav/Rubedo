import os
import shutil
import pytest
from unittest.mock import patch

from rubedo import run, step, pipeline
from rubedo.db import init_db, get_session
import rubedo.store as store

TEST_FOLDER = ".test_concurrency_data"
ENV_FOLDER = ".test_concurrency_env"

@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
        
    store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    # Must use a physical file so other sessions can see it.
    os.environ["RUBEDO_DB_PATH"] = f"sqlite:///{abs_env_folder}/rubedo.sqlite"
    init_db()
    
    with open(os.path.join(abs_test_folder, "a.txt"), "w") as f:
        f.write("A")
        
    yield
    
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


@step(name="my_step", version="1")
def my_step(content):
    return content.lower()

def test_concurrency_identical_bytes_collision():
    # We patch stage_and_commit to first insert a competing materialization directly into the DB.
    # This simulates another worker completing the same task right before we commit.
    original_stage_and_commit = store.stage_and_commit
    
    pipe = pipeline(id="p1", name="p1", folder=TEST_FOLDER, steps=[my_step])

    has_injected = False
    
    def mock_stage_and_commit(run_id, coordinate, result):
        nonlocal has_injected
        final_path, output_content_hash, content_type = original_stage_and_commit(run_id, coordinate, result)
        
        if not has_injected:
            has_injected = True
            # Simulate a competing run inserting a live materialization
            with get_session() as session:
                from rubedo.planning import _plan_step
                from rubedo.ledger import _commit_materialization
                from rubedo.sources import FolderSource
                
                # We need the step's input hash to compute the output address
                source = FolderSource(TEST_FOLDER)
                item = list(source.scan())[0]
                
                # Mock a decision for the step
                decision = _plan_step(session, my_step, [item], {}, "", False, False)[0]
                
                # Insert the identical materialization
                _commit_materialization(
                    session,
                    pipeline_id="p1",
                    step=my_step,
                    input_hash=decision.input_hash,
                    output_address=decision.output_address,
                    output_content_hash=output_content_hash,
                    content_type=content_type,
                    output_path=final_path,
                    metadata_json=None,
                    run_id="run_concurrent",
                )
                session.commit()
                
        return final_path, output_content_hash, content_type

    with patch("rubedo.ledger.stage_and_commit", side_effect=mock_stage_and_commit):
        summary = run(pipe)
        
    if summary.failed_count > 0:
        with get_session() as session:
            from rubedo.models import RunCoordinateStatus
            for s in session.query(RunCoordinateStatus).filter_by(status='failed').all():
                print(f"FAILED TRACE: {s.error_message}")
                
    # Since bytes were identical, the original run should mark it as "reused"!
    assert summary.reused_count == 1
    assert summary.created_count == 0

def test_concurrency_different_bytes_collision():
    # Same as above, but the competing run inserted DIFFERENT bytes for the same address.
    # This means the current run must supersede it.
    original_stage_and_commit = store.stage_and_commit
    
    pipe = pipeline(id="p2", name="p2", folder=TEST_FOLDER, steps=[my_step])

    has_injected = False
    
    def mock_stage_and_commit(run_id, coordinate, result):
        nonlocal has_injected
        final_path, output_content_hash, content_type = original_stage_and_commit(run_id, coordinate, result)
        
        if not has_injected:
            has_injected = True
            with get_session() as session:
                from rubedo.planning import _plan_step
                from rubedo.ledger import _commit_materialization
                from rubedo.sources import FolderSource
                
                source = FolderSource(TEST_FOLDER)
                item = list(source.scan())[0]
                decision = _plan_step(session, my_step, [item], {}, "", False, False)[0]
                
                # Insert a competing materialization with DIFFERENT content hash
                _commit_materialization(
                    session,
                    pipeline_id="p2",
                    step=my_step,
                    input_hash=decision.input_hash,
                    output_address=decision.output_address,
                    output_content_hash="mocked_different_hash",
                    content_type=content_type,
                    output_path="mocked_different_path",
                    metadata_json=None,
                    run_id="run_concurrent",
                )
                session.commit()
                
        return final_path, output_content_hash, content_type

    with patch("rubedo.ledger.stage_and_commit", side_effect=mock_stage_and_commit):
        summary = run(pipe)
        
    if summary.failed_count > 0:
        with get_session() as session:
            from rubedo.models import RunCoordinateStatus
            for s in session.query(RunCoordinateStatus).filter_by(status='failed').all():
                print(f"FAILED TRACE: {s.error_message}")

    # The current run supersedes the injected one, which counts as "created" (since it made a new live row).
    assert summary.created_count == 1
    assert summary.reused_count == 0
    assert summary.failed_count == 0

def test_sqlite_pragmas():
    from sqlalchemy import text
    with get_session() as session:
        result = session.execute(text("PRAGMA journal_mode")).scalar()
        assert result.upper() == "WAL"
        
        result2 = session.execute(text("PRAGMA busy_timeout")).scalar()
        assert str(result2) == "5000"
