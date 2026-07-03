import os
import shutil
import pytest
from unittest.mock import patch

from batchbrain import run, step, pipeline
from batchbrain.db import init_db
import batchbrain.store as store

TEST_FOLDER = ".test_staging_data"
ENV_FOLDER = ".test_staging_env"

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

    os.environ["BATCHBRAIN_DB_PATH"] = f"sqlite:///{abs_env_folder}/batchbrain.sqlite"
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


def test_staging_cleanup_on_error():
    # We patch _commit_materialization to raise an unexpected Exception.
    # This will trigger the exception handler in _commit_execution_result,
    # and the finally block should clean up staging.
    
    pipe = pipeline(id="p1", name="p1", folder=TEST_FOLDER, steps=[my_step])

    
    # We intercept stage_and_commit just to mock a failure during the DB commit phase.
    # Wait, the easiest way to test staging cleanup is to fail DURING stage_and_commit
    # right after writing the staging file, OR fail _commit_materialization.
    # If we fail _commit_materialization, stage_and_commit has already moved it.
    # Let's fail stage_and_commit halfway!
    
    def mock_stage_and_commit(run_id, coordinate, result):
        # We manually write a file to staging to prove it gets cleaned up
        staging_path = store._get_staging_path(run_id, coordinate, "mockhash")
        os.makedirs(os.path.dirname(staging_path), exist_ok=True)
        with open(staging_path, "w") as f:
            f.write("staged_but_failed")
            
        # Then we raise an error!
        raise ValueError("Simulated failure during stage_and_commit")

    with patch("batchbrain.ledger.stage_and_commit", side_effect=mock_stage_and_commit):
        summary = run(pipe)
        
    assert summary.failed_count == 1
    
    # Assert staging directory for the run is gone!
    run_staging = os.path.join(store.STAGING_DIR, summary.run_id)
    assert not os.path.exists(run_staging)

def test_staging_cleanup_on_commit_error():
    pipe = pipeline(id="p2", name="p2", folder=TEST_FOLDER, steps=[my_step])
    
    def mock_commit():
        raise RuntimeError("DB commit failed")
        
    # Patch session.commit inside ledger's _commit_execution_result context
    # It's tricky to patch just one commit, so we patch _commit_materialization to fail.
    with patch("batchbrain.ledger._commit_materialization", side_effect=RuntimeError("Simulated DB failure")):
        summary = run(pipe)
        
    assert summary.failed_count == 1
    
    run_staging = os.path.join(store.STAGING_DIR, summary.run_id)
    assert not os.path.exists(run_staging)

