import os
import shutil
import pytest
from unittest.mock import patch

from rubedo import step, pipeline
from conftest import make_home

TEST_FOLDER = ".test_staging_data"
ENV_FOLDER = ".test_staging_env"

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
    
    with open(os.path.join(abs_test_folder, "a.txt"), "w") as f:
        f.write("A")
        
    TEST_HOME = make_home(ENV_FOLDER)
    yield
    
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


@step
def my_step(params):
    return params["content"].lower()


def test_staging_cleanup_on_error():
    # We patch serialize_output to raise an unexpected Exception.
    # This will trigger the exception handler in _commit_execution_result,
    # and the finally block should clean up staging.

    pipe = pipeline(name="p1", steps=[my_step], home=TEST_HOME)

    def mock_serialize_output(run_id, coordinate, result):
        # We manually write a file to staging to prove it gets cleaned up
        staging_path = TEST_HOME.store._staging_path(run_id, coordinate, "mockhash")
        os.makedirs(os.path.dirname(staging_path), exist_ok=True)
        with open(staging_path, "w") as f:
            f.write("staged_but_failed")

        # Then we raise an error!
        raise ValueError("Simulated failure during serialize_output")

    with patch.object(TEST_HOME.store, "serialize_output", side_effect=mock_serialize_output):
        summary = pipe.run(params={"content": "A"})

    assert summary.failed_count == 1

    # Assert staging directory for the run is gone!
    run_staging = os.path.join(TEST_HOME.store.staging_dir, summary.run_id)
    assert not os.path.exists(run_staging)

def test_staging_cleanup_on_commit_error():
    pipe = pipeline(name="p2", steps=[my_step], home=TEST_HOME)

    # Simulate a failure during the Arrow write (the new commit path)
    with patch.object(TEST_HOME.lanes, "append_filled", side_effect=RuntimeError("Simulated Arrow write failure")):
        summary = pipe.run(params={"content": "A"})

    assert summary.failed_count == 1

    run_staging = os.path.join(TEST_HOME.store.staging_dir, summary.run_id)
    assert not os.path.exists(run_staging)

