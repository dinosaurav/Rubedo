import os
import shutil
import pytest

from batchbrain import run, step, pipeline
from batchbrain.db import init_db

TEST_FOLDER = ".test_process_data"
ENV_FOLDER = ".test_process_env"

@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
        
    import batchbrain.store
    batchbrain.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    batchbrain.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["BATCHBRAIN_DB_PATH"] = (
        "sqlite:///:memory:?cache=shared"
    )
    init_db()
    
    with open(os.path.join(abs_test_folder, "a.txt"), "w") as f:
        f.write("A")
    with open(os.path.join(abs_test_folder, "b.txt"), "w") as f:
        f.write("B")
        
    yield
    
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)

# Process executor steps must be module-level
def process_ok(content):
    import time
    time.sleep(0.1) # tiny sleep to ensure concurrency doesn't blow up
    return content.lower()

# Counter for retries - file based so it works across processes
def process_fail(content):
    basename = os.path.basename(content)
    counter_file = os.path.join(os.path.abspath(TEST_FOLDER), f"fail_count_{basename}")
    count = 0
    if os.path.exists(counter_file):
        with open(counter_file, "r") as f:
            count = int(f.read().strip())
    
    count += 1
    with open(counter_file, "w") as f:
        f.write(str(count))
        
    if count < 2:
        raise ValueError("simulated failure")
        
    return f"success after {count} tries"

def test_process_executor_basic():
    # Register steps
    step1 = step(name="ok", version="1", executor="process")(process_ok)
    
    pipe = pipeline(id="p1", name="p1", folder=TEST_FOLDER, steps=[step1])
    summary = run(pipe, workers=2)
    
    assert summary.created_count == 2
    assert summary.reused_count == 0
    
    # Check that they can be reused
    summary2 = run(pipe, workers=2)
    assert summary2.created_count == 0
    assert summary2.reused_count == 2

def test_process_executor_registration_rejection():
    # Local closure
    def my_local_func(content):
        return content
        
    with pytest.raises(ValueError, match="process-executor steps must be module-level"):
        step(name="local", version="1", executor="process")(my_local_func)
        
def test_process_executor_retries():
    # Retries happen in the parent thread pool and submit to process pool
    step_fail = step(name="fail", version="1", executor="process", retries=2)(process_fail)
    
    pipe = pipeline(id="p2", name="p2", folder=TEST_FOLDER, steps=[step_fail])
    summary = run(pipe, workers=2)
    
    assert summary.created_count == 2
    assert summary.failed_count == 0
    
    # Read the counter files to ensure they actually retried
    with open(os.path.join(TEST_FOLDER, "fail_count_a.txt"), "r") as f:
        assert f.read().strip() == "2"
    with open(os.path.join(TEST_FOLDER, "fail_count_b.txt"), "r") as f:
        assert f.read().strip() == "2"

class Unpicklable:
    def __reduce__(self):
        raise TypeError("Cannot pickle me")

def process_unpicklable(content):
    return Unpicklable()

def test_process_executor_pickling_error():
    # If the process pool cannot pickle the return value, it should surface as a step failure.
    step_unpicklable = step(name="unpicklable", version="1", executor="process")(process_unpicklable)
    pipe = pipeline(id="p3", name="p3", folder=TEST_FOLDER, steps=[step_unpicklable])
    
    summary = run(pipe, workers=2)
    assert summary.failed_count == 2
    assert summary.created_count == 0
