import os
import shutil
import pytest

from rubedo import step, pipeline
from rubedo.db import init_db

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
        
    import rubedo.store
    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
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

# Folder recipe: walk TEST_FOLDER, yield each file's content. Must
# also be module-level since it's shared across process-executor pipelines.
@step
def scan():
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

# Process executor steps must be module-level
def process_ok(scan):
    import time
    time.sleep(0.1) # tiny sleep to ensure concurrency doesn't blow up
    return scan["text"].lower()

# Counter for retries - file based so it works across processes
def process_fail(scan):
    basename = scan["path"]
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
    step1 = step(name="ok", executor="process")(process_ok)

    pipe = pipeline(name="p1", steps=[scan, step1])
    summary = pipe.run(workers=2)

    # 2 scan lanes + 2 "ok" lanes
    assert summary.created_count == 4
    assert summary.reused_count == 0

    # Check that they can be reused
    summary2 = pipe.run(workers=2)
    assert summary2.created_count == 0
    assert summary2.reused_count == 4

def test_process_executor_closure_ok():
    # Local closure should work fine with loky + cloudpickle
    prefix = "Closure: "
    def my_local_func(scan):
        return prefix + scan["text"]

    step_local = step(name="local", executor="process")(my_local_func)
    pipe = pipeline(name="p4", steps=[scan, step_local])
    summary = pipe.run(workers=2)
    # 2 scan lanes + 2 "local" lanes
    assert summary.created_count == 4
    assert summary.failed_count == 0

def test_process_executor_retries():
    # Retries happen in the parent thread pool and submit to process pool
    step_fail = step(name="fail", executor="process", retries=2)(process_fail)

    pipe = pipeline(name="p2", steps=[scan, step_fail])
    summary = pipe.run(workers=2)

    # 2 scan lanes + 2 "fail" lanes
    assert summary.created_count == 4
    assert summary.failed_count == 0
    
    # Read the counter files to ensure they actually retried
    with open(os.path.join(TEST_FOLDER, "fail_count_a.txt"), "r") as f:
        assert f.read().strip() == "2"
    with open(os.path.join(TEST_FOLDER, "fail_count_b.txt"), "r") as f:
        assert f.read().strip() == "2"

class Unpicklable:
    def __reduce__(self):
        raise TypeError("Cannot pickle me")

def process_unpicklable(scan):
    return Unpicklable()

def test_process_executor_pickling_error():
    # If the process pool cannot pickle the return value, it should surface as a step failure.
    step_unpicklable = step(name="unpicklable", executor="process")(process_unpicklable)
    pipe = pipeline(name="p3", steps=[scan, step_unpicklable])

    summary = pipe.run(workers=2)
    assert summary.failed_count == 2
    # scan's 2 lanes still materialize fine; only "unpicklable" fails.
    assert summary.created_count == 2
