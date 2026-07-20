"""/api/pipelines is ledger-derived: a pipeline exists once it has run."""

import os

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from rubedo import step, pipeline
from rubedo.server import create_app
from conftest import isolated_test_env

TEST_FOLDER = ".test_pipelines_data"
ENV_FOLDER = ".test_pipelines_env"

TEST_HOME = None

client = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME, client
    with isolated_test_env("pipelines") as env:
        TEST_HOME = env.home
        client = TestClient(create_app(home=TEST_HOME))
        yield
        client = None

class MyParams(BaseModel):
    my_val: int = 7


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def make_pipeline():
    @step(
        name="my-step", version="v1", params_model=MyParams, retries=2,
        depends_on=["scan"],
    )
    def my_proc(scan: dict, params: MyParams):
        return {"val": params.my_val}

    return pipeline(name="test-proc", steps=[scan, my_proc], params_model=MyParams, home=TEST_HOME)


def test_unrun_pipelines_are_invisible():
    make_pipeline()  # defined but never run
    res = client.get("/api/pipelines")
    assert res.status_code == 200
    assert res.json() == []


def test_run_pipeline_appears_with_definition_snapshot():
    with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
        f.write("hello")

    pipe = make_pipeline()
    pipe.run(workers=1)
    pipe.run(workers=1)

    res = client.get("/api/pipelines")
    assert res.status_code == 200
    (item,) = res.json()
    assert item["id"] == "test-proc"
    assert item["run_count"] == 2
    # source_id is the sorted, comma-joined names of the pipeline's root
    # steps — here a single "scan" root.
    assert item["source_id"] == "scan"
    assert item["last_run_at"] is not None

    definition = item["definition"]
    assert definition["name"] == "test-proc"
    (step_def,) = [s for s in definition["steps"] if s["name"] == "my-step"]
    assert step_def["name"] == "my-step"
    assert step_def["version"] == "v1"
    assert step_def["retries"] == 2
    assert step_def["params_schema"]["properties"]["my_val"]["default"] == 7


def test_describe_renders_dag_without_running():
    pipe = make_pipeline()

    text = pipe.describe()
    assert "test-proc" in text
    # "scan" is the root now; "my-step" depends on it.
    assert "scan (1) (root)" in text
    assert "my-step (v1) <- scan" in text
    assert "retries=2" in text

    mermaid = pipe.describe(format="mermaid")
    assert mermaid.startswith("graph TD")
    assert 'my-step["my-step<br/>v1"]' in mermaid
