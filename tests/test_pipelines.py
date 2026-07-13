"""/api/pipelines is ledger-derived: a pipeline exists once it has run."""

import os
import shutil
import uuid

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo.server import app
from rubedo.store import init_store

TEST_FOLDER = ".test_pipelines_data"
ENV_FOLDER = ".test_pipelines_env"

client = TestClient(app)


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
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


class MyParams(BaseModel):
    my_val: int = 7


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content — the
    replacement for the old folder=TEST_FOLDER source sugar (TODO 14)."""
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

    return pipeline(name="test-proc", steps=[scan, my_proc], params_model=MyParams)


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
    # source_id is now the sorted, comma-joined names of the pipeline's root
    # steps — here a single "scan" root (TODO 14).
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
