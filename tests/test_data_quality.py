"""Tests for data quality assertions (`output_model` and `assertions`)."""

import os
import shutil
import uuid

import pytest
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import RunCoordinateStatus
from rubedo.store import init_store

TEST_FOLDER = ".test_dq_data"
ENV_FOLDER = ".test_dq_env"


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


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


class UserRecord(BaseModel):
    id: int
    name: str


def test_output_model_success():
    create_file("f1.txt", "1,alice")

    @step(name="parse", version="1", output_model=UserRecord, depends_on=["scan"])
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse])
    summary = pipe.run(workers=1)

    assert summary.created_count == 2  # scan's lane + parse's lane
    assert summary.failed_count == 0


def test_output_model_failure():
    create_file("f1.txt", "1,alice")

    # Missing the required "id" field in the return value
    @step(name="parse", version="1", output_model=UserRecord, depends_on=["scan"])
    def parse(scan):
        return {"name": "alice"}

    pipe = pipeline(name="p", steps=[scan, parse])
    summary = pipe.run(workers=1)

    # scan's own lane still succeeds; only the dependent parse lane fails.
    assert summary.created_count == 1
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="parse").one()
        assert rc.status == "failed"
        assert rc.error_message is not None
        assert "ValidationError" in rc.error_message


def test_assertions_success():
    create_file("f1.txt", "1,alice")

    def must_be_positive(val):
        assert val["id"] > 0, "ID must be positive"

    @step(name="parse", version="1", assertions=[must_be_positive], depends_on=["scan"])
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse])
    summary = pipe.run(workers=1)

    assert summary.created_count == 2  # scan's lane + parse's lane
    assert summary.failed_count == 0


def test_assertions_failure():
    create_file("f1.txt", "-5,alice")

    def must_be_positive(val):
        if val["id"] <= 0:
            raise ValueError("ID must be positive")

    @step(name="parse", version="1", assertions=[must_be_positive], depends_on=["scan"])
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse])
    summary = pipe.run(workers=1)

    # scan's own lane still succeeds; only the dependent parse lane fails.
    assert summary.created_count == 1
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="parse").one()
        assert rc.status == "failed"
        assert rc.error_message is not None
        assert "ID must be positive" in rc.error_message


def test_expand_step_validation():
    class ItemModel(BaseModel):
        num: int

    # `produce` is itself a root expand (no depends_on): it needs no scan/
    # folder recipe at all, so unlike the other tests in this file this
    # pipeline stays single-step.
    @step(name="produce", version="1", shape="expand", output_model=ItemModel)
    def produce():
        yield {"num": 1}
        yield {"bad_key": 2} # This should fail validation

    pipe = pipeline(name="p", steps=[produce])
    summary = pipe.run(workers=1)

    # Expand step runs once per parent. The entire parent execution fails if any child fails.
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).one()
        assert rc.status == "failed"
        assert rc.error_message is not None
        assert "ValidationError" in rc.error_message
