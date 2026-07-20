"""Tests for data quality assertions (`output_model` and `assertions`)."""

import os

import pytest
from pydantic import BaseModel

from rubedo import step, pipeline
from rubedo.models import RunCoordinateStatus
from conftest import isolated_test_env

TEST_FOLDER = ".test_dq_data"
ENV_FOLDER = ".test_dq_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("dq") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step
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

    @step(output_model=UserRecord)
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse], home=TEST_HOME)
    summary = pipe.run(workers=1)

    assert summary.created_count == 2  # scan's lane + parse's lane
    assert summary.failed_count == 0


def test_output_model_failure():
    create_file("f1.txt", "1,alice")

    # Missing the required "id" field in the return value
    @step(output_model=UserRecord)
    def parse(scan):
        return {"name": "alice"}

    pipe = pipeline(name="p", steps=[scan, parse], home=TEST_HOME)
    summary = pipe.run(workers=1)

    # scan's own lane still succeeds; only the dependent parse lane fails.
    assert summary.created_count == 1
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="parse").one()
        assert rc.status == "failed"
        assert rc.error_message is not None
        assert "ValidationError" in rc.error_message


def test_assertions_success():
    create_file("f1.txt", "1,alice")

    def must_be_positive(val):
        assert val["id"] > 0, "ID must be positive"

    @step(assertions=[must_be_positive])
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse], home=TEST_HOME)
    summary = pipe.run(workers=1)

    assert summary.created_count == 2  # scan's lane + parse's lane
    assert summary.failed_count == 0


def test_assertions_failure():
    create_file("f1.txt", "-5,alice")

    def must_be_positive(val):
        if val["id"] <= 0:
            raise ValueError("ID must be positive")

    @step(assertions=[must_be_positive])
    def parse(scan):
        parts = scan["text"].split(",")
        return {"id": int(parts[0]), "name": parts[1]}

    pipe = pipeline(name="p", steps=[scan, parse], home=TEST_HOME)
    summary = pipe.run(workers=1)

    # scan's own lane still succeeds; only the dependent parse lane fails.
    assert summary.created_count == 1
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
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
    @step(output_model=ItemModel)
    def produce():
        yield {"num": 1}
        yield {"bad_key": 2} # This should fail validation

    pipe = pipeline(name="p", steps=[produce], home=TEST_HOME)
    summary = pipe.run(workers=1)

    # Expand step runs once per parent. The entire parent execution fails if any child fails.
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
        rc = session.query(RunCoordinateStatus).one()
        assert rc.status == "failed"
        assert rc.error_message is not None
        assert "ValidationError" in rc.error_message
