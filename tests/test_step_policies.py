"""Step policies: retries (with retry_on filtering) and rate limits."""

import json
import os
import shutil
import time
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from batchbrain import run, step, pipeline
from batchbrain.db import init_db, get_session
from batchbrain.models import RunCoordinateStatus, RunEvent
from batchbrain.registry import clear_registry, parse_rate_limit
from batchbrain.store import init_store

TEST_FOLDER = ".test_policies_data"
ENV_FOLDER = ".test_policies_env"


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
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import batchbrain.db

    if batchbrain.db.engine is not None:
        batchbrain.db.engine.dispose()

    from batchbrain.models import Base
    from sqlalchemy.orm import sessionmaker

    batchbrain.db.engine = create_engine(
        os.environ["BATCHBRAIN_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=batchbrain.db.engine)
    batchbrain.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=batchbrain.db.engine
    )

    init_store()
    clear_registry()

    yield

    clear_registry()
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def test_retries_until_success():
    create_file("f1.txt", "hello")
    calls = {"n": 0}

    @step(name="flaky", version="1", retries=2, retry_on=TimeoutError)
    def flaky(path):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("transient")
        return "ok"

    pipeline(id="p", name="p", folder=TEST_FOLDER, steps=[flaky])
    summary = run("p", workers=1)

    assert calls["n"] == 3
    assert (summary.created_count, summary.failed_count) == (1, 0)

    with get_session() as session:
        attempt_events = (
            session.query(RunEvent).filter_by(event_type="step_attempt_failed").all()
        )
        assert len(attempt_events) == 2
        assert json.loads(attempt_events[0].data_json) == {
            "attempt": 1,
            "max_attempts": 3,
        }

        rc = session.query(RunCoordinateStatus).one()
        assert json.loads(rc.metadata_json) == {"attempts": 3}


def test_retries_exhausted_records_failure():
    create_file("f1.txt", "hello")

    @step(name="doomed", version="1", retries=1, retry_on=TimeoutError)
    def doomed(path):
        raise TimeoutError("always")

    pipeline(id="p", name="p", folder=TEST_FOLDER, steps=[doomed])
    summary = run("p", workers=1)

    assert summary.failed_count == 1
    with get_session() as session:
        assert (
            session.query(RunEvent)
            .filter_by(event_type="step_attempt_failed")
            .count()
            == 1
        )
        rc = session.query(RunCoordinateStatus).one()
        assert rc.status == "failed"
        assert json.loads(rc.metadata_json) == {"attempts": 2}


def test_retry_on_filters_exception_types():
    create_file("f1.txt", "hello")
    calls = {"n": 0}

    @step(name="buggy", version="1", retries=5, retry_on=TimeoutError)
    def buggy(path):
        calls["n"] += 1
        raise ValueError("deterministic bug — retrying just multiplies cost")

    pipeline(id="p", name="p", folder=TEST_FOLDER, steps=[buggy])
    summary = run("p", workers=1)

    assert calls["n"] == 1, "non-matching exception must not be retried"
    assert summary.failed_count == 1


def test_rate_limit_paces_execution():
    for i in range(4):
        create_file(f"f{i}.txt", f"content-{i}")

    @step(name="polite", version="1", rate_limit="10/s")
    def polite(path):
        return "done"

    pipeline(id="p", name="p", folder=TEST_FOLDER, steps=[polite])

    start = time.monotonic()
    summary = run("p", workers=4)
    elapsed = time.monotonic() - start

    assert summary.created_count == 4
    # 4 calls at 10/s = at least 3 intervals of 0.1s
    assert elapsed >= 0.28


def test_rate_limit_parsing():
    assert parse_rate_limit("10/min") == (10, 60.0)
    assert parse_rate_limit("2 / s") == (2, 1.0)
    assert parse_rate_limit("500/hour") == (500, 3600.0)
    with pytest.raises(ValueError, match="Invalid rate_limit"):
        parse_rate_limit("fast")
    with pytest.raises(ValueError, match="Invalid rate_limit"):
        parse_rate_limit("10/fortnight")


def test_bad_rate_limit_rejected_at_registration():
    with pytest.raises(ValueError, match="Invalid rate_limit"):

        @step(name="x", version="1", rate_limit="oops")
        def x(path):
            pass
