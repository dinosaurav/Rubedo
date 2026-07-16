"""Step policies: retries (with retry_on filtering) and rate limits."""

import json
import os
import shutil
import time
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import RunCoordinateStatus, RunEvent
from rubedo.spec import parse_rate_limit
from rubedo.store import init_store

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
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


def test_retries_until_success():
    path = create_file("f1.txt", "hello")
    calls = {"n": 0}

    @step(retries=2, retry_on=TimeoutError)
    def flaky(params):
        calls["n"] += 1
        if calls["n"] < 3:
            raise TimeoutError("transient")
        return "ok"

    pipe = pipeline(name="p", steps=[flaky])
    summary = pipe.run(params={"path": path}, workers=1)

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
    path = create_file("f1.txt", "hello")

    @step(retries=1, retry_on=TimeoutError)
    def doomed(params):
        raise TimeoutError("always")

    pipe = pipeline(name="p", steps=[doomed])
    summary = pipe.run(params={"path": path}, workers=1)

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
    path = create_file("f1.txt", "hello")
    calls = {"n": 0}

    @step(retries=5, retry_on=TimeoutError)
    def buggy(params):
        calls["n"] += 1
        raise ValueError("deterministic bug — retrying just multiplies cost")

    pipe = pipeline(name="p", steps=[buggy])
    summary = pipe.run(params={"path": path}, workers=1)

    assert calls["n"] == 1, "non-matching exception must not be retried"
    assert summary.failed_count == 1


def test_rate_limit_paces_execution():
    for i in range(4):
        create_file(f"f{i}.txt", f"content-{i}")

    # Needs genuine multi-lane parallelism, so a folder-scan expand root
    # rather than a single param-fed lane.
    @step
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step(rate_limit="10/s")
    def polite(scan):
        return "done"

    pipe = pipeline(name="p", steps=[scan, polite])

    start = time.monotonic()
    summary = pipe.run(workers=4)
    elapsed = time.monotonic() - start

    assert summary.created_count == 8  # 4 scan + 4 polite
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

        @step(rate_limit="oops")
        def x(params):
            pass


# ---------- staleness ----------


def test_duration_parsing():
    from rubedo.spec import parse_duration

    assert parse_duration("30s") == 30.0
    assert parse_duration("15min") == 900.0
    assert parse_duration("24h") == 86400.0
    assert parse_duration("7d") == 604800.0
    assert parse_duration("1.5h") == 5400.0
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("soon")


def backdate_materializations(iso_timestamp):
    """Raw SQL on purpose: ORM guards forbid mutating created_at."""
    from sqlalchemy import text

    with get_session() as session:
        session.execute(
            text("UPDATE materializations SET created_at = :ts"),
            {"ts": iso_timestamp},
        )
        session.commit()


def test_fresh_output_is_reused():
    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape])
    params = {"path": path}
    pipe.run(params=params, workers=1)
    summary = pipe.run(params=params, workers=1)
    assert (summary.created_count, summary.reused_count) == (0, 1)


def test_expired_deterministic_output_is_refreshed():
    from rubedo.models import Materialization

    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape])
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    # Expired -> re-executes; identical bytes -> refreshed, not a new row
    summary = pipe.run(params=params, workers=1)
    assert (summary.created_count, summary.reused_count) == (1, 0)

    with get_session() as session:
        mat = session.query(Materialization).one()
        assert mat.refreshed_at is not None

    # refreshed_at reset the clock: next run reuses again
    summary3 = pipe.run(params=params, workers=1)
    assert (summary3.created_count, summary3.reused_count) == (0, 1)


def test_expired_nondeterministic_output_is_superseded():
    import itertools

    from rubedo.models import Materialization

    path = create_file("f1.txt", "hello")
    counter = itertools.count()

    @step(stale_after="1h")
    def scrape(params):
        return {"attempt": next(counter)}

    pipe = pipeline(name="p", steps=[scrape])
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    summary = pipe.run(params=params, workers=1)
    assert summary.created_count == 1

    with get_session() as session:
        mats = session.query(Materialization).order_by(Materialization.id).all()
        assert len(mats) == 2
        # Old generation's is_live flipped for the unique index, but
        # liveness is input_hash_usages.fulfilled — both rows exist as
        # history in the lane_store.
        assert mats[0].is_live is False, "old generation superseded"
        assert mats[1].is_live is True


def test_staleness_visible_in_plan():
    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape])
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    p = pipe.plan(params=params)
    assert p.counts == {"execute": 1}
