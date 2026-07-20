"""Step policies: retries (with retry_on filtering) and rate limits."""

import json
import os
import time

import pytest

from rubedo import step, pipeline
from rubedo.models import RunCoordinateStatus, RunEvent
from rubedo.spec import parse_rate_limit
from conftest import isolated_test_env

TEST_FOLDER = ".test_policies_data"
ENV_FOLDER = ".test_policies_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("policies") as env:
        TEST_HOME = env.home
        yield

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

    pipe = pipeline(name="p", steps=[flaky], home=TEST_HOME)
    summary = pipe.run(params={"path": path}, workers=1)

    assert calls["n"] == 3
    assert (summary.created_count, summary.failed_count) == (1, 0)

    with TEST_HOME.session() as session:
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

    pipe = pipeline(name="p", steps=[doomed], home=TEST_HOME)
    summary = pipe.run(params={"path": path}, workers=1)

    assert summary.failed_count == 1
    with TEST_HOME.session() as session:
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

    pipe = pipeline(name="p", steps=[buggy], home=TEST_HOME)
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

    pipe = pipeline(name="p", steps=[scan, polite], home=TEST_HOME)

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
    """Backdates the Arrow lane_store ts column, which is what the
    planning staleness check reads."""
    from datetime import datetime

    # Backdate the Arrow rows' ts column in every step file
    dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
    for row in TEST_HOME.lanes.all_filled_rows():
        pipe = row.get("pipeline_id", "")
        step = row.get("step_name", "")
        table = TEST_HOME.lanes._combined_table(pipe, step)
        if table is None or table.num_rows == 0:
            continue
        import pyarrow as pa

        ts_idx = table.column_names.index("ts")
        new_ts = pa.array([dt] * table.num_rows, type=pa.timestamp("us", tz="UTC"))
        table = table.set_column(ts_idx, "ts", new_ts)
        path = TEST_HOME.lanes._get_step_file(pipe, step)
        import os

        os.makedirs(os.path.dirname(path), exist_ok=True)
        with pa.ipc.new_file(path, table.schema) as writer:
            writer.write_table(table)

    # Invalidate the read cache — the Arrow files changed on disk.
    TEST_HOME.lanes.clear_read_caches()
    TEST_HOME.lanes.clear_run_buffers()


def test_fresh_output_is_reused():
    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape], home=TEST_HOME)
    params = {"path": path}
    pipe.run(params=params, workers=1)
    summary = pipe.run(params=params, workers=1)
    assert (summary.created_count, summary.reused_count) == (0, 1)


def test_expired_deterministic_output_is_refreshed():
    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape], home=TEST_HOME)
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    # Expired -> re-executes; identical bytes -> refreshed, not a new row
    summary = pipe.run(params=params, workers=1)
    assert (summary.created_count, summary.reused_count) == (1, 0)

    # refreshed_at reset the clock: next run reuses again
    summary3 = pipe.run(params=params, workers=1)
    assert (summary3.created_count, summary3.reused_count) == (0, 1)


def test_expired_nondeterministic_output_is_superseded():
    import itertools

    path = create_file("f1.txt", "hello")
    counter = itertools.count()

    @step(stale_after="1h")
    def scrape(params):
        return {"attempt": next(counter)}

    pipe = pipeline(name="p", steps=[scrape], home=TEST_HOME)
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    summary = pipe.run(params=params, workers=1)
    assert summary.created_count == 1

    from rubedo.models import InputHashUsage

    # Two Arrow rows (two generations), one IHU fulfilled (the new one)
    rows = TEST_HOME.lanes.all_filled_rows()
    assert len(rows) == 2
    with TEST_HOME.session() as session:
        fulfilled = session.query(InputHashUsage).filter(InputHashUsage.fulfilled.is_(True)).count()
        assert fulfilled == 1


def test_staleness_visible_in_plan():
    path = create_file("f1.txt", "hello")

    @step(stale_after="1h")
    def scrape(params):
        return open(params["path"]).read()

    pipe = pipeline(name="p", steps=[scrape], home=TEST_HOME)
    params = {"path": path}
    pipe.run(params=params, workers=1)
    backdate_materializations("2020-01-01T00:00:00Z")

    p = pipe.plan(params=params)
    assert p.counts == {"execute": 1}
