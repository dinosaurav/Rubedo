"""Tests for batch_lookup_by_address — the planning phase's reuse lookup.

Two-step lookup (SQLite input_hash_usages for liveness, Arrow lane_store
for content) that replaces the single-step SQLite Materialization query.
These tests set up both sides and verify the batch lookup returns the
correct row dicts."""

from datetime import datetime, timezone, timedelta

import pytest
from conftest import isolated_test_env
from rubedo.models import InputHashUsage, Run
from rubedo.util import utcnow_iso


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("batch_lookup", with_data=False) as env:
        TEST_HOME = env.home
        TEST_HOME.lanes.clear_read_caches()
        yield
        TEST_HOME.lanes.clear_run_buffers()
        TEST_HOME = None

PIPE = "batch-pipe"
STEP = "extract"
RUN1 = "run_batch_1"


def _ts(minutes_ago: float = 0):
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


def _setup_usage(session, address, fulfilled, run_id=RUN1):
    run = session.query(Run).filter_by(id=run_id).first()
    if not run:
        session.add(Run(id=run_id, kind="pipeline", started_at=utcnow_iso(),
                        last_heartbeat_at=utcnow_iso()))
        session.flush()
    session.add(InputHashUsage(address=address, last_run_id=run_id, fulfilled=fulfilled))


def test_batch_lookup_returns_fulfilled_rows():
    with TEST_HOME.session() as s:
        _setup_usage(s, "addr_a", fulfilled=True)
        _setup_usage(s, "addr_b", fulfilled=True)
        s.commit()

    TEST_HOME.lanes.append_filled(PIPE, STEP, "lane_a", "addr_a", "ih_a", "ch_a", "json",
                  RUN1, code_hash="code_a", ts=_ts(minutes_ago=5))
    TEST_HOME.lanes.append_filled(PIPE, STEP, "lane_b", "addr_b", "ih_b", "ch_b", "json",
                  RUN1, code_hash="code_b", ts=_ts(minutes_ago=3))

    with TEST_HOME.session() as s:
        result = TEST_HOME.lanes.batch_lookup_by_address(PIPE, STEP, {"addr_a", "addr_b"}, s)

    assert "addr_a" in result
    assert result["addr_a"]["output"] == "ch_a"
    assert result["addr_a"]["code_hash"] == "code_a"
    assert "addr_b" in result
    assert result["addr_b"]["output"] == "ch_b"


def test_batch_lookup_skips_unfulfilled():
    with TEST_HOME.session() as s:
        _setup_usage(s, "addr_fulfilled", fulfilled=True)
        _setup_usage(s, "addr_unfulfilled", fulfilled=False)
        s.commit()

    TEST_HOME.lanes.append_filled(PIPE, STEP, "lane_f", "addr_fulfilled", "ih_f", "ch_f", "json",
                  RUN1, ts=_ts(minutes_ago=5))
    TEST_HOME.lanes.append_filled(PIPE, STEP, "lane_u", "addr_unfulfilled", "ih_u", "ch_u", "json",
                  RUN1, ts=_ts(minutes_ago=3))

    with TEST_HOME.session() as s:
        result = TEST_HOME.lanes.batch_lookup_by_address(
            PIPE, STEP, {"addr_fulfilled", "addr_unfulfilled"}, s
        )

    assert "addr_fulfilled" in result
    assert "addr_unfulfilled" not in result


def test_batch_lookup_skips_missing_arrow_row():
    with TEST_HOME.session() as s:
        _setup_usage(s, "addr_no_arrow", fulfilled=True)
        s.commit()

    with TEST_HOME.session() as s:
        result = TEST_HOME.lanes.batch_lookup_by_address(PIPE, STEP, {"addr_no_arrow"}, s)

    assert result == {}


def test_batch_lookup_empty_addresses():
    with TEST_HOME.session() as s:
        result = TEST_HOME.lanes.batch_lookup_by_address(PIPE, STEP, set(), s)
    assert result == {}


def test_batch_lookup_missing_address():
    with TEST_HOME.session() as s:
        _setup_usage(s, "addr_exists", fulfilled=True)
        s.commit()
    TEST_HOME.lanes.append_filled(PIPE, STEP, "lane_e", "addr_exists", "ih_e", "ch_e", "json",
                  RUN1, ts=_ts(minutes_ago=5))

    with TEST_HOME.session() as s:
        result = TEST_HOME.lanes.batch_lookup_by_address(
            PIPE, STEP, {"addr_exists", "addr_missing"}, s
        )

    assert "addr_exists" in result
    assert "addr_missing" not in result