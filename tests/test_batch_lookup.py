"""Tests for batch_lookup_by_address — the planning phase's reuse lookup.

Two-step lookup (SQLite input_hash_usages for liveness, Arrow lane_store
for content) that replaces the single-step SQLite Materialization query.
These tests set up both sides and verify the batch lookup returns the
correct row dicts."""

import os
import shutil
import uuid
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo.db import init_db, get_session
from rubedo.lane_store import (
    append_filled,
    batch_lookup_by_address,
    clear_run_buffers,
    init_tables,
)
from rubedo.models import InputHashUsage, Run
from rubedo.util import utcnow_iso


@pytest.fixture(autouse=True)
def isolated_env():
    abs_env = os.path.abspath(".test_batch_lookup_env")
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)
    os.makedirs(abs_env, exist_ok=True)
    import rubedo.lane_store
    rubedo.lane_store.TABLES_DIR = f"{abs_env}/tables"
    init_tables()
    clear_run_buffers()

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_batch_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()
    import rubedo.db
    rubedo.db._engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    yield
    clear_run_buffers()
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)


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
    with get_session() as s:
        _setup_usage(s, "addr_a", fulfilled=True)
        _setup_usage(s, "addr_b", fulfilled=True)
        s.commit()

    append_filled(PIPE, STEP, "lane_a", "addr_a", "ih_a", "ch_a", "json",
                  RUN1, code_hash="code_a", ts=_ts(minutes_ago=5))
    append_filled(PIPE, STEP, "lane_b", "addr_b", "ih_b", "ch_b", "json",
                  RUN1, code_hash="code_b", ts=_ts(minutes_ago=3))

    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, {"addr_a", "addr_b"}, s)

    assert "addr_a" in result
    assert result["addr_a"]["output"] == "ch_a"
    assert result["addr_a"]["code_hash"] == "code_a"
    assert "addr_b" in result
    assert result["addr_b"]["output"] == "ch_b"


def test_batch_lookup_skips_unfulfilled():
    with get_session() as s:
        _setup_usage(s, "addr_fulfilled", fulfilled=True)
        _setup_usage(s, "addr_unfulfilled", fulfilled=False)
        s.commit()

    append_filled(PIPE, STEP, "lane_f", "addr_fulfilled", "ih_f", "ch_f", "json",
                  RUN1, ts=_ts(minutes_ago=5))
    append_filled(PIPE, STEP, "lane_u", "addr_unfulfilled", "ih_u", "ch_u", "json",
                  RUN1, ts=_ts(minutes_ago=3))

    with get_session() as s:
        result = batch_lookup_by_address(
            PIPE, STEP, {"addr_fulfilled", "addr_unfulfilled"}, s
        )

    assert "addr_fulfilled" in result
    assert "addr_unfulfilled" not in result


def test_batch_lookup_skips_missing_arrow_row():
    with get_session() as s:
        _setup_usage(s, "addr_no_arrow", fulfilled=True)
        s.commit()

    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, {"addr_no_arrow"}, s)

    assert result == {}


def test_batch_lookup_empty_addresses():
    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, set(), s)
    assert result == {}


def test_batch_lookup_missing_address():
    with get_session() as s:
        _setup_usage(s, "addr_exists", fulfilled=True)
        s.commit()
    append_filled(PIPE, STEP, "lane_e", "addr_exists", "ih_e", "ch_e", "json",
                  RUN1, ts=_ts(minutes_ago=5))

    with get_session() as s:
        result = batch_lookup_by_address(
            PIPE, STEP, {"addr_exists", "addr_missing"}, s
        )

    assert "addr_exists" in result
    assert "addr_missing" not in result