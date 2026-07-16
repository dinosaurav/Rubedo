"""Tests for batch_lookup_by_address — the planning phase's reuse lookup.

This is the two-step lookup (SQLite input_hash_usages for liveness,
Arrow lane_store for content) that replaces the single-step SQLite
`Materialization.filter(output_address IN (...), is_live=True)` query.
These tests set up both sides and verify the lookup returns the correct
MatRef-compatible row dicts."""

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


def _setup_usage(session, address, lane_key, fulfilled, run_id=RUN1):
    run = session.query(Run).filter_by(id=run_id).first()
    if not run:
        session.add(Run(id=run_id, kind="pipeline", started_at=utcnow_iso(),
                        last_heartbeat_at=utcnow_iso()))
        session.flush()
    session.add(
        InputHashUsage(
            address=address,
            lane_key=lane_key,
            step_name=STEP,
            pipeline_id=PIPE,
            last_run_id=run_id,
            claimed_at=utcnow_iso(),
            fulfilled=fulfilled,
        )
    )


def test_batch_lookup_returns_fulfilled_rows():
    """Addresses with fulfilled=True and an Arrow row are returned."""
    with get_session() as s:
        _setup_usage(s, "addr_a", "lane_a", fulfilled=True)
        _setup_usage(s, "addr_b", "lane_b", fulfilled=True)
        s.commit()

    append_filled(PIPE, STEP, "lane_a", "addr_a", "ih_a", "ch_a", "json", "obj/a",
                  RUN1, code_hash="code_a", ts=_ts(minutes_ago=5))
    append_filled(PIPE, STEP, "lane_b", "addr_b", "ih_b", "ch_b", "json", "obj/b",
                  RUN1, code_hash="code_b", ts=_ts(minutes_ago=3))

    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, {"addr_a", "addr_b"}, s)

    assert "addr_a" in result
    assert result["addr_a"]["content_hash"] == "ch_a"
    assert result["addr_a"]["code_hash"] == "code_a"
    assert "addr_b" in result
    assert result["addr_b"]["content_hash"] == "ch_b"


def test_batch_lookup_skips_unfulfilled():
    """Addresses with fulfilled=False are NOT returned (recompute)."""
    with get_session() as s:
        _setup_usage(s, "addr_fulfilled", "lane_f", fulfilled=True)
        _setup_usage(s, "addr_unfulfilled", "lane_u", fulfilled=False)
        s.commit()

    append_filled(PIPE, STEP, "lane_f", "addr_fulfilled", "ih_f", "ch_f", "json",
                  "obj/f", RUN1, ts=_ts(minutes_ago=5))
    append_filled(PIPE, STEP, "lane_u", "addr_unfulfilled", "ih_u", "ch_u", "json",
                  "obj/u", RUN1, ts=_ts(minutes_ago=3))

    with get_session() as s:
        result = batch_lookup_by_address(
            PIPE, STEP, {"addr_fulfilled", "addr_unfulfilled"}, s
        )

    assert "addr_fulfilled" in result
    assert "addr_unfulfilled" not in result


def test_batch_lookup_skips_missing_arrow_row():
    """If fulfilled=True but no Arrow row exists (edge case — file deleted),
    the address is not returned (can't reuse without content)."""
    with get_session() as s:
        _setup_usage(s, "addr_no_arrow", "lane_na", fulfilled=True)
        s.commit()
    # No append_filled — Arrow file is empty

    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, {"addr_no_arrow"}, s)

    assert result == {}


def test_batch_lookup_empty_addresses():
    """An empty address set returns an empty dict."""
    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, set(), s)
    assert result == {}


def test_batch_lookup_missing_address():
    """An address not in input_hash_usages is a miss."""
    with get_session() as s:
        _setup_usage(s, "addr_exists", "lane_e", fulfilled=True)
        s.commit()
    append_filled(PIPE, STEP, "lane_e", "addr_exists", "ih_e", "ch_e", "json",
                  "obj/e", RUN1, ts=_ts(minutes_ago=5))

    with get_session() as s:
        result = batch_lookup_by_address(
            PIPE, STEP, {"addr_exists", "addr_missing"}, s
        )

    assert "addr_exists" in result
    assert "addr_missing" not in result


def test_batch_lookup_isolates_by_pipeline_and_step():
    """The lookup filters by pipeline_id and step_name — the same address
    in a different pipeline or step is not returned."""
    with get_session() as s:
        _setup_usage(s, "addr_shared", "lane_s", fulfilled=True)
        # Same address, different pipeline
        s.add(Run(id="run_other", kind="pipeline", started_at=utcnow_iso(),
                   last_heartbeat_at=utcnow_iso()))
        s.add(
            InputHashUsage(
                address="addr_shared",
                lane_key="lane_other",
                step_name=STEP,
                pipeline_id="other-pipe",
                last_run_id="run_other",
                claimed_at=utcnow_iso(),
                fulfilled=True,
            )
        )
        s.commit()

    append_filled(PIPE, STEP, "lane_s", "addr_shared", "ih_s", "ch_s", "json",
                  "obj/s", RUN1, ts=_ts(minutes_ago=5))
    append_filled("other-pipe", STEP, "lane_other", "addr_shared", "ih_o", "ch_o",
                  "json", "obj/o", "run_other", ts=_ts(minutes_ago=5))

    with get_session() as s:
        result = batch_lookup_by_address(PIPE, STEP, {"addr_shared"}, s)

    assert "addr_shared" in result
    assert result["addr_shared"]["content_hash"] == "ch_s"  # not ch_o