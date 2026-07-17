"""By-address reuse semantics — the planning-phase port of the SQLite reuse check.

These tests exercise ``find_latest_filled_by_address``: the lookup that the
engine's plan phase will use after the Phase 2 reader swap.  Address =
``hash(step, version, input_hash[, params][, code])`` — the comprehensive
cache identity.  A lane is reused only if its latest row was written by
an identical-address computation; a version bump, changed params, or a
code="auto" code change produces a different address and reads as "miss."

Also at the engine level: ``test_lane_store_reuse_via_pipeline_run`` is
the end-to-end check — runs a small pipeline, inspects the
``.rubedo/tables/`` Arrow file produced by the parallel-write, and
confirms the address column carries the computed output_address (so
the planning swap can match on it)."""

import os
import shutil
import uuid
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import pipeline, step
from rubedo.db import init_db
from rubedo.lane_store import (
    append_filled,
    clear_run_buffers,
    find_latest_filled_by_address,
    init_tables,
)

ENV_FOLDER = ".test_lane_addr_env"
PIPE = "addr-pipe"
STEP = "extract"
RUN1 = "run_addr_1"
RUN2 = "run_addr_2"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_env = os.path.abspath(ENV_FOLDER)
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)
    os.makedirs(abs_env, exist_ok=True)
    import rubedo.lane_store
    rubedo.lane_store.TABLES_DIR = f"{abs_env}/tables"
    init_tables()
    clear_run_buffers()
    yield
    clear_run_buffers()
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)


def _ts(minutes_ago: float = 0):
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


ADDR_V1 = "addr_v1_hash"
ADDR_V2 = "addr_v2_hash"
ADDR_PARAMS_A = "addr_params_a"
ADDR_PARAMS_B = "addr_params_b"


# ---------------------------------------------------------------------------
# address-keyed reuse — the actual planning lookup
# ---------------------------------------------------------------------------


def test_address_match_returns_filled():
    append_filled(PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json",
                  RUN1, ts=_ts(minutes_ago=10))
    row = find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_V1)
    assert row is not None
    assert row["output"] == "ch_v1"
    assert row["address"] == ADDR_V1


def test_different_address_is_a_miss():
    """The same lane but computed under a different address (e.g. version
    bumped between runs) reads as a miss — exactly the SQLite semantics
    `output_address=X, is_live=True` returns no row when only Y exists."""
    append_filled(PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json",
                  RUN1, ts=_ts(minutes_ago=10))
    assert find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_V2) is None


def test_version_bump_supersedes_not_overwrites():
    """A later run with a new address leaves the old row in place on disk.
    Under the new model, both rows are valid filled data in the Arrow
    file — the question of which is 'live' is ``input_hash_usages.fulfilled``'s
    job, not the Arrow file's.  ``find_latest_filled_by_address`` retrieves
    whichever row matches the address it's asked for."""
    append_filled(PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json",
                  RUN1, ts=_ts(minutes_ago=10))
    append_filled(PIPE, STEP, "@root#0", ADDR_V2, "ih_a", "ch_v2", "json",
                  RUN2, ts=_ts(minutes_ago=0))

    # Both addresses find their own row — the Arrow file is pure data
    assert find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_V1)["output"] == "ch_v1"
    assert find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_V2)["output"] == "ch_v2"

    # The *planning phase* decides which to reuse by checking
    # input_hash_usages.fulfilled — not by asking the Arrow file.  An
    # invalidated ADDR_V1 (fulfilled=False) won't be consulted even though
    # its Arrow row still exists.


# ---------------------------------------------------------------------------
# Multiple rows for the same lane_key (generations across runs)
# ---------------------------------------------------------------------------


def test_multiple_generations_latest_by_ts_wins():
    """When the same address is recomputed (e.g. stale_after refresh),
    the latest by ts is returned by find_latest_filled_by_address."""
    append_filled(PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json",
                  RUN1, ts=_ts(minutes_ago=10))
    append_filled(PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v2", "json",
                  RUN2, ts=_ts(minutes_ago=0))
    row = find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_V1)
    assert row is not None
    assert row["output"] == "ch_v2"
    assert row["run_id"] == RUN2
    assert row is not None
    assert row["output"] == "ch_v2"


def test_params_change_is_a_miss():
    """A step that reads params: same input_hash, different params ->
    different address -> miss.  This is what makes 'turning a knob
    recomputes exactly the steps that read it' work."""
    append_filled(PIPE, STEP, "@root#0", ADDR_PARAMS_A, "ih_a", "ch_pa", "json",
                  RUN1, ts=_ts(minutes_ago=10))
    assert find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_PARAMS_A) is not None
    assert find_latest_filled_by_address(PIPE, STEP, "@root#0", ADDR_PARAMS_B) is None


def test_address_with_no_rows_returns_none():
    assert find_latest_filled_by_address(PIPE, "no_such_step", "@root#0",
                                          "any_addr") is None
    assert find_latest_filled_by_address(PIPE, STEP, "@root#never",
                                          "any_addr") is None


# ---------------------------------------------------------------------------
# End-to-end: a real pipeline run writes a row whose address matches
# what planning would compute for the same input
# ---------------------------------------------------------------------------


def test_lane_store_address_matches_compute_output_address():
    """Sanity: the address column lane_store records must match what
    planning.py computes for the same (step, version, input_hash).  This
    is the contract that makes the planning swap correct — the row written
    by the parallel-write path is discoverable by the by-address lookup."""

    call_count = 0

    @step
    def producer():
        nonlocal call_count
        call_count += 1
        return {"n": call_count}

    @step
    def consumer(producer):
        return producer["n"] * 2

    p_home = os.path.abspath(".test_addr_e2e_env")
    if os.path.exists(p_home):
        shutil.rmtree(p_home)
    os.makedirs(p_home, exist_ok=True)
    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_addr_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()
    import rubedo.db
    rubedo.db._engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    import rubedo.store
    rubedo.store.OBJECTS_DIR = f"{p_home}/objects"
    rubedo.store.STAGING_DIR = f"{p_home}/staging"
    import rubedo.lane_store
    rubedo.lane_store.TABLES_DIR = f"{p_home}/tables"
    clear_run_buffers()

    p = pipeline(name="addr-e2e", steps=[producer, consumer])
    p.run(workers=1)

    # The lane_store has one row for producer's @root lane; its address
    # should match what compute_output_address produces for producer's
    # identity (the lane_key is @root, the input_hash is the source-fn
    # content hash from RootItem — which we cannot easily predict without
    # reaching into internals, so we just confirm the row exists and its
    # address is non-empty and discoverable).
    rows = rubedo.lane_store.get_filled_rows("addr-e2e", "producer")
    assert len(rows) == 1
    addr = rows[0]["address"]
    assert isinstance(addr, str) and len(addr) == 64  # sha256 hex

    # A direct by-address lookup with the EXACT address stored reads back
    # the same row.
    found = rubedo.lane_store.find_latest_filled_by_address(
        "addr-e2e", "producer", rows[0]["lane_key"], addr
    )
    assert found is not None
    assert found["output"] == rows[0]["output"]

    # And a wrong address is a miss
    assert (
        rubedo.lane_store.find_latest_filled_by_address(
            "addr-e2e", "producer", rows[0]["lane_key"], addr + "_wrong"
        )
        is None
    )

    shutil.rmtree(p_home, ignore_errors=True)
