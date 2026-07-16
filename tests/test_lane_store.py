"""Tests for the per-step Arrow lane store (notes/arrow-storage.md Phase 2a).

The lane store replaces the `materializations` SQLite table with append-only
Arrow IPC files.  These tests exercise the store primitives in isolation:
append-filled, find-latest-filled (reuse check), find-latest (latest row),
flush (durability), and compaction (GC).  The Arrow file is pure data —
no blank tombstones; liveness (reuse vs. recompute) is the
``input_hash_usages`` SQLite table's job."""

import os
import shutil
from datetime import datetime, timezone, timedelta

import pytest

from rubedo.lane_store import (
    append_filled,
    clear_run_buffers,
    compact_step,
    find_latest,
    find_latest_filled,
    flush_all,
    flush_step,
    get_all_lane_keys,
    get_filled_rows,
    init_tables,
)

ENV_FOLDER = ".test_lane_store_env"


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


PIPE = "test-pipe"
STEP = "extract"
RUN1 = "run_001"
RUN2 = "run_002"


def _ts(minutes_ago: float = 0):
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


# ---------------------------------------------------------------------------
# Basic append + find
# ---------------------------------------------------------------------------


def test_append_filled_and_find():
    append_filled(PIPE, STEP, "@root#0", "addr_1", "ih_a", "ch_0", "json", "obj/0", RUN1)
    row = find_latest_filled(PIPE, STEP, "@root#0")
    assert row is not None
    assert row["content_hash"] == "ch_0"
    assert row["input_hash"] == "ih_a"
    assert row["output_path"] == "obj/0"
    assert row["run_id"] == RUN1
    assert row["filtered"] is False


def test_find_latest_filled_missing_lane():
    append_filled(PIPE, STEP, "@root#0", "addr_2", "ih_a", "ch_0", "json", "obj/0", RUN1)
    assert find_latest_filled(PIPE, STEP, "@root#nonexistent") is None


def test_find_latest_filled_missing_step():
    assert find_latest_filled(PIPE, "no_such_step", "@root#0") is None


def test_find_latest_filled_with_input_hash_filter():
    """When input_hash is given, only rows with a matching hash are returned."""
    append_filled(PIPE, STEP, "@root#0", "addr_3", "ih_v1", "ch_v1", "json", "obj/1", RUN1)
    append_filled(PIPE, STEP, "@root#0", "addr_4", "ih_v2", "ch_v2", "json", "obj/2", RUN1,
                  ts=_ts(minutes_ago=1))  # explicitly older

    # Matching input_hash → reuse
    row = find_latest_filled(PIPE, STEP, "@root#0", input_hash="ih_v1")
    assert row is not None
    assert row["content_hash"] == "ch_v1"

    # Non-matching input_hash → recompute (returns None)
    assert find_latest_filled(PIPE, STEP, "@root#0", input_hash="ih_v3") is None


def test_latest_by_ts_wins():
    """When multiple filled rows exist for a lane, the latest by ts is returned."""
    old_ts = _ts(minutes_ago=10)
    new_ts = _ts(minutes_ago=0)
    append_filled(PIPE, STEP, "@root#0", "addr_5", "ih_a", "ch_old", "json", "obj/old", RUN1,
                  ts=old_ts)
    append_filled(PIPE, STEP, "@root#0", "addr_6", "ih_a", "ch_new", "json", "obj/new", RUN2,
                  ts=new_ts)
    row = find_latest_filled(PIPE, STEP, "@root#0")
    assert row["content_hash"] == "ch_new"
    assert row["run_id"] == RUN2


# ---------------------------------------------------------------------------
# Latest row lookup
# ---------------------------------------------------------------------------


def test_find_latest_absent_lane():
    """A lane that was never computed returns None from find_latest."""
    append_filled(PIPE, STEP, "@root#0", "addr_8", "ih_a", "ch_0", "json", "obj/0", RUN1)
    assert find_latest(PIPE, STEP, "@root#never") is None


def test_find_latest_returns_most_recent():
    """find_latest returns the most recent row by ts, regardless of address."""
    append_filled(PIPE, STEP, "@root#0", "addr_old", "ih_a", "ch_old", "json", "obj/old",
                  RUN1, ts=_ts(minutes_ago=10))
    append_filled(PIPE, STEP, "@root#0", "addr_new", "ih_a", "ch_new", "json", "obj/new",
                  RUN2, ts=_ts(minutes_ago=0))
    latest = find_latest(PIPE, STEP, "@root#0")
    assert latest is not None
    assert latest["content_hash"] == "ch_new"
    assert latest["address"] == "addr_new"


# ---------------------------------------------------------------------------
# Multiple lanes
# ---------------------------------------------------------------------------


def test_multiple_lanes_independent():
    for i in range(5):
        append_filled(PIPE, STEP, f"@root#{i}", f"addr_{i}", f"ih_{i}", f"ch_{i}",
                       "json", f"obj/{i}", RUN1)
    for i in range(5):
        row = find_latest_filled(PIPE, STEP, f"@root#{i}")
        assert row is not None
        assert row["content_hash"] == f"ch_{i}"


def test_get_all_lane_keys():
    append_filled(PIPE, STEP, "@root#0", "addr_11", "ih_a", "ch_0", "json", "obj/0", RUN1)
    append_filled(PIPE, STEP, "@root#1", "addr_12", "ih_b", "ch_1", "json", "obj/1", RUN1)
    keys = set(get_all_lane_keys(PIPE, STEP))
    assert keys == {"@root#0", "@root#1"}
    # All rows are filled — filled_only doesn't filter anything out
    filled = set(get_all_lane_keys(PIPE, STEP, filled_only=True))
    assert filled == {"@root#0", "@root#1"}


def test_get_filled_rows():
    append_filled(PIPE, STEP, "@root#0", "addr_13", "ih_a", "ch_0", "json", "obj/0", RUN1,
                  ts=_ts(minutes_ago=5))
    append_filled(PIPE, STEP, "@root#0", "addr_14", "ih_a", "ch_1", "json", "obj/1", RUN2,
                  ts=_ts(minutes_ago=0))  # newer generation
    append_filled(PIPE, STEP, "@root#1", "addr_15", "ih_b", "ch_2", "json", "obj/2", RUN1)
    rows = get_filled_rows(PIPE, STEP)
    by_lane = {r["lane_key"]: r for r in rows}
    assert by_lane["@root#0"]["content_hash"] == "ch_1"  # latest generation
    assert by_lane["@root#1"]["content_hash"] == "ch_2"


# ---------------------------------------------------------------------------
# Flush (durability)
# ---------------------------------------------------------------------------


def test_flush_persists_to_disk():
    append_filled(PIPE, STEP, "@root#0", "addr_16", "ih_a", "ch_0", "json", "obj/0", RUN1)
    flush_step(PIPE, STEP)

    # After flush, the in-memory buffer is cleared
    clear_run_buffers()  # simulate a new process

    # The disk file has the row
    row = find_latest_filled(PIPE, STEP, "@root#0")
    assert row is not None
    assert row["content_hash"] == "ch_0"


def test_flush_all_writes_every_step():
    append_filled(PIPE, STEP, "@root#0", "addr_17", "ih_a", "ch_0", "json", "obj/0", RUN1)
    append_filled(PIPE, "other_step", "@root#1", "addr_other", "ih_b", "ch_1", "json", "obj/1",
                  RUN1)
    flush_all()
    clear_run_buffers()

    assert find_latest_filled(PIPE, STEP, "@root#0") is not None
    assert find_latest_filled(PIPE, "other_step", "@root#1") is not None


def test_flush_accumulates_across_runs():
    """A second flush doesn't clobber the first run's rows."""
    append_filled(PIPE, STEP, "@root#0", "addr_18", "ih_a", "ch_v1", "json", "obj/1", RUN1,
                  ts=_ts(minutes_ago=10))
    flush_step(PIPE, STEP)
    clear_run_buffers()

    append_filled(PIPE, STEP, "@root#0", "addr_19", "ih_a", "ch_v2", "json", "obj/2", RUN2,
                  ts=_ts(minutes_ago=0))
    flush_step(PIPE, STEP)
    clear_run_buffers()

    row = find_latest_filled(PIPE, STEP, "@root#0")
    assert row["content_hash"] == "ch_v2"  # latest wins


def test_in_memory_buffer_visible_before_flush():
    """During a run, downstream reads see the buffer without a disk flush."""
    append_filled(PIPE, STEP, "@root#0", "addr_20", "ih_a", "ch_0", "json", "obj/0", RUN1)
    # No flush — still in buffer
    row = find_latest_filled(PIPE, STEP, "@root#0")
    assert row is not None
    assert row["content_hash"] == "ch_0"


# ---------------------------------------------------------------------------
# Compaction (GC)
# ---------------------------------------------------------------------------


def test_compact_keeps_latest_per_lane():
    append_filled(PIPE, STEP, "@root#0", "addr_21", "ih_a", "ch_v1", "json", "obj/1", RUN1,
                  ts=_ts(minutes_ago=10))
    append_filled(PIPE, STEP, "@root#0", "addr_22", "ih_a", "ch_v2", "json", "obj/2", RUN2,
                  ts=_ts(minutes_ago=0))
    append_filled(PIPE, STEP, "@root#1", "addr_23", "ih_b", "ch_3", "json", "obj/3", RUN1)
    flush_step(PIPE, STEP)
    clear_run_buffers()

    compact_step(PIPE, STEP, keep_lane_keys={"@root#0", "@root#1"})

    rows = get_filled_rows(PIPE, STEP)
    by_lane = {r["lane_key"]: r for r in rows}
    assert len(rows) == 2
    assert by_lane["@root#0"]["content_hash"] == "ch_v2"  # latest only
    assert by_lane["@root#1"]["content_hash"] == "ch_3"


def test_compact_drops_unkept_lanes():
    append_filled(PIPE, STEP, "@root#0", "addr_24", "ih_a", "ch_0", "json", "obj/0", RUN1)
    append_filled(PIPE, STEP, "@root#1", "addr_25", "ih_b", "ch_1", "json", "obj/1", RUN1)
    flush_step(PIPE, STEP)
    clear_run_buffers()

    compact_step(PIPE, STEP, keep_lane_keys={"@root#0"})

    assert find_latest_filled(PIPE, STEP, "@root#0") is not None
    assert find_latest_filled(PIPE, STEP, "@root#1") is None


def test_compact_removes_file_when_nothing_kept():
    append_filled(PIPE, STEP, "@root#0", "addr_26", "ih_a", "ch_0", "json", "obj/0", RUN1)
    flush_step(PIPE, STEP)
    clear_run_buffers()

    compact_step(PIPE, STEP, keep_lane_keys=set())
    assert find_latest_filled(PIPE, STEP, "@root#0") is None


# ---------------------------------------------------------------------------
# Cross-pipeline isolation
# ---------------------------------------------------------------------------


def test_different_pipelines_isolated():
    append_filled("pipe_a", STEP, "@root#0", "addr_a", "ih_a", "ch_a", "json", "obj/a", RUN1)
    append_filled("pipe_b", STEP, "@root#0", "addr_b", "ih_b", "ch_b", "json", "obj/b", RUN1)
    assert find_latest_filled("pipe_a", STEP, "@root#0")["content_hash"] == "ch_a"
    assert find_latest_filled("pipe_b", STEP, "@root#0")["content_hash"] == "ch_b"