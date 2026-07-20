"""By-address reuse semantics for the Home-owned lane store."""

import os
import shutil
from datetime import datetime, timedelta, timezone

import pytest

from conftest import make_home
from rubedo import pipeline, step

ENV_FOLDER = ".test_lane_addr_env"
PIPE = "addr-pipe"
STEP = "extract"
RUN1 = "run_addr_1"
RUN2 = "run_addr_2"
ADDR_V1 = "addr_v1_hash"
ADDR_V2 = "addr_v2_hash"
ADDR_PARAMS_A = "addr_params_a"
ADDR_PARAMS_B = "addr_params_b"
TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_env = os.path.abspath(ENV_FOLDER)
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)
    os.makedirs(abs_env, exist_ok=True)
    TEST_HOME = make_home(abs_env)
    yield
    TEST_HOME.lanes.clear_run_buffers()
    TEST_HOME = None
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)


def _ts(minutes_ago: float = 0):
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


def test_address_match_returns_filled():
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json", RUN1,
        ts=_ts(minutes_ago=10),
    )
    row = TEST_HOME.lanes.find_latest_filled_by_address(
        PIPE, STEP, "@root#0", ADDR_V1
    )
    assert row is not None
    assert row["output"] == "ch_v1"
    assert row["address"] == ADDR_V1


def test_different_address_is_a_miss():
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json", RUN1,
        ts=_ts(minutes_ago=10),
    )
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#0", ADDR_V2
        )
        is None
    )


def test_version_bump_supersedes_not_overwrites():
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json", RUN1,
        ts=_ts(minutes_ago=10),
    )
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V2, "ih_a", "ch_v2", "json", RUN2,
        ts=_ts(minutes_ago=0),
    )

    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#0", ADDR_V1
        )["output"]
        == "ch_v1"
    )
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#0", ADDR_V2
        )["output"]
        == "ch_v2"
    )


def test_multiple_generations_latest_by_ts_wins():
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v1", "json", RUN1,
        ts=_ts(minutes_ago=10),
    )
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_V1, "ih_a", "ch_v2", "json", RUN2,
        ts=_ts(minutes_ago=0),
    )
    row = TEST_HOME.lanes.find_latest_filled_by_address(
        PIPE, STEP, "@root#0", ADDR_V1
    )
    assert row is not None
    assert row["output"] == "ch_v2"
    assert row["run_id"] == RUN2


def test_params_change_is_a_miss():
    TEST_HOME.lanes.append_filled(
        PIPE, STEP, "@root#0", ADDR_PARAMS_A, "ih_a", "ch_pa", "json", RUN1,
        ts=_ts(minutes_ago=10),
    )
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#0", ADDR_PARAMS_A
        )
        is not None
    )
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#0", ADDR_PARAMS_B
        )
        is None
    )


def test_address_with_no_rows_returns_none():
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, "no_such_step", "@root#0", "any_addr"
        )
        is None
    )
    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            PIPE, STEP, "@root#never", "any_addr"
        )
        is None
    )


def test_lane_store_address_matches_compute_output_address():
    call_count = 0

    @step
    def producer():
        nonlocal call_count
        call_count += 1
        return {"n": call_count}

    @step
    def consumer(producer):
        return producer["n"] * 2

    p = pipeline(name="addr-e2e", steps=[producer, consumer], home=TEST_HOME)
    p.run(workers=1)

    rows = TEST_HOME.lanes.get_filled_rows("addr-e2e", "producer")
    assert len(rows) == 1
    addr = rows[0]["address"]
    assert isinstance(addr, str) and len(addr) == 64

    found = TEST_HOME.lanes.find_latest_filled_by_address(
        "addr-e2e", "producer", rows[0]["lane_key"], addr
    )
    assert found is not None
    assert found["output"] == rows[0]["output"]

    assert (
        TEST_HOME.lanes.find_latest_filled_by_address(
            "addr-e2e", "producer", rows[0]["lane_key"], addr + "_wrong"
        )
        is None
    )
