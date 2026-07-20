"""Planning reuse semantics — verifies the lane_store + input_hash_usages
path produces the same reuse/execute decisions as the SQLite path.

These tests run actual pipelines through the public API and assert the
run summaries (created/reused/failed counts) match expectations.  They
exercise:
  - Basic reuse (run twice, second run all reused)
  - Version bump (recompute)
  - Params change (recompute only params-reading steps)
  - Invalidation (recompute invalidated lanes)
  - Filtered output reuse
  - Code drift (code="warn" reuse with warning)
  - Stale_after (recompute after TTL)

The parallel-write path ensures both SQLite and Arrow have the same data,
so swapping the planning reader from SQLite to Arrow should leave these
assertions unchanged — that's the verification."""

import os
import shutil

import pytest

from rubedo import Selection, Filtered, invalidate, pipeline, step
from conftest import make_home

TEST_FOLDER = ".test_planning_reuse_data"
ENV_FOLDER = ".test_planning_reuse_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)



    TEST_HOME = make_home(ENV_FOLDER)
    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


# ---------------------------------------------------------------------------
# Basic reuse: run twice, second run all reused
# ---------------------------------------------------------------------------


def test_basic_reuse():
    call_count = 0

    @step
    def producer():
        nonlocal call_count
        call_count += 1
        return {"n": call_count}

    @step
    def consumer(producer):
        return producer["n"] * 10

    p = pipeline(name="reuse-test", steps=[producer, consumer], home=TEST_HOME)
    s1 = p.run(workers=1)
    assert s1.created_count == 2
    assert s1.reused_count == 0
    assert call_count == 1

    s2 = p.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 2
    assert call_count == 1  # not re-executed


# ---------------------------------------------------------------------------
# Version bump: recompute
# ---------------------------------------------------------------------------


def test_version_bump_recomputes():
    call_count = 0

    @step(version="0")
    def gen_v0():
        nonlocal call_count
        call_count += 1
        return {"v": 0}

    p = pipeline(name="ver-test", steps=[gen_v0], home=TEST_HOME)
    p.run(workers=1)
    assert call_count == 1

    @step(version="1")
    def gen_v1():
        nonlocal call_count
        call_count += 1
        return {"v": 1}

    p2 = pipeline(name="ver-test", steps=[gen_v1], home=TEST_HOME)
    s = p2.run(workers=1)
    assert s.created_count == 1
    assert s.reused_count == 0
    assert call_count == 2


# ---------------------------------------------------------------------------
# Params change: recompute only params-reading steps
# ---------------------------------------------------------------------------


def test_params_change_recomputes():
    from pydantic import BaseModel

    class Params(BaseModel):
        threshold: int = 10

    call_count = 0

    @step
    def data_src():
        return {"x": 5}

    @step
    def filter_step(data_src, params: Params):
        nonlocal call_count
        call_count += 1
        threshold = params["threshold"] if isinstance(params, dict) else params.threshold
        return {"passed": data_src["x"] > threshold}

    p = pipeline(name="params-test", steps=[data_src, filter_step], params_model=Params, home=TEST_HOME)
    s1 = p.run(workers=1, params={"threshold": 10})
    assert s1.created_count == 2
    assert call_count == 1

    # Same params → all reused
    s2 = p.run(workers=1, params={"threshold": 10})
    assert s2.reused_count == 2
    assert call_count == 1

    # Different params → filter_step recomputes, data_src reused
    s3 = p.run(workers=1, params={"threshold": 3})
    assert s3.created_count == 1
    assert s3.reused_count == 1
    assert call_count == 2


# ---------------------------------------------------------------------------
# Invalidation: recompute invalidated lanes
# ---------------------------------------------------------------------------


def test_invalidation_recomputes():
    call_count = 0

    @step
    def producer():
        nonlocal call_count
        call_count += 1
        return {"value": "acme"}

    @step
    def consumer(producer):
        return {"doubled": producer["value"] * 2}

    p = pipeline(name="inval-test", steps=[producer, consumer], home=TEST_HOME)
    p.run(workers=1)
    assert call_count == 1

    # Invalidate the producer
    invalidate(Selection.parse("step:producer value:acme"), reason="test", home=TEST_HOME)
    # Consumer should also be invalidated (downstream)

    s = p.run(workers=1)
    assert s.created_count >= 1  # at least producer recomputed
    assert call_count == 2


# ---------------------------------------------------------------------------
# Filtered output reuse
# ---------------------------------------------------------------------------


def test_filtered_output_cached_and_reused():
    call_count = 0

    @step
    def source():
        nonlocal call_count
        call_count += 1
        return {"n": 5}

    @step
    def filter_step(source):
        if source["n"] < 10:
            return Filtered(reason="too small")
        return source

    @step
    def consumer(filter_step):
        return filter_step

    p = pipeline(name="filter-test", steps=[source, filter_step, consumer], home=TEST_HOME)
    s1 = p.run(workers=1)
    # source created, filter_step filtered (cached), consumer filtered (parent filtered)
    assert s1.created_count >= 1

    s2 = p.run(workers=1)
    # All reused/filtered from cache — source not re-executed
    assert s2.created_count == 0
    assert call_count == 1


# ---------------------------------------------------------------------------
# Code drift: code="warn" reuses with warning
# ---------------------------------------------------------------------------


def test_code_warn_reuses_with_warning():
    @step(code="warn")
    def gen():
        return {"n": 1}

    p = pipeline(name="drift-test", steps=[gen], home=TEST_HOME)
    p.run(workers=1)

    # Redefine with same version but different code → drift warning, but reuse
    @step(code="warn")
    def gen():  # noqa: F811 — intentional redefinition for drift test
        return {"n": 2}

    p2 = pipeline(name="drift-test", steps=[gen], home=TEST_HOME)
    with pytest.warns(UserWarning, match="source code changed"):
        s = p2.run(workers=1)
    # Reused (not re-executed) — code="warn" never recomputes on edits
    assert s.reused_count == 1
    assert s.created_count == 0


# ---------------------------------------------------------------------------
# Multiple lanes: only changed lane recomputes
# ---------------------------------------------------------------------------


def test_partial_recompute_on_input_change():
    """When one lane's input changes, only that lane and its downstream
    recompute; the other lanes reuse."""
    files = {"a.txt": "hello", "b.txt": "world"}
    for name, content in files.items():
        with open(os.path.join(TEST_FOLDER, name), "w") as f:
            f.write(content)

    @step(check_cache=False)
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def count(scan: dict):
        return {"path": scan["path"], "lines": len(scan["text"].splitlines())}

    p = pipeline(name="partial-test", steps=[scan, count], home=TEST_HOME)
    s1 = p.run(workers=1)
    assert s1.created_count > 0

    # Run again — all reused
    s2 = p.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count > 0

    # Change one file
    with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
        f.write("hello\nworld")

    s3 = p.run(workers=1)
    # Only the changed file's lane + scan recompute; b.txt reuses
    assert s3.created_count > 0
    assert s3.reused_count > 0  # b.txt's count lane is reused