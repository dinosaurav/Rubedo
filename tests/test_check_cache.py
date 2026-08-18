"""@step(force=True): per-step force — always re-execute, still commit.

force=True on a step is the per-step equivalent of run(force=True): plan
skips the reuse check and emits "execute" every run, but the commit path
is unaffected — the result lands in cache, so downstream steps reuse and a
later run without force sees the fresh output.
"""


import pytest

from rubedo import step, pipeline
from conftest import isolated_test_env

ENV_FOLDER = ".test_check_cache_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("check_cache", with_data=False) as env:
        TEST_HOME = env.home
        yield


def test_step_force_reruns_but_commits():
    """A force=True step re-executes every run, but its output is
    committed — so a downstream step (no force) reuses on run 2."""
    root_calls = []
    child_calls = []

    @step(force=True)
    def root():
        root_calls.append(1)
        return {"value": 42}

    @step
    def child(root: dict):
        child_calls.append(1)
        return {"doubled": root["value"] * 2}

    pipe = pipeline(name="cc", steps=[root, child], home=TEST_HOME)
    r1 = pipe.run(workers=1)
    assert len(root_calls) == 1
    assert len(child_calls) == 1
    assert r1.created_count == 2

    r2 = pipe.run(workers=1)
    assert len(root_calls) == 2  # re-executed (force=True)
    assert len(child_calls) == 1  # NOT re-run — reused (root's output identical)
    # Root re-executed but produced the same output → mat_action "reused"
    # (same address, same output_identity). The run summary counts the
    # materialization action, not whether the function ran.
    assert r2.reused_count == 2


def test_force_false_default_reuses():
    """Default (no force): both steps reuse on run 2."""
    root_calls = []

    @step
    def root():
        root_calls.append(1)
        return {"value": 42}

    @step
    def child(root: dict):
        return {"doubled": root["value"] * 2}

    pipe = pipeline(name="cc_default", steps=[root, child], home=TEST_HOME)
    pipe.run(workers=1)
    assert len(root_calls) == 1
    r2 = pipe.run(workers=1)
    assert len(root_calls) == 1  # reused
    assert r2.reused_count == 2


def test_step_force_then_unforced_reuses():
    """After a force=True run, switching to force=False (default)
    should reuse the committed output (it's in cache)."""
    root_calls = []

    def make_root(force_root):
        @step(force=force_root)
        def root():
            root_calls.append(1)
            return {"value": 42}

        return root

    @step
    def child(root: dict):
        return {"doubled": root["value"] * 2}

    root1 = make_root(True)
    pipe = pipeline(name="cc_switch", steps=[root1, child], home=TEST_HOME)
    pipe.run(workers=1)
    assert len(root_calls) == 1

    root2 = make_root(False)
    # Same step name "root", different force setting — version is
    # still "0", so the address is the same and the cached output is seen.
    pipe2 = pipeline(name="cc_switch", steps=[root2, child], home=TEST_HOME)
    r2 = pipe2.run(workers=1)
    assert len(root_calls) == 1  # reused this time
    assert r2.reused_count == 2


def test_force_with_use_cache_false_raises():
    with pytest.raises(ValueError, match="contradictory with use_cache=False"):

        @step(force=True, use_cache=False)
        def util():
            return {"x": 1}


def test_force_with_stale_after_raises():
    with pytest.raises(ValueError, match="meaningless with force=True"):

        @step(force=True, stale_after="24h")
        def scraper():
            return {"data": "scraped"}


def test_force_in_definition_snapshot():
    from rubedo.spec import definition

    @step(force=True)
    def root():
        return {"value": 1}

    @step
    def child(root: dict):
        return {"y": root["value"]}

    pipe = pipeline(name="cc_snap", steps=[root, child], home=TEST_HOME)
    snap = definition(pipe.spec)
    root_entry = next(s for s in snap["steps"] if s["name"] == "root")
    assert root_entry.get("force") is True
    child_entry = next(s for s in snap["steps"] if s["name"] == "child")
    assert "force" not in child_entry  # default False omitted
