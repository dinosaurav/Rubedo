import os
import shutil
import threading

import pytest

from conftest import make_home
from rubedo import Home, pipeline, step

ENV_FOLDER = ".test_home_concurrency_env"


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch):
    abs_env = os.path.abspath(ENV_FOLDER)
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)
    os.makedirs(abs_env, exist_ok=True)
    Home.clear_registry_for_tests()
    monkeypatch.setenv("RUBEDO_HOME", os.path.join(abs_env, "default"))
    yield
    Home.clear_registry_for_tests()
    if os.path.exists(abs_env):
        shutil.rmtree(abs_env)


def _home(name):
    return make_home(os.path.join(os.path.abspath(ENV_FOLDER), name))


def _rows_for(home, pipeline_id):
    return [
        r
        for r in home.lanes.all_filled_rows()
        if r.get("pipeline_id") == pipeline_id
    ]


def test_concurrent_different_homes_are_isolated_and_reuse():
    gate = threading.Event()
    started = threading.Event()
    home_a = _home("homeA")
    home_b = _home("homeB")

    @step
    def slow():
        started.set()
        assert gate.wait(timeout=5), "test gate never opened"
        yield {"v": "a"}

    @step
    def quick():
        yield {"v": "b"}

    pipe_a = pipeline(name="same-name", steps=[slow], home=home_a)
    pipe_b = pipeline(name="same-name", steps=[quick], home=home_b)
    outcome = {}

    def run_a():
        outcome["a"] = pipe_a.run()

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    try:
        assert started.wait(timeout=5), "home-A run never started"
        outcome["b"] = pipe_b.run()
    finally:
        gate.set()
        thread_a.join(timeout=5)

    assert outcome["a"].status == "completed"
    assert outcome["b"].status == "completed"
    assert {r["output"]["v"] for r in _rows_for(home_a, "same-name") if r["lane_key"] != "@root"} == {"a"}
    assert {r["output"]["v"] for r in _rows_for(home_b, "same-name") if r["lane_key"] != "@root"} == {"b"}

    assert pipe_a.run().reused_count == 1
    assert pipe_b.run().reused_count == 1


def test_concurrent_same_home_passes():
    home = _home("homeSame")
    gate = threading.Event()
    started = threading.Event()

    @step
    def slow():
        started.set()
        assert gate.wait(timeout=5), "test gate never opened"
        yield {"v": 1}

    @step
    def quick():
        yield {"v": 2}

    pipe_a = pipeline(name="same_a", steps=[slow], home=home)
    pipe_b = pipeline(name="same_b", steps=[quick], home=home)
    outcome = {}

    def run_a():
        outcome["a"] = pipe_a.run()

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    try:
        assert started.wait(timeout=5), "first run never started"
        outcome["b"] = pipe_b.run()
    finally:
        gate.set()
        thread_a.join(timeout=5)

    assert outcome["a"].status == "completed"
    assert outcome["b"].status == "completed"


def test_sequential_different_homes_work():
    home_a = _home("homeSeqA")
    home_b = _home("homeSeqB")

    @step
    def quick():
        yield {"v": 1}

    pipe_a = pipeline(name="seq", steps=[quick], home=home_a)
    pipe_b = pipeline(name="seq", steps=[quick], home=home_b)

    assert pipe_a.run().status == "completed"
    assert pipe_b.run().status == "completed"
    assert pipe_a.run().reused_count == 1
    assert pipe_b.run().reused_count == 1


def test_default_home_still_works():
    @step
    def quick():
        yield {"v": 1}

    pipe = pipeline(name="default_home", steps=[quick])
    summary = pipe.run()

    assert summary.status == "completed"
    assert Home.default().path.endswith(os.path.join(ENV_FOLDER, "default"))
