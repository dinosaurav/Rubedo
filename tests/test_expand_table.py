"""Tests for expand steps that return an Arrow Table / DataFrame.

An expand step can return a pa.Table, polars DataFrame, or pandas
DataFrame instead of yielding.  Each row becomes a content-addressed
lane — the table IS the fan-out, no Python loop.
"""
import os
import shutil
import pytest
import pyarrow as pa

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo import lane_store
import rubedo.store as store
from rubedo.store import read_output

TEST_FOLDER = ".test_expand_table_data"
ENV_FOLDER = ".test_expand_table_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test = os.path.abspath(TEST_FOLDER)
    abs_env = os.path.abspath(ENV_FOLDER)
    for d in (abs_test, abs_env):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d)

    store.OBJECTS_DIR = f"{abs_env}/objects"
    store.STAGING_DIR = f"{abs_env}/staging"
    os.environ["RUBEDO_DB_PATH"] = f"sqlite:///{abs_env}/rubedo.sqlite"
    init_db()

    yield

    for d in (abs_test, abs_env):
        if os.path.exists(d):
            shutil.rmtree(d)


def _outputs(step_name):
    rows = [r for r in lane_store.all_filled_rows() if r.get("step_name") == step_name]
    return {
        r.get("lane_key"): read_output(r.get("output"), r.get("content_type"))
        for r in rows
    }


def test_root_expand_returns_pa_table():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "name": ["alice", "bob", "carol"],
            "score": [100, 200, 300],
        })

    @step
    def process(load_data: dict):
        return {"greeting": f"hi {load_data['name']}", "doubled": load_data["score"] * 2}

    pipe = pipeline(name="t1", steps=[load_data, process])
    summary = pipe.run(workers=1)

    assert summary.failed_count == 0
    assert summary.created_count == 6  # 3 expand + 3 process

    outs = _outputs("process")
    assert len(outs) == 3
    names = {v["greeting"] for v in outs.values()}
    assert names == {"hi alice", "hi bob", "hi carol"}


def test_root_expand_table_rerun_reuses():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "name": ["alice", "bob"],
            "score": [100, 200],
        })

    @step
    def process(load_data: dict):
        return load_data["name"].upper()

    pipe = pipeline(name="t2", steps=[load_data, process])
    s1 = pipe.run(workers=1)
    assert s1.created_count == 4

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 4


def test_dependent_expand_returns_table():
    @step
    def source():
        yield {"batch": "A"}
        yield {"batch": "B"}

    @step(shape="expand")
    def expand_batch(source: dict):
        n = 2 if source["batch"] == "A" else 1
        return pa.table({
            "item": [f"{source['batch']}_{i}" for i in range(n)],
            "val": list(range(n)),
        })

    @step
    def process(expand_batch: dict):
        return {"item": expand_batch["item"], "val": expand_batch["val"]}

    pipe = pipeline(name="t3", steps=[source, expand_batch, process])
    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    # 2 source + 3 expand children + 3 process = 8
    assert s1.created_count == 8

    outs = _outputs("process")
    assert len(outs) == 3
    items = {v["item"] for v in outs.values()}
    assert items == {"A_0", "A_1", "B_0"}

    # Re-run: anchor should skip the expand fn
    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 8


def test_expand_table_dedup_identical_rows():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "name": ["alice", "alice", "bob"],
            "score": [100, 100, 200],
        })

    @step
    def process(load_data: dict):
        return load_data["name"]

    pipe = pipeline(name="t4", steps=[load_data, process])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    # 2 expand lanes (alice deduped) + 2 process = 4
    assert summary.created_count == 4

    outs = _outputs("process")
    assert len(outs) == 2


def test_expand_polars_table():
    pl = pytest.importorskip("polars")

    @step(shape="expand")
    def load_data():
        return pl.DataFrame({
            "name": ["alice", "bob"],
            "age": [30, 25],
        })

    @step
    def greet(load_data: dict):
        return f"Hello {load_data['name']}, age {load_data['age']}"

    pipe = pipeline(name="t5", steps=[load_data, greet])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 4

    outs = _outputs("greet")
    assert len(outs) == 2
    greetings = set(outs.values())
    assert "Hello alice, age 30" in greetings
    assert "Hello bob, age 25" in greetings
