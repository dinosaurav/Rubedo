"""Tests for arrow_reduce — reduce steps that receive a pa.Table instead
of a dict-of-lanes.

@step(shape="reduce", arrow_reduce=True) gets the parent's output struct
column as a pa.Table (fields → columns). Vectorized Arrow operations
replace per-lane Python dict iteration.
"""
import os
import shutil
import pytest
import pyarrow as pa
import pyarrow.compute as pc

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo import lane_store
import rubedo.store as store
from rubedo.store import read_output

TEST_FOLDER = ".test_arrow_reduce_data"
ENV_FOLDER = ".test_arrow_reduce_env"


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


def test_arrow_reduce_gets_table():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "name": ["alice", "bob", "carol"],
            "score": [100, 200, 300],
        })

    @step
    def enrich(load_data: dict):
        return {"name": load_data["name"], "doubled": load_data["score"] * 2}

    @step(depends_on=["enrich"], shape="reduce", arrow_reduce=True)
    def total(enrich):
        assert hasattr(enrich, "column_names"), f"expected pa.Table, got {type(enrich)}"
        assert "doubled" in enrich.column_names
        return {"total": int(pc.sum(enrich["doubled"]).as_py())}

    pipe = pipeline(name="ar1", steps=[load_data, enrich, total])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("total")
    assert len(outs) == 1
    assert list(outs.values())[0]["total"] == 1200  # (100+200+300)*2


def test_arrow_reduce_with_group_key():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "category": ["tech", "tech", "biz", "biz"],
            "amount": [10, 20, 30, 40],
        })

    @step(depends_on=["load_data"], shape="reduce", group_key="category", arrow_reduce=True)
    def subtotal(load_data):
        return {"category": load_data["category"][0].as_py(), "sum": int(pc.sum(load_data["amount"]).as_py())}

    pipe = pipeline(name="ar2", steps=[load_data, subtotal])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("subtotal")
    assert len(outs) == 2
    by_cat = {v["category"]: v["sum"] for v in outs.values()}
    assert by_cat["tech"] == 30
    assert by_cat["biz"] == 70


def test_arrow_reduce_rerun_reuses():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "x": [1, 2, 3],
        })

    @step
    def double(load_data: dict):
        return {"x": load_data["x"], "y": load_data["x"] * 10}

    @step(depends_on=["double"], shape="reduce", arrow_reduce=True)
    def total(double):
        return {"sum": int(pc.sum(double["y"]).as_py())}

    pipe = pipeline(name="ar3", steps=[load_data, double, total])
    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count > 0

    s2 = pipe.run(workers=1)
    # Expand root re-runs (source) — 3 children "created"
    # double + total reuse (same addresses, IHU fulfilled)
    assert s2.created_count == 3  # expand children
    assert s2.reused_count > 0    # double + total


def test_non_arrow_reduce_still_gets_dict():
    """A reduce without arrow_reduce=True still gets the dict-of-lanes."""
    @step
    def source():
        yield {"x": 1}
        yield {"x": 2}

    @step(depends_on=["source"], shape="reduce")
    def total(source):
        assert isinstance(source, dict), f"expected dict, got {type(source)}"
        return {"sum": sum(v["x"] for v in source.values())}

    pipe = pipeline(name="ar4", steps=[source, total])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("total")
    assert list(outs.values())[0]["sum"] == 3
