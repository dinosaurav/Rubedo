"""Tests for arrow_aggregate — aggregate steps that receive a pa.Table instead
of a dict-of-lanes.

@step(in_shape="aggregate", arrow_aggregate=True) gets the parent's output struct
column as a pa.Table (fields → columns). Vectorized Arrow operations
replace per-lane Python dict iteration.
"""
import pytest
import pyarrow as pa
import pyarrow.compute as pc

from rubedo import step, pipeline
from conftest import isolated_test_env

TEST_FOLDER = ".test_arrow_aggregate_data"
ENV_FOLDER = ".test_arrow_aggregate_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("arrow_aggregate") as env:
        TEST_HOME = env.home
        yield

def _outputs(step_name):
    rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == step_name]
    return {
        r.get("lane_key"): TEST_HOME.store.read_output(r.get("output"), r.get("content_type"))
        for r in rows
    }


def test_arrow_aggregate_gets_table():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "name": ["alice", "bob", "carol"],
            "score": [100, 200, 300],
        })

    @step
    def enrich(load_data: dict):
        return {"name": load_data["name"], "doubled": load_data["score"] * 2}

    @step(depends_on=["enrich"], in_shape="aggregate", arrow_aggregate=True)
    def total(enrich):
        assert hasattr(enrich, "column_names"), f"expected pa.Table, got {type(enrich)}"
        assert "doubled" in enrich.column_names
        return {"total": int(pc.sum(enrich["doubled"]).as_py())}

    pipe = pipeline(name="ar1", steps=[load_data, enrich, total], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("total")
    assert len(outs) == 1
    assert list(outs.values())[0]["total"] == 1200  # (100+200+300)*2


def test_arrow_aggregate_with_group_key():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "category": ["tech", "tech", "biz", "biz"],
            "amount": [10, 20, 30, 40],
        })

    @step(depends_on=["load_data"], in_shape="aggregate", group_key="category", arrow_aggregate=True)
    def subtotal(load_data):
        return {"category": load_data["category"][0].as_py(), "sum": int(pc.sum(load_data["amount"]).as_py())}

    pipe = pipeline(name="ar2", steps=[load_data, subtotal], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("subtotal")
    assert len(outs) == 2
    by_cat = {v["category"]: v["sum"] for v in outs.values()}
    assert by_cat["tech"] == 30
    assert by_cat["biz"] == 70


def test_arrow_aggregate_rerun_reuses():
    @step(shape="expand")
    def load_data():
        return pa.table({
            "x": [1, 2, 3],
        })

    @step
    def double(load_data: dict):
        return {"x": load_data["x"], "y": load_data["x"] * 10}

    @step(depends_on=["double"], in_shape="aggregate", arrow_aggregate=True)
    def total(double):
        return {"sum": int(pc.sum(double["y"]).as_py())}

    pipe = pipeline(name="ar3", steps=[load_data, double, total], home=TEST_HOME)
    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count > 0

    s2 = pipe.run(workers=1)
    # Root expand reuses via anchor — children + double + total all reuse
    assert s2.created_count == 0
    assert s2.reused_count > 0    # double + total + children


def test_non_arrow_aggregate_still_gets_dict():
    """An aggregate without arrow_aggregate=True still gets the dict-of-lanes."""
    @step
    def source():
        yield {"x": 1}
        yield {"x": 2}

    @step(depends_on=["source"], in_shape="aggregate")
    def total(source):
        assert isinstance(source, dict), f"expected dict, got {type(source)}"
        return {"sum": sum(v["x"] for v in source.values())}

    pipe = pipeline(name="ar4", steps=[source, total], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("total")
    assert list(outs.values())[0]["sum"] == 3
