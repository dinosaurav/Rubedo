"""Tests for p.join() and p.union() — declarative steps with no function body.

p.join() builds a nested struct from matched parents: {"orders": {...}, "customers": {...}}.
p.union() merges lane sets from multiple parents, deduped by content hash.
Both require name= and produce a StepSpec with declarative=True.
"""
import pytest

from rubedo import step, pipeline
from conftest import isolated_test_env

TEST_FOLDER = ".test_declarative_data"
ENV_FOLDER = ".test_declarative_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("declarative") as env:
        TEST_HOME = env.home
        yield

def _outputs(step_name):
    rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == step_name]
    return {
        r.get("lane_key"): TEST_HOME.store.read_output(r.get("output"), r.get("content_type"))
        for r in rows
    }


# ---------------------------------------------------------------------------
# p.join()
# ---------------------------------------------------------------------------

def test_declarative_join_nested_output():
    @step
    def orders():
        yield {"oid": "o1", "cust": "alice"}
        yield {"oid": "o2", "cust": "alice"}
        yield {"oid": "o3", "cust": "bob"}

    @step
    def customers():
        yield {"cid": "alice", "name": "Alice Smith"}
        yield {"cid": "bob", "name": "Bob Jones"}

    p = pipeline(name="dj1", steps=[orders, customers], home=TEST_HOME)
    p.join(name="joined", join_on={"orders": "cust", "customers": "cid"})

    @p.step
    def enrich(joined: dict):
        return {"oid": joined["orders"]["oid"], "name": joined["customers"]["name"]}

    summary = p.run(workers=1)
    assert summary.failed_count == 0

    joined = _outputs("joined")
    assert len(joined) == 3
    for val in joined.values():
        assert "orders" in val
        assert "customers" in val
        assert val["orders"]["oid"] in ("o1", "o2", "o3")
        assert val["customers"]["name"] in ("Alice Smith", "Bob Jones")


def test_declarative_join_reuses():
    @step
    def orders():
        yield {"oid": "o1", "cust": "alice"}

    @step
    def customers():
        yield {"cid": "alice", "name": "Alice"}

    p = pipeline(name="dj2", steps=[orders, customers], home=TEST_HOME)
    p.join(name="joined", join_on={"orders": "cust", "customers": "cid"})

    s1 = p.run(workers=1)
    assert s1.created_count > 0

    s2 = p.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count > 0


def test_declarative_join_requires_name():
    @step
    def a():
        yield {"k": "x"}

    @step
    def b():
        yield {"k": "x"}

    p = pipeline(name="dj3", steps=[a, b], home=TEST_HOME)
    with pytest.raises(TypeError, match="name"):
        p.join(join_on={"a": "k", "b": "k"})


def test_declarative_join_rejects_empty_join_on():
    p = pipeline(name="dj4", steps=[], home=TEST_HOME)
    with pytest.raises(ValueError, match="Step 'j': .*at least two parents"):
        p.join(name="j", join_on={})


def test_declarative_join_rejects_single_parent():
    p = pipeline(name="dj5", steps=[], home=TEST_HOME)
    with pytest.raises(ValueError, match="Step 'j': .*at least two parents"):
        p.join(name="j", join_on={"a": "x"})


# ---------------------------------------------------------------------------
# p.union()
# ---------------------------------------------------------------------------

def test_declarative_union_merges_lane_sets():
    @step
    def source_a():
        yield {"val": 1}
        yield {"val": 2}

    @step
    def source_b():
        yield {"val": 3}
        yield {"val": 1}  # dup of source_a's val=1

    p = pipeline(name="du1", steps=[source_a, source_b], home=TEST_HOME)
    p.union(name="combined", depends_on=["source_a", "source_b"])

    @p.step
    def process(combined: dict):
        return {"doubled": combined["val"] * 2}

    summary = p.run(workers=1)
    assert summary.failed_count == 0

    combined = _outputs("combined")
    assert len(combined) == 3  # val=1 deduped, val=2, val=3
    vals = {v["val"] for v in combined.values()}
    assert vals == {1, 2, 3}


def test_declarative_union_reuses():
    @step
    def source_a():
        yield {"val": 1}

    @step
    def source_b():
        yield {"val": 2}

    p = pipeline(name="du2", steps=[source_a, source_b], home=TEST_HOME)
    p.union(name="combined", depends_on=["source_a", "source_b"])

    s1 = p.run(workers=1)
    assert s1.created_count > 0

    s2 = p.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count > 0


def test_declarative_union_single_parent_passthrough():
    @step
    def source():
        yield {"val": 10}
        yield {"val": 20}

    p = pipeline(name="du3", steps=[source], home=TEST_HOME)
    p.union(name="passed", depends_on=["source"])

    @p.step
    def process(passed: dict):
        return {"v": passed["val"]}

    summary = p.run(workers=1)
    assert summary.failed_count == 0

    outs = _outputs("process")
    assert len(outs) == 2
    vals = {v["v"] for v in outs.values()}
    assert vals == {10, 20}


def test_declarative_union_rejects_no_parents():
    p = pipeline(name="du4", steps=[], home=TEST_HOME)
    with pytest.raises(ValueError, match="Step 'u': .*at least one parent"):
        p.union(name="u", depends_on=[])
