"""Fused maps over a table parent apply a row kernel without minting.

use_cache=False never mints. When the parent is a table and the function
takes a dict, the engine calls it once per inner row and stacks: a scalar
becomes a column named after the step; a dict becomes a table of those
rows. Annotate a DataFrame/Table to receive the frame in one call.
"""
import pytest
import pyarrow as pa

from rubedo import step, pipeline
from conftest import isolated_test_env

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("fused_row_kernel", with_data=False) as env:
        TEST_HOME = env.home
        yield


def _outputs(step_name):
    rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == step_name]
    return {
        r.get("lane_key"): TEST_HOME.store.read_output(r.get("output"), r.get("content_type"))
        for r in rows
    }


def _lane_keys(step_name):
    return sorted(
        r.get("lane_key")
        for r in TEST_HOME.lanes.all_filled_rows()
        if r.get("step_name") == step_name
    )


def test_fused_dict_scalar_adds_column_without_minting():
    pl = pytest.importorskip("polars")
    calls = []

    @step(as_table=True)
    def census():
        return pl.DataFrame({"name": [" Alice ", "Bob"], "n": [1, 2]})

    @step(use_cache=False)
    def name_clean(census: dict) -> str:
        calls.append(census["name"])
        return census["name"].strip().lower()

    @step(as_table=True)
    def out(name_clean: pl.DataFrame):
        return name_clean

    pipe = pipeline(name="rk_scalar", steps=[census, name_clean, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 2  # census + out; name_clean fused
    assert _lane_keys("census") == ["@root"]
    assert _lane_keys("out") == ["@root"]
    assert "name_clean" not in {r.get("step_name") for r in TEST_HOME.lanes.all_filled_rows()}
    assert calls == [" Alice ", "Bob"]

    result = list(_outputs("out").values())[0]
    assert result["name"].to_list() == [" Alice ", "Bob"]
    assert result["name_clean"].to_list() == ["alice", "bob"]

    summary2 = pipe.run(workers=1)
    assert summary2.reused_count == 2
    assert calls == [" Alice ", "Bob"], "fused kernel must not run when consumer reuses"


def test_fused_dataframe_annotation_is_one_call():
    pl = pytest.importorskip("polars")
    calls = []

    @step(as_table=True)
    def census():
        return pl.DataFrame({"name": ["a", "b"]})

    @step(use_cache=False)
    def title(census: pl.DataFrame) -> pl.DataFrame:
        calls.append(len(census))
        return census.with_columns(pl.col("name").str.to_uppercase())

    @step(as_table=True)
    def out(title: pl.DataFrame):
        return title

    pipe = pipeline(name="rk_frame", steps=[census, title, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert calls == [2]
    result = list(_outputs("out").values())[0]
    assert result["name"].to_list() == ["A", "B"]


def test_fused_dict_return_stacks_table():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def census():
        return pl.DataFrame({"name": ["a", "b"], "n": [1, 2]})

    @step(use_cache=False)
    def doubled(census: dict) -> dict:
        return {"name": census["name"], "n": census["n"] * 2}

    @step(as_table=True)
    def out(doubled: pl.DataFrame):
        return doubled

    pipe = pipeline(name="rk_dict", steps=[census, doubled, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    result = list(_outputs("out").values())[0]
    assert result["name"].to_list() == ["a", "b"]
    assert result["n"].to_list() == [2, 4]


def test_unannotated_table_parent_is_one_call_not_kernel():
    pl = pytest.importorskip("polars")
    calls = []

    @step(as_table=True)
    def census():
        return pl.DataFrame({"name": ["a", "b"]})

    @step(use_cache=False)
    def once(census):
        calls.append(type(census).__name__)
        return census.height

    @step
    def out(once: int):
        return {"n": once}

    pipe = pipeline(name="rk_once", steps=[census, once, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert calls == ["DataFrame"]
    assert list(_outputs("out").values())[0]["n"] == 2


def test_fused_kernel_on_arrow_table_parent():
    @step(as_table=True)
    def census():
        return pa.table({"name": ["X", "Y"]})

    @step(use_cache=False)
    def lower(census: dict) -> str:
        return census["name"].lower()

    @step(as_table=True)
    def out(lower: pa.Table):
        return lower

    pipe = pipeline(name="rk_pa", steps=[census, lower, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    result = list(_outputs("out").values())[0]
    assert result.column("lower").to_pylist() == ["x", "y"]


def test_cache_default_false_kernels_dict_over_table():
    pl = pytest.importorskip("polars")

    @step(as_table=True, use_cache=True)
    def census():
        return pl.DataFrame({"name": ["A"]})

    @step
    def lower(census: dict) -> str:
        return census["name"].lower()

    @step(as_table=True, use_cache=True)
    def out(lower: pl.DataFrame):
        return lower

    pipe = pipeline(
        name="rk_cd",
        steps=[census, lower, out],
        home=TEST_HOME,
        cache_default=False,
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 2
    result = list(_outputs("out").values())[0]
    assert result["lower"].to_list() == ["a"]


def test_chained_fused_kernels():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def census():
        return pl.DataFrame({"name": ["  Hi  "]})

    @step(use_cache=False)
    def stripped(census: dict) -> str:
        return census["name"].strip()

    @step(use_cache=False)
    def lowered(stripped: dict) -> str:
        return stripped["stripped"].lower()

    @step(as_table=True)
    def out(lowered: pl.DataFrame):
        return lowered

    pipe = pipeline(
        name="rk_chain", steps=[census, stripped, lowered, out], home=TEST_HOME
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    result = list(_outputs("out").values())[0]
    assert result["stripped"].to_list() == ["Hi"]
    assert result["lowered"].to_list() == ["hi"]
