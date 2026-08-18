"""Table grain vs lane minting: as_table keeps one cache entry; expand mints.

Returning a DataFrame without as_table=True errors. Expand returning a
table errors unless row_key= is set (mint dict lanes, identity is the key).
Expand returning a dict/str errors (no iterating keys/characters).
"""
import pytest
import pyarrow as pa

from rubedo import step, pipeline
from conftest import isolated_test_env

TEST_FOLDER = ".test_expand_table_data"
ENV_FOLDER = ".test_expand_table_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("expand_table") as env:
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


def test_map_as_table_keeps_one_lane():
    @step(as_table=True)
    def load_data():
        return pa.table({
            "name": ["alice", "bob", "carol"],
            "score": [100, 200, 300],
        })

    pipe = pipeline(name="t1", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 1
    assert _lane_keys("load_data") == ["@root"]
    out = list(_outputs("load_data").values())[0]
    assert out.column("name").to_pylist() == ["alice", "bob", "carol"]


def test_map_as_table_rerun_reuses():
    @step(as_table=True)
    def load_data():
        return pa.table({"name": ["alice", "bob"], "score": [100, 200]})

    pipe = pipeline(name="t2", steps=[load_data], home=TEST_HOME)
    s1 = pipe.run(workers=1)
    assert s1.created_count == 1
    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 1


def test_dataframe_without_as_table_errors():
    @step
    def load_data():
        return pa.table({"name": ["alice"]})

    pipe = pipeline(name="t-no-flag", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    err = " ".join((f.get("error_message") or "") for f in summary.failures())
    assert "as_table=True" in err


def test_expand_return_table_without_row_key_errors():
    @step(shape="expand")
    def load_data():
        return pa.table({"name": ["alice", "bob"]})

    pipe = pipeline(name="t-explode", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1


def test_expand_return_dict_does_not_mint_keys():
    @step(shape="expand")
    def load_data():
        return {"a": 1}

    pipe = pipeline(name="t-dict", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    assert _lane_keys("load_data") == []


def test_expand_return_str_does_not_mint_characters():
    @step(shape="expand")
    def load_data():
        return "ab"

    pipe = pipeline(name="t-str", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1


def test_expand_return_list_mints_lanes():
    @step(shape="expand")
    def load_data():
        return [{"name": "alice"}, {"name": "bob"}]

    pipe = pipeline(name="t-list", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 2
    names = {v["name"] for v in _outputs("load_data").values() if isinstance(v, dict)}
    assert names == {"alice", "bob"}


def test_census_as_table_chain_is_o1_coordinates():
    """Two as_table loads → as_table merge → as_table agg: one lane each."""
    pl = pytest.importorskip("polars")

    @step(as_table=True, force=True)
    def patients():
        return pl.DataFrame({"patient_id": [1, 2], "name": ["a", "b"]})

    @step(as_table=True, force=True)
    def claims():
        return pl.DataFrame({"patient_id": [1, 1, 2], "dx": ["x", "y", "z"]})

    @step(as_table=True)
    def joined(patients, claims):
        return patients.join(claims, on="patient_id")

    @step(as_table=True)
    def summary(joined):
        return joined.group_by("dx").len()

    pipe = pipeline(
        name="census", steps=[patients, claims, joined, summary], home=TEST_HOME
    )
    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 4
    for name in ("patients", "claims", "joined", "summary"):
        assert _lane_keys(name) == ["@root"]

    s2 = pipe.run(workers=1)
    assert s2.failed_count == 0
    assert s2.created_count == 0
    assert s2.reused_count == 4


def test_expand_from_table_with_row_key():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def normalize():
        return pl.DataFrame({
            "id": ["a", "b", "c"],
            "score": [1, 2, 3],
        })

    @step(shape="expand", row_key="id")
    def cells(normalize):
        return normalize

    @step
    def greet(cells: dict):
        return f"{cells['id']}:{cells['score']}"

    pipe = pipeline(name="row-key", steps=[normalize, cells, greet], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    # 1 table + 3 expand children + 3 greet
    assert summary.created_count == 7
    greetings = set(_outputs("greet").values())
    assert greetings == {"a:1", "b:2", "c:3"}

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 7


def test_expand_from_table_row_key_identity_not_full_payload():
    """Same row_key, different other fields: still the same child coordinate."""
    pl = pytest.importorskip("polars")

    @step(as_table=True, version="1")
    def src():
        return pl.DataFrame({"id": ["a"], "score": [1]})

    @step(shape="expand", row_key="id")
    def cells(src):
        return src

    p1 = pipeline(name="rk-id", steps=[src, cells], home=TEST_HOME)
    p1.run(workers=1)
    keys_v1 = _lane_keys("cells")

    @step(as_table=True, version="2")
    def src2():
        return pl.DataFrame({"id": ["a"], "score": [99]})

    @step(shape="expand", row_key="id")
    def cells2(src2):
        return src2

    p2 = pipeline(name="rk-id", steps=[src2, cells2], home=TEST_HOME)
    p2.run(workers=1)
    keys_v2 = [
        r.get("lane_key")
        for r in TEST_HOME.lanes.all_filled_rows()
        if r.get("step_name") == "cells2"
    ]
    assert keys_v1 == keys_v2


def test_expand_from_table_duplicate_row_key_errors():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def src():
        return pl.DataFrame({"id": ["a", "a"], "score": [1, 2]})

    @step(shape="expand", row_key="id")
    def cells(src):
        return src

    pipe = pipeline(name="rk-dup", steps=[src, cells], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1


def test_expand_from_table_missing_row_key_errors():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def src():
        return pl.DataFrame({"id": ["a", None], "score": [1, 2]})

    @step(shape="expand", row_key="id")
    def cells(src):
        return src

    pipe = pipeline(name="rk-miss", steps=[src, cells], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1


def test_as_table_on_expand_raises_at_decoration():
    with pytest.raises(ValueError, match="as_table=True is not valid with shape='expand'"):

        @step(as_table=True)
        def rows():
            yield {"a": 1}


def test_row_key_on_map_raises():
    with pytest.raises(ValueError, match="row_key= is only valid with shape='expand'"):

        @step(row_key="id")
        def src():
            return {"id": "a"}


def test_as_table_returning_dict_errors():
    @step(as_table=True)
    def load_data():
        return {"name": "alice"}

    pipe = pipeline(name="t-dict-out", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    err = " ".join((f.get("error_message") or "") for f in summary.failures())
    assert "as_table=True" in err


def test_map_return_list_is_one_payload():
    @step
    def load_data():
        return [{"name": "alice"}, {"name": "bob"}]

    pipe = pipeline(name="t-list-map", steps=[load_data], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 1
    assert _lane_keys("load_data") == ["@root"]
    assert summary.output_for("load_data")["@root"] == [
        {"name": "alice"},
        {"name": "bob"},
    ]


def test_definition_snapshots_as_table_row_key_and_table_input():
    import pyarrow as pa
    from rubedo.spec import definition

    @step(as_table=True)
    def patients():
        return pa.table({"id": [1]})

    @step(shape="expand", row_key="id")
    def cells(patients):
        return patients

    @step(shape="aggregate")
    def total(cells: pa.Table):
        return {"n": cells.num_rows}

    snap = definition(
        pipeline(name="def-grain", steps=[patients, cells, total], home=TEST_HOME).spec
    )
    by_name = {s["name"]: s for s in snap["steps"]}
    assert by_name["patients"]["as_table"] is True
    assert by_name["cells"]["shape"] == "expand"
    assert by_name["cells"]["row_key"] == "id"
    assert by_name["total"]["table_input"] is True
    assert "in_shape" not in by_name["cells"]
    assert "out_shape" not in by_name["cells"]
