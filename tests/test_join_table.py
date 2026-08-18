"""shape='join_table': one coordinate, table payloads, join_on/join_mode invariants."""
import pytest

from rubedo import pipeline, step
from conftest import isolated_test_env

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("join_table") as env:
        TEST_HOME = env.home
        yield


def _lane_keys(step_name):
    return sorted(
        r.get("lane_key")
        for r in TEST_HOME.lanes.all_filled_rows()
        if r.get("step_name") == step_name
    )


def test_join_table_one_coordinate_and_reuse():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def patients():
        return pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    @step(as_table=True)
    def claims():
        return pl.DataFrame({"patient_id": [1, 2], "dx": ["x", "y"]})

    @step(shape="join_table", join_on={"patients": "id", "claims": "patient_id"})
    def joined(patients, claims):
        return patients.join(claims, left_on="id", right_on="patient_id")

    pipe = pipeline(
        name="jt-fn", steps=[patients, claims, joined], home=TEST_HOME
    )
    assert joined.as_table is True
    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 3
    assert _lane_keys("joined") == ["@all"]
    out = s1.output_for("joined")["@all"]
    assert out.height == 2

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 3


def test_p_join_table_declarative():
    pl = pytest.importorskip("polars")

    p = pipeline(name="jt-decl", home=TEST_HOME)

    @p.step(as_table=True)
    def patients():
        return pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    @p.step(as_table=True)
    def claims():
        return pl.DataFrame({"patient_id": [1, 2], "dx": ["x", "y"]})

    p.join_table(name="joined", join_on={"patients": "id", "claims": "patient_id"})

    s1 = p.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 3
    assert _lane_keys("joined") == ["@all"]
    out = s1.output_for("joined")["@all"]
    assert out.height == 2
    assert "name" in out.columns
    assert "dx" in out.columns

    s2 = p.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 3


def test_join_table_union_keeps_unmatched():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1, 2], "l": ["a", "b"]})

    @step(as_table=True)
    def right():
        return pl.DataFrame({"k": [1, 3], "r": ["x", "z"]})

    @step(
        shape="join_table",
        join_on={"left": "k", "right": "k"},
        join_mode="union",
    )
    def joined(left, right):
        return left.join(right, on="k", how="full")

    pipe = pipeline(name="jt-union", steps=[left, right, joined], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    out = summary.output_for("joined")["@all"]
    assert out.height == 3


def test_join_table_null_key_errors():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1, None], "l": ["a", "b"]})

    @step(as_table=True)
    def right():
        return pl.DataFrame({"k": [1], "r": ["x"]})

    @step(shape="join_table", join_on={"left": "k", "right": "k"})
    def joined(left, right):
        return left.join(right, on="k")

    pipe = pipeline(name="jt-null", steps=[left, right, joined], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    err = " ".join((f.get("error_message") or "") for f in summary.failures())
    assert "null" in err.lower()


def test_join_table_duplicate_keys_warn_and_cartesian():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1, 1], "l": ["a", "b"]})

    @step(as_table=True)
    def right():
        return pl.DataFrame({"k": [1], "r": ["x"]})

    @step(shape="join_table", join_on={"left": "k", "right": "k"})
    def joined(left, right):
        return left.join(right, on="k")

    pipe = pipeline(name="jt-dup", steps=[left, right, joined], home=TEST_HOME)
    with pytest.warns(UserWarning, match="duplicate"):
        summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.output_for("joined")["@all"].height == 2


def test_join_table_rejects_skip_cache_parent():
    pl = pytest.importorskip("polars")

    @step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1]})

    @step(use_cache=False, as_table=True)
    def right(left):
        return left

    @step(shape="join_table", join_on={"left": "k", "right": "k"})
    def joined(left, right):
        return left

    with pytest.raises(ValueError, match="use_cache=False parent"):
        pipeline(name="jt-skip", steps=[left, right, joined], home=TEST_HOME).spec


def test_join_on_does_not_infer_join_table():
    s = step(depends_on=["a", "b"], join_on={"a": "k", "b": "k"})(lambda a, b: None)
    assert s.shape == "join"
    assert s.as_table is False


def test_join_table_dict_parent_fails():
    @step
    def left():
        yield {"k": 1, "l": "a"}

    @step
    def right():
        yield {"k": 1, "r": "x"}

    @step(shape="join_table", join_on={"left": "k", "right": "k"})
    def joined(left, right):
        return left

    pipe = pipeline(name="jt-dict", steps=[left, right, joined], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1
    err = " ".join((f.get("error_message") or "") for f in summary.failures())
    assert "as_table=True" in err or "table-valued" in err


def test_p_join_table_declarative_union():
    pl = pytest.importorskip("polars")

    p = pipeline(name="jt-decl-union", home=TEST_HOME)

    @p.step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1, 2], "l": ["a", "b"]})

    @p.step(as_table=True)
    def right():
        return pl.DataFrame({"k": [1, 3], "r": ["x", "z"]})

    p.join_table(
        name="joined",
        join_on={"left": "k", "right": "k"},
        join_mode="union",
    )

    summary = p.run(workers=1)
    assert summary.failed_count == 0
    out = summary.output_for("joined")["@all"]
    assert out.height == 3


def test_join_table_describe_and_definition():
    pl = pytest.importorskip("polars")
    from rubedo.spec import definition

    p = pipeline(name="jt-desc", home=TEST_HOME)

    @p.step(as_table=True)
    def left():
        return pl.DataFrame({"k": [1]})

    @p.step(as_table=True)
    def right():
        return pl.DataFrame({"k": [1]})

    p.join_table(name="joined", join_on={"left": "k", "right": "k"})

    text = p.describe(format="ascii")
    assert "[join_table]" in text
    snap = definition(p.spec)
    by_name = {s["name"]: s for s in snap["steps"]}
    assert by_name["joined"]["shape"] == "join_table"
    assert by_name["joined"]["as_table"] is True
    assert by_name["joined"]["join_on"] == {"left": "k", "right": "k"}
    assert "in_shape" not in by_name["joined"]
