"""Arrow IPC serialization for DataFrame / pa.Table outputs.

Covers Phase 1 of the Arrow storage plan (notes/arrow-storage.md): a
polars/pandas DataFrame or bare pyarrow Table returned from a step is
serialized as Arrow IPC bytes and content-addressed like any other object;
on cache hit the original Python type is reconstructed."""

import os
import shutil

import pyarrow as pa
import pytest

from rubedo import pipeline, step
from conftest import make_home

TEST_FOLDER = ".test_arrow_data"
ENV_FOLDER = ".test_arrow_env"

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
# Low-level serializer round-trips
# ---------------------------------------------------------------------------


def test_serialize_polars_roundtrip():
    import polars as pl

    from rubedo.store import _serialize, _from_arrow_table

    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    raw, ct = _serialize(df)
    assert ct == "arrow-ipc:polars"
    assert isinstance(raw, bytes)

    pa  # module-level import above
    reader = pa.ipc.open_stream(raw)
    tbl = reader.read_all()
    restored = _from_arrow_table(tbl, "polars")
    assert isinstance(restored, pl.DataFrame)
    assert restored.shape == (3, 2)
    assert restored["a"].to_list() == [1, 2, 3]


def test_serialize_pa_table_roundtrip():
    import pyarrow as pa

    from rubedo.store import _serialize

    tbl = pa.table({"x": [1.0, 2.0], "y": [True, False]})
    raw, ct = _serialize(tbl)
    assert ct == "arrow-ipc:table"

    pa  # module-level import above
    reader = pa.ipc.open_stream(raw)
    restored = reader.read_all()
    assert isinstance(restored, pa.Table)
    assert restored.num_rows == 2


def test_serialize_pandas_roundtrip():
    pd = pytest.importorskip("pandas")

    from rubedo.store import _serialize, _from_arrow_table

    df = pd.DataFrame({"a": [10, 20], "b": ["p", "q"]})
    raw, ct = _serialize(df)
    assert ct == "arrow-ipc:pandas"

    pa  # module-level import above
    reader = pa.ipc.open_stream(raw)
    tbl = reader.read_all()
    restored = _from_arrow_table(tbl, "pandas")
    assert isinstance(restored, pd.DataFrame)
    assert list(restored["a"]) == [10, 20]


def test_json_path_unchanged():
    """A dict output still serializes as JSON — the Arrow branch never fires."""
    from rubedo.store import _serialize

    raw, ct = _serialize({"k": "v", "n": 3})
    assert ct == "json"
    assert b'"k":"v"' in raw


def test_bytes_path_unchanged():
    from rubedo.store import _serialize

    raw, ct = _serialize(b"\x00\x01")
    assert ct == "bytes"
    assert raw == b"\x00\x01"


def test_text_path_unchanged():
    from rubedo.store import _serialize

    raw, ct = _serialize("plain string")
    assert ct == "text"
    assert raw == b"plain string"


# ---------------------------------------------------------------------------
# End-to-end: a step returning a DataFrame caches, reuses, round-trips
# ---------------------------------------------------------------------------


def test_dataframe_step_caches_and_reuses():
    """The headline Phase 1 win: a step that returns a DataFrame is cached
    as Arrow IPC and reused across runs, not requiring skip_cache=True.

    Run twice, expect the second run to reuse (no re-execution), and the
    output_for() helper to hand back a DataFrame of the same shape."""
    import polars as pl

    call_count = 0

    @step
    def make_df():
        nonlocal call_count
        call_count += 1
        return pl.DataFrame({"amount": [0, 500_000, 10], "name": ["X", "Y", "Z"]})

    p = pipeline(name="arrow-cached", steps=[make_df], home=TEST_HOME)
    p.run(workers=1)
    assert call_count == 1

    summary = p.run(workers=1)
    assert summary.reused_count == 1
    assert summary.created_count == 0
    assert call_count == 1  # not re-executed

    out = summary.output_for("make_df")
    assert "@root" in out
    df = out["@root"]
    assert isinstance(df, pl.DataFrame)
    assert df["amount"].to_list() == [0, 500_000, 10]


def test_dataframe_recompute_on_version_bump():
    """A version bump re-executes the step; the new DataFrame supersedes the
    old.  Confirms the Arrow path participates in generations like any
    other output."""
    import polars as pl

    calls = 0

    @step(version="0")
    def gen():
        nonlocal calls
        calls += 1
        return pl.DataFrame({"v": [calls]})

    p = pipeline(name="arrow-ver", steps=[gen], home=TEST_HOME)
    p.run(workers=1)
    assert calls == 1

    @step(version="1")
    def gen2():
        nonlocal calls
        calls += 1
        return pl.DataFrame({"v": [calls]})

    p2 = pipeline(name="arrow-ver", steps=[gen2], home=TEST_HOME)
    s = p2.run(workers=1)
    assert calls == 2
    assert s.created_count == 1
    out = s.output_for("gen2")
    assert out["@root"]["v"].to_list() == [2]