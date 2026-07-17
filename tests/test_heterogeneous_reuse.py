"""Regression tests for heterogeneous dict output reuse.

When a step's lanes return dicts with different key sets, pyarrow unions
the struct fields and null-fills missing ones — ``{"a": 1}`` reads back
as ``{"a": 1, "b": None}`` when another lane has a ``b`` key.  Without
canonicalization (stripping None-valued keys before hashing/comparing),
this causes:

1. One-time downstream cache bust: a reused step's identity changes
   between commit time (original dict) and plan time (read-back dict),
   so downstream lanes recompute once.

2. Permanent phantom churn on expand children: the _outputs_equal check
   compares the fresh dict (no extra keys) vs the null-filled read-back
   → never equal → mat_action="created" every run, Arrow file growing.

These tests verify that canonicalization makes identity stable across
the Arrow write/read round-trip, so both symptoms are fixed.
"""
import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo import lane_store
from rubedo.store import init_store

TEST_FOLDER = ".test_hetero_data"
ENV_FOLDER = ".test_hetero_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test = os.path.abspath(TEST_FOLDER)
    abs_env = os.path.abspath(ENV_FOLDER)
    for d in (abs_test, abs_env):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{abs_env}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    init_store()

    yield

    for d in (abs_test, abs_env):
        if os.path.exists(d):
            shutil.rmtree(d)


def test_heterogeneous_expand_root_no_phantom_churn():
    """Expand root yields heterogeneous dicts.  On re-run, children
    should be 'reused' (not 'created' every run) — no duplicate Arrow
    rows, no file growth."""
    @step(shape="expand")
    def source():
        yield {"a": "x", "b": "y"}
        yield {"a": "z"}  # no "b" key — Arrow null-fills it on read-back

    @step
    def transform(source: dict):
        return {"out": source["a"] + "_t"}

    @step
    def leaf(transform: dict):
        return {"final": transform["out"] + "_f"}

    pipe = pipeline(name="hetero_expand", steps=[source, transform, leaf])

    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 6  # 2 expand + 2 transform + 2 leaf

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0, f"phantom churn: {s2.created_count} created"
    assert s2.reused_count == 6

    s3 = pipe.run(workers=1)
    assert s3.created_count == 0, f"phantom churn: {s3.created_count} created"
    assert s3.reused_count == 6


def test_heterogeneous_map_step_no_downstream_bust():
    """A non-root map step returns heterogeneous dicts.  On re-run, the
    step is reused and its downstream should also reuse — no one-time
    cache bust from the identity mismatch."""
    @step(shape="expand")
    def source():
        yield {"val": "x"}
        yield {"val": "z"}

    @step
    def process(source: dict):
        if source["val"] == "x":
            return {"a": source["val"], "b": "extra"}
        return {"a": source["val"]}  # no "b" key

    @step
    def leaf(process: dict):
        return {"final": process["a"] + "_f"}

    pipe = pipeline(name="hetero_map", steps=[source, process, leaf])

    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 6  # 2 source + 2 process + 2 leaf

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0, f"downstream bust: {s2.created_count} created"
    assert s2.reused_count == 6

    s3 = pipe.run(workers=1)
    assert s3.created_count == 0
    assert s3.reused_count == 6


def test_heterogeneous_nested_dicts_reuse():
    """Heterogeneous nested dicts (a dict value inside the output dict)
    also reuse correctly — canonicalization is recursive."""
    @step(shape="expand")
    def source():
        yield {"meta": {"name": "a", "tag": "t1"}, "v": 1}
        yield {"meta": {"name": "b"}, "v": 2}  # no "tag" key in nested dict

    @step
    def downstream(source: dict):
        return {"out": source["meta"]["name"]}

    pipe = pipeline(name="hetero_nested", steps=[source, downstream])

    s1 = pipe.run(workers=1)
    assert s1.failed_count == 0
    assert s1.created_count == 4  # 2 source + 2 downstream

    s2 = pipe.run(workers=1)
    assert s2.created_count == 0, f"nested bust: {s2.created_count} created"
    assert s2.reused_count == 4


def test_arrow_row_count_stable_across_runs():
    """Verify that the Arrow file for a heterogeneous expand root does
    not grow across runs — no duplicate rows from phantom churn."""
    @step(shape="expand")
    def source():
        yield {"a": "x", "b": "y"}
        yield {"a": "z"}

    @step
    def transform(source: dict):
        return {"out": source["a"]}

    pipe = pipeline(name="hetero_rows", steps=[source, transform])

    pipe.run(workers=1)
    rows_after_r1 = lane_store.get_filled_rows("hetero_rows", "source")
    assert len(rows_after_r1) == 2  # 2 children (anchor in separate file)

    pipe.run(workers=1)
    rows_after_r2 = lane_store.get_filled_rows("hetero_rows", "source")
    assert len(rows_after_r2) == 2, (
        f"Arrow file grew: {len(rows_after_r1)} → {len(rows_after_r2)} rows"
    )

    pipe.run(workers=1)
    rows_after_r3 = lane_store.get_filled_rows("hetero_rows", "source")
    assert len(rows_after_r3) == 2
