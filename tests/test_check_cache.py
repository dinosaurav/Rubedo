"""check_cache=False: per-step force — always re-execute, still commit.

check_cache=False is the per-step equivalent of --force: plan skips the
reuse check and emits "execute" every run, but the commit path is
unaffected — the result lands in cache, so downstream steps reuse and a
later run with check_cache=True (default) sees the fresh output.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo.lane_store import init_tables, clear_run_buffers
from rubedo.store import init_store

ENV_FOLDER = ".test_check_cache_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    if os.path.exists(abs_env_folder):
        shutil.rmtree(abs_env_folder)
    os.makedirs(abs_env_folder, exist_ok=True)

    import rubedo.store
    import rubedo.lane_store

    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"
    rubedo.lane_store.TABLES_DIR = f"{abs_env_folder}/tables"
    init_tables()
    clear_run_buffers()
    rubedo.lane_store.clear_read_caches()

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

    clear_run_buffers()
    rubedo.lane_store.clear_read_caches()
    if os.path.exists(abs_env_folder):
        shutil.rmtree(abs_env_folder)


def test_check_cache_false_reruns_but_commits():
    """A check_cache=False step re-executes every run, but its output is
    committed — so a downstream step (check_cache=True) reuses on run 2."""
    root_calls = []
    child_calls = []

    @step(check_cache=False)
    def root():
        root_calls.append(1)
        return {"value": 42}

    @step
    def child(root: dict):
        child_calls.append(1)
        return {"doubled": root["value"] * 2}

    pipe = pipeline(name="cc", steps=[root, child])
    r1 = pipe.run(workers=1)
    assert len(root_calls) == 1
    assert len(child_calls) == 1
    assert r1.created_count == 2

    r2 = pipe.run(workers=1)
    assert len(root_calls) == 2  # re-executed (check_cache=False)
    assert len(child_calls) == 1  # NOT re-run — reused (root's output identical)
    # Root re-executed but produced the same output → mat_action "reused"
    # (same address, same output_identity). The run summary counts the
    # materialization action, not whether the function ran.
    assert r2.reused_count == 2


def test_check_cache_true_default_reuses():
    """Default (check_cache=True): both steps reuse on run 2."""
    root_calls = []

    @step
    def root():
        root_calls.append(1)
        return {"value": 42}

    @step
    def child(root: dict):
        return {"doubled": root["value"] * 2}

    pipe = pipeline(name="cc_default", steps=[root, child])
    pipe.run(workers=1)
    assert len(root_calls) == 1
    r2 = pipe.run(workers=1)
    assert len(root_calls) == 1  # reused
    assert r2.reused_count == 2


def test_check_cache_false_then_true_reuses():
    """After a check_cache=False run, switching to check_cache=True
    should reuse the committed output (it's in cache)."""
    root_calls = []

    def make_root(check):
        @step(check_cache=check)
        def root():
            root_calls.append(1)
            return {"value": 42}

        return root

    @step
    def child(root: dict):
        return {"doubled": root["value"] * 2}

    root1 = make_root(False)
    pipe = pipeline(name="cc_switch", steps=[root1, child])
    pipe.run(workers=1)
    assert len(root_calls) == 1

    root2 = make_root(True)
    # Same step name "root", different check_cache setting — version is
    # still "0", so the address is the same and the cached output is seen.
    pipe2 = pipeline(name="cc_switch", steps=[root2, child])
    r2 = pipe2.run(workers=1)
    assert len(root_calls) == 1  # reused this time
    assert r2.reused_count == 2


def test_check_cache_false_with_skip_cache_raises():
    with pytest.raises(ValueError, match="contradictory with skip_cache"):

        @step(check_cache=False, skip_cache=True)
        def util():
            return {"x": 1}


def test_check_cache_false_with_stale_after_raises():
    with pytest.raises(ValueError, match="meaningless with check_cache=False"):

        @step(check_cache=False, stale_after="24h")
        def scraper():
            return {"data": "scraped"}


def test_check_cache_false_in_definition_snapshot():
    from rubedo.spec import definition

    @step(check_cache=False)
    def root():
        return {"value": 1}

    @step
    def child(root: dict):
        return {"y": root["value"]}

    pipe = pipeline(name="cc_snap", steps=[root, child])
    snap = definition(pipe.spec)
    root_entry = next(s for s in snap["steps"] if s["name"] == "root")
    assert root_entry.get("check_cache") is False
    child_entry = next(s for s in snap["steps"] if s["name"] == "child")
    assert "check_cache" not in child_entry  # default True omitted
