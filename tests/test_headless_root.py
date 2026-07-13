"""Source-less root steps: a map root that mints a single '@root' lane.

A pipeline needs no Source and no expand root — a plain map step at the head
originates one lane whose input is its params (or a constant when it takes
none). Same params reuse; changed params recompute; and the lane feeds
downstream steps exactly like a scanned one.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import source, step, pipeline
from rubedo.db import init_db
from rubedo.store import init_store

TEST_FOLDER = ".test_headless_root_data"
ENV_FOLDER = ".test_headless_root_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

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

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def test_param_fed_root_runs_once_then_reuses():
    @step(name="head", version="1")
    def head(params):
        return {"seen": params["n"]}

    pipe = pipeline(name="headless", steps=[head])

    s1 = pipe.run(params={"n": 7})
    assert (s1.created_count, s1.reused_count) == (1, 0)
    assert s1.output_for("head") == {"@root": {"seen": 7}}

    s2 = pipe.run(params={"n": 7})
    assert (s2.created_count, s2.reused_count) == (0, 1)
    assert s2.output_for("head") == {"@root": {"seen": 7}}


def test_changed_params_recompute_and_old_params_still_cached():
    @step(name="head", version="1")
    def head(params):
        return {"seen": params["n"]}

    pipe = pipeline(name="headless", steps=[head])

    assert pipe.run(params={"n": 1}).created_count == 1
    # A different param value is a distinct address -> a new generation.
    assert pipe.run(params={"n": 2}).created_count == 1
    # The first value's output was never superseded (distinct address): reuse.
    assert pipe.run(params={"n": 1}).reused_count == 1


def test_root_with_no_params_is_a_constant():
    calls = {"n": 0}

    @step(name="head", version="1")
    def head():
        calls["n"] += 1
        return 42

    pipe = pipeline(name="const", steps=[head])

    s1 = pipe.run()
    assert (s1.created_count, s1.reused_count) == (1, 0)
    assert s1.output_for("head") == {"@root": 42}

    s2 = pipe.run()
    assert (s2.created_count, s2.reused_count) == (0, 1)
    # Executed exactly once across both runs.
    assert calls["n"] == 1


def test_root_lane_feeds_downstream_map():
    @step(name="head", version="1")
    def head(params):
        return {"base": params["base"]}

    @step(name="double", version="1", depends_on=["head"])
    def double(head):
        return head["base"] * 2

    pipe = pipeline(name="chain", steps=[head, double])

    s = pipe.run(params={"base": 21})
    assert s.created_count == 2
    assert s.output_for("double") == {"@root": 42}


def test_headless_map_root_and_expand_root_coexist():
    @source
    def rows():
        yield {"v": 1}
        yield {"v": 2}

    @step(name="config", version="1")
    def config():
        return {"scale": 10}

    pipe = pipeline(name="mixed", steps=[rows, config])

    s = pipe.run()
    # two expand children + one @root config lane
    assert s.created_count == 3
    assert s.output_for("config") == {"@root": {"scale": 10}}
    assert sorted(v["v"] for v in s.output_for("rows").values()) == [1, 2]


def test_bare_pipeline_with_no_source_and_no_root_is_rejected():
    @step(name="leaf", version="1", depends_on=["ghost"])
    def leaf(ghost):
        return ghost

    # Validation (at least one root) runs lazily on first verb/`.spec`
    # access, not at pipeline() construction time (TODO 15: no eager
    # .build() anymore).
    with pytest.raises(ValueError):
        pipeline(name="empty", steps=[leaf]).spec
