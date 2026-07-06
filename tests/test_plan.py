"""plan(): dry-run answers "what would run() do" without writing anything."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, plan, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Run, RunEvent
from rubedo.store import init_store

TEST_FOLDER = ".test_plan_data"
ENV_FOLDER = ".test_plan_env"


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


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def make_two_step_pipeline(pipe_id="pl"):
    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    return pipeline(id=pipe_id, name=pipe_id, folder=TEST_FOLDER, steps=[read, upper])


def actions(run_plan):
    return {(i.coordinate, i.step_name): i.action for i in run_plan.items}


def test_fresh_state_executes_roots_and_pends_downstream():
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")

    p = plan(pipe)
    assert actions(p) == {
        ("f1.txt", "read"): "execute",
        ("f1.txt", "upper"): "pending",
    }
    assert p.counts == {"execute": 1, "pending": 1}


def test_fully_cached_state_reuses_everything():
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")
    run(pipe, workers=1)

    p = plan(pipe)
    assert set(actions(p).values()) == {"reuse"}
    assert p.counts == {"reuse": 2}


def test_invalidation_shows_execute_and_pending_chain():
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")
    run(pipe, workers=1)

    invalidate(Selection(step="read"), reason="redo")
    p = plan(pipe)
    assert actions(p)[("f1.txt", "read")] == "execute"
    # Downstream depends on what the re-execution produces
    assert actions(p)[("f1.txt", "upper")] == "pending"


def test_deleted_coordinate_absent_from_plan():
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")
    run(pipe, workers=1)

    os.remove(os.path.join(TEST_FOLDER, "f2.txt"))
    p = plan(pipe)
    # A vanished coordinate simply isn't scanned, so it isn't in the plan —
    # no "removed" action. Only f1.txt remains, and it reuses.
    acts = actions(p)
    assert ("f2.txt", "read") not in acts
    assert ("f2.txt", "upper") not in acts
    assert acts[("f1.txt", "read")] == "reuse"


def test_plan_writes_nothing():
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")
    run(pipe, workers=1)

    with get_session() as session:
        runs_before = session.query(Run).count()
        events_before = session.query(RunEvent).count()

    plan(pipe)
    plan(pipe, force=True)

    with get_session() as session:
        assert session.query(Run).count() == runs_before
        assert session.query(RunEvent).count() == events_before


def test_plan_matches_run():
    """The plan's execute/reuse split is exactly what run() then does."""
    pipe = make_two_step_pipeline()
    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")
    run(pipe, workers=1)
    create_file("f3.txt", "new")

    p = plan(pipe)
    planned_executes = sum(1 for a in actions(p).values() if a == "execute")
    planned_pending = sum(1 for a in actions(p).values() if a == "pending")
    planned_reuses = sum(1 for a in actions(p).values() if a == "reuse")

    summary = run(pipe, workers=1)
    # Deterministic steps: pendings resolve to creates
    assert summary.created_count == planned_executes + planned_pending == 2
    assert summary.reused_count == planned_reuses == 4
