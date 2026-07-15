"""plan(): dry-run answers "what would run() do" without writing anything."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline
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


def make_pipeline(pipe_id="pl"):
    # Folder recipe: walk TEST_FOLDER, yield each file's content. A root
    # expand has no parent to cache its enumeration against, so it always
    # plans as "execute": plan() can never preview what it will yield
    # without actually running it (see the tests below).
    @step
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def read(scan):
        return scan["text"].strip()

    @step
    def upper(read):
        return read.upper()

    return pipeline(name=pipe_id, steps=[scan, read, upper])


def actions(run_plan):
    return {(i.coordinate, i.step_name): i.action for i in run_plan.items}


def test_fresh_state_executes_source_and_pends_downstream():
    pipe = make_pipeline()
    create_file("f1.txt", "hello")

    p = pipe.plan()
    # One execute for the source (a root expand step — no parent to
    # cache its enumeration against, so it always executes) and one
    # "pending" per downstream step, not per file: the individual file
    # lanes don't exist yet at plan time.
    assert actions(p) == {
        ("@root", "scan"): "execute",
        ("@root", "read"): "pending",
        ("@root", "upper"): "pending",
    }
    assert p.counts == {"execute": 1, "pending": 2}


def test_second_plan_still_shows_execute_and_pending_not_reuse():
    """A root expand (the source) never writes a cache anchor for its own
    enumeration — it has no parent to key one on — so it always plans as
    "execute", forever, even immediately after a completed run. Downstream
    steps then can never resolve past "pending" in a dry-run plan(): their
    real lanes are only known by actually running the generator. This is
    the counterpart to the Trap's "second *run*'s planning reuses
    unchanged lanes" (verified via run()'s reused_count in
    examples/count_lines and examples/newsroom) — plan() itself, being a
    pure dry-run, can't reach into a hypothetical future execution."""
    pipe = make_pipeline()
    create_file("f1.txt", "hello")
    pipe.run(workers=1)

    p = pipe.plan()
    assert actions(p) == {
        ("@root", "scan"): "execute",
        ("@root", "read"): "pending",
        ("@root", "upper"): "pending",
    }
    assert p.counts == {"execute": 1, "pending": 2}


def test_invalidation_does_not_change_the_coarse_plan_shape():
    pipe = make_pipeline()
    create_file("f1.txt", "hello")
    pipe.run(workers=1)

    invalidate(Selection(step="read"), reason="redo")
    p = pipe.plan()
    # Same coarse shape as any fresh/cached state — invalidating a lane
    # that plan() can't even see yet has no visible effect on a dry-run.
    assert actions(p)[("@root", "scan")] == "execute"
    assert actions(p)[("@root", "read")] == "pending"


def test_plan_cannot_preview_effect_of_a_deleted_file():
    """plan() can't see individual files at all — deleting one changes
    nothing about the coarse execute+pending shape. The real behavior (a
    run simply not touching the vanished file's lane) is covered at the
    run() level by test_engine.py::test_logical_deletion."""
    pipe = make_pipeline()
    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")
    pipe.run(workers=1)

    before = actions(pipe.plan())
    os.remove(os.path.join(TEST_FOLDER, "f2.txt"))
    after = actions(pipe.plan())
    assert before == after == {
        ("@root", "scan"): "execute",
        ("@root", "read"): "pending",
        ("@root", "upper"): "pending",
    }


def test_plan_writes_nothing():
    pipe = make_pipeline()
    create_file("f1.txt", "hello")
    pipe.run(workers=1)

    with get_session() as session:
        runs_before = session.query(Run).count()
        events_before = session.query(RunEvent).count()

    pipe.plan()
    pipe.plan(force=True)

    with get_session() as session:
        assert session.query(Run).count() == runs_before
        assert session.query(RunEvent).count() == events_before


def test_run_resolves_every_planned_pending_to_created():
    """A fresh store's plan() says execute+pending everywhere; actually
    running it must turn all of that into "created" (never "reused" — there
    was nothing to reuse yet)."""
    pipe = make_pipeline()
    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")

    summary = pipe.run(workers=1)
    assert summary.reused_count == 0
    assert summary.created_count == 6  # 2 files x (scan + read + upper)

    # And now that it's cached, a second run reuses everything — this is
    # the "per-lane reuse" the Trap describes, visible via run(), not a
    # standalone plan() call (see test_second_plan_still_shows_execute_and_pending_not_reuse).
    summary2 = pipe.run(workers=1)
    assert summary2.created_count == 0
    assert summary2.reused_count == 6
