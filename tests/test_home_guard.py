"""TODO 34 (buildable slice): one home per process, enforced.

_init_home (runner.py) repoints module-global DB/object-store/lane-table
state, so two concurrent runs in one process targeting *different* homes
would otherwise silently switch each other's backing store mid-run. The
guard tracks the process's single active effective home (None = the
ambient default) plus a live-run count, and raises a clear error naming
both homes when a run starts with a conflicting home while the count is
nonzero. Same-home concurrency and the no-home default path must pass
through untouched.
"""

import os
import shutil
import threading
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import pipeline, step
from rubedo.db import init_db
from rubedo.store import init_store
import rubedo.runner as runner_module

TEST_FOLDER = ".test_home_guard_data"
ENV_FOLDER = ".test_home_guard_env"


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

    # A prior test failure could in principle leave the guard held; never
    # let that leak across tests.
    runner_module._active_home = None
    runner_module._active_home_runs = 0

    yield

    runner_module._active_home = None
    runner_module._active_home_runs = 0

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def _home(name):
    return os.path.join(os.path.abspath(ENV_FOLDER), name)


def test_concurrent_different_homes_raises():
    """A run held open on home A; a second run targeting home B while A is
    in flight must raise, naming both homes."""
    home_a = _home("homeA")
    home_b = _home("homeB")

    gate = threading.Event()
    started = threading.Event()

    @step
    def slow():
        started.set()
        assert gate.wait(timeout=5), "test gate never opened"
        yield {"v": 1}

    pipe_a = pipeline(name="guard_a", steps=[slow], home=home_a)

    outcome = {}

    def run_a():
        try:
            outcome["summary"] = pipe_a.run()
        except Exception as exc:  # pragma: no cover - surfaced via assertion
            outcome["error"] = exc

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    try:
        assert started.wait(timeout=5), "home-A run never started"

        @step
        def quick():
            yield {"v": 2}

        pipe_b = pipeline(name="guard_b", steps=[quick], home=home_b)

        with pytest.raises(RuntimeError) as excinfo:
            pipe_b.run()

        message = str(excinfo.value)
        assert home_a in message
        assert home_b in message
    finally:
        gate.set()
        thread_a.join(timeout=5)

    assert "error" not in outcome
    assert outcome["summary"].status == "completed"
    # The guard must be fully released once both attempts have finished.
    assert runner_module._active_home_runs == 0


def test_concurrent_same_home_passes_through():
    """Two threads racing the same explicit home must not trip the guard —
    only *conflicting* homes are rejected."""
    home = _home("homeSame")

    gate = threading.Event()
    started = threading.Event()

    @step
    def slow():
        started.set()
        assert gate.wait(timeout=5), "test gate never opened"
        yield {"v": 1}

    pipe_a = pipeline(name="same_a", steps=[slow], home=home)

    outcome = {}

    def run_a():
        outcome["summary_a"] = pipe_a.run()

    thread_a = threading.Thread(target=run_a)
    thread_a.start()
    try:
        assert started.wait(timeout=5), "first run never started"

        @step
        def quick():
            yield {"v": 2}

        pipe_b = pipeline(name="same_b", steps=[quick], home=home)
        summary_b = pipe_b.run()
        assert summary_b.status == "completed"
    finally:
        gate.set()
        thread_a.join(timeout=5)

    assert outcome["summary_a"].status == "completed"
    assert runner_module._active_home_runs == 0


def test_sequential_different_homes_still_work():
    """The guard only rejects *concurrent* conflicting homes — one run
    finishing before the next starts must work exactly as before."""
    home_a = _home("homeSeqA")
    home_b = _home("homeSeqB")

    @step
    def quick():
        yield {"v": 1}

    pipe_a = pipeline(name="seq_a", steps=[quick], home=home_a)
    pipe_b = pipeline(name="seq_b", steps=[quick], home=home_b)

    summary_a = pipe_a.run()
    summary_b = pipe_b.run()

    assert summary_a.status == "completed"
    assert summary_b.status == "completed"
    assert runner_module._active_home_runs == 0


def test_no_home_default_untouched():
    """A run with no home= (the ambient default) must run exactly as
    before, with the guard passing through silently."""

    @step
    def quick():
        yield {"v": 1}

    pipe = pipeline(name="default_home", steps=[quick])
    summary = pipe.run()

    assert summary.status == "completed"
    assert runner_module._active_home_runs == 0
    assert runner_module._active_home is None


def test_explicit_home_conflicts_with_inflight_default_run():
    """The default (no home=) path counts as its own effective home for
    the guard: an explicit home= must not silently repoint state out from
    under an in-flight default-home run."""
    gate = threading.Event()
    started = threading.Event()

    @step
    def slow():
        started.set()
        assert gate.wait(timeout=5), "test gate never opened"
        yield {"v": 1}

    pipe_default = pipeline(name="default_slow", steps=[slow])

    outcome = {}

    def run_default():
        outcome["summary"] = pipe_default.run()

    thread_default = threading.Thread(target=run_default)
    thread_default.start()
    try:
        assert started.wait(timeout=5), "default-home run never started"

        @step
        def quick():
            yield {"v": 2}

        home_b = _home("homeConflict")
        pipe_b = pipeline(name="conflict_b", steps=[quick], home=home_b)

        with pytest.raises(RuntimeError) as excinfo:
            pipe_b.run()

        message = str(excinfo.value)
        assert "default" in message.lower()
        assert home_b in message
    finally:
        gate.set()
        thread_default.join(timeout=5)

    assert outcome["summary"].status == "completed"
    assert runner_module._active_home_runs == 0
