"""Run liveness: terminal-only stored status, heartbeat-derived running/interrupted.

"running" is never stored (a killed process would leave it lying forever).
An unfinished run reads as "running" while last_heartbeat_at is fresh and
"interrupted" once it goes stale — derived at read time by
effective_run_status(), which the query layer applies for the CLI and API.
"""

import os
import shutil
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import (
    RUN_HEARTBEAT_STALE_SECONDS,
    Run,
    effective_run_status,
)
from rubedo.queries import get_recent_runs, get_run_summary
from rubedo.store import init_store
from rubedo.util import utcnow_iso

TEST_FOLDER = ".test_run_liveness_data"
ENV_FOLDER = ".test_run_liveness_env"


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


def _stale_iso() -> str:
    then = datetime.now(timezone.utc) - timedelta(
        seconds=RUN_HEARTBEAT_STALE_SECONDS + 60
    )
    return then.isoformat().replace("+00:00", "Z")


def _insert_unfinished_run(run_id: str, heartbeat_at: str):
    """An in-flight run row: no status, only a heartbeat — legal to insert."""
    with get_session() as session:
        session.add(
            Run(
                id=run_id,
                kind="process",
                pipeline_id="p",
                started_at=heartbeat_at,
                last_heartbeat_at=heartbeat_at,
            )
        )
        session.commit()


def test_completed_run_stores_terminal_status_and_heartbeat():
    with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
        f.write("hello")

    @step
    def scan():
        """Folder recipe: walk TEST_FOLDER, yield each file's content."""
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def upper(scan):
        return {"text": scan["text"].upper()}

    summary = pipeline(name="lv", steps=[scan, upper]).run()
    assert summary.status == "completed"

    with get_session() as session:
        row = session.query(Run).filter_by(id=summary.run_id).one()
        assert row.status == "completed"  # stored, not derived
        assert row.last_heartbeat_at is not None
        assert effective_run_status(row) == "completed"


def test_fresh_heartbeat_derives_running():
    _insert_unfinished_run("run_fresh", utcnow_iso())
    with get_session() as session:
        row = session.query(Run).filter_by(id="run_fresh").one()
        assert row.status is None
        assert effective_run_status(row) == "running"


def test_stale_heartbeat_derives_interrupted():
    _insert_unfinished_run("run_stale", _stale_iso())
    with get_session() as session:
        row = session.query(Run).filter_by(id="run_stale").one()
        assert effective_run_status(row) == "interrupted"


def test_terminal_status_wins_over_heartbeat():
    """A finished run stays finished no matter how old its heartbeat gets."""
    _insert_unfinished_run("run_done", _stale_iso())
    with get_session() as session:
        row = session.query(Run).filter_by(id="run_done").one()
        row.status = "failed"
        row.finished_at = utcnow_iso()
        session.commit()
        assert effective_run_status(row) == "failed"


def test_query_layer_reports_derived_status():
    """The CLI/API read path shows running/interrupted, never a NULL status."""
    _insert_unfinished_run("run_a", utcnow_iso())
    _insert_unfinished_run("run_b", _stale_iso())

    with get_session() as session:
        by_id = {r.id: r.status for r in get_recent_runs(session)}
        assert by_id["run_a"] == "running"
        assert by_id["run_b"] == "interrupted"

        detail = get_run_summary(session, "run_b")
        assert detail is not None and detail.status == "interrupted"
