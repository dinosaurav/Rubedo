"""Pairing-rule guard (invariant 8): every is_live/refreshed_at flip must ship
a materialization_lifecycle row in the same transaction. The guard is enforced
at commit (not flush) because the supersede path flushes a demotion before its
lifecycle row can name the replacement.

The invalidate/supersede/restore/refresh paths themselves are exercised in
test_generations.py / test_step_policies.py — that whole suite passing with the
guard installed is the proof those paths pair correctly. Here we prove the guard
actually fires on an unpaired flip and clears its state on rollback."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import (
    ImmutabilityError,
    Materialization,
    MaterializationLifecycle,
)
from rubedo.store import init_store
from rubedo.util import utcnow_iso

TEST_FOLDER = ".test_pairing_guard_data"
ENV_FOLDER = ".test_pairing_guard_env"


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
    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()
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


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content — the
    replacement for the old folder=TEST_FOLDER source sugar (TODO 14)."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def seed_pipeline():
    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"].strip()

    pipe = pipeline(name="pg", steps=[scan, read])
    with open(os.path.join(TEST_FOLDER, "f1.txt"), "w") as f:
        f.write("hello")
    pipe.run(workers=1)
    return pipe


def test_is_live_flip_without_lifecycle_row_raises():
    seed_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.is_live = False  # a liveness transition with no lifecycle row
        with pytest.raises(ImmutabilityError, match="invariant 8"):
            session.commit()
        session.rollback()

    # The illegal flip did not persist.
    with get_session() as session:
        assert session.query(Materialization).first().is_live is True


def test_refreshed_at_flip_without_lifecycle_row_raises():
    seed_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.refreshed_at = utcnow_iso()
        with pytest.raises(ImmutabilityError, match="invariant 8"):
            session.commit()
        session.rollback()


def test_paired_flip_commits():
    seed_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.is_live = False
        session.add(
            MaterializationLifecycle(
                materialization_id=mat.id,
                action="invalidated",
                reason="paired flip",
                created_at=utcnow_iso(),
            )
        )
        session.commit()  # both halves present -> allowed

    with get_session() as session:
        assert session.query(Materialization).first().is_live is False


def test_lifecycle_row_for_a_different_materialization_does_not_satisfy_the_guard():
    """The pairing is per-materialization: a lifecycle row about mat B does not
    license an unlogged flip of mat A."""
    seed_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.is_live = False
        session.add(
            MaterializationLifecycle(
                materialization_id=mat.id + 999,  # some other (here nonexistent) mat
                action="invalidated",
                reason="wrong target",
                created_at=utcnow_iso(),
            )
        )
        with pytest.raises(ImmutabilityError, match="invariant 8"):
            session.commit()
        session.rollback()


def test_rollback_clears_tracking_state():
    seed_pipeline()
    # A failed (unpaired) commit followed by rollback must not leave the flagged
    # materialization id lingering in session.info to poison a later commit.
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.is_live = False
        with pytest.raises(ImmutabilityError):
            session.commit()
        session.rollback()
        assert session.info.get("_liveness_changed") is None
        assert session.info.get("_liveness_paired") is None

        # The same session can now do an unrelated, legal commit without the
        # stale flag re-triggering the guard.
        from rubedo.models import RunEvent

        session.add(
            RunEvent(
                run_id=None,
                timestamp=utcnow_iso(),
                level="info",
                event_type="probe",
                message="after rollback",
            )
        )
        session.commit()  # must not raise
