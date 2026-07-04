"""The ledger is append-only: guards reject illegal writes, history survives."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from batchbrain import Selection, invalidate, run, step, pipeline
from batchbrain.db import init_db, get_session
from batchbrain.models import (
    ImmutabilityError,
    Materialization,
    MaterializationLifecycle,
    Run,
    RunCoordinateStatus,
    RunEvent,
)
from batchbrain.store import init_store
from batchbrain.util import utcnow_iso

TEST_FOLDER = ".test_immutability_data"
ENV_FOLDER = ".test_immutability_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import batchbrain.store

    batchbrain.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    batchbrain.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["BATCHBRAIN_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import batchbrain.db

    if batchbrain.db.engine is not None:
        batchbrain.db.engine.dispose()

    from batchbrain.models import Base
    from sqlalchemy.orm import sessionmaker

    batchbrain.db.engine = create_engine(
        os.environ["BATCHBRAIN_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=batchbrain.db.engine)
    batchbrain.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=batchbrain.db.engine
    )

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def run_simple_pipeline(pipe_id="imm"):
    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    pipe = pipeline(id=pipe_id, name=pipe_id, folder=TEST_FOLDER, steps=[read])
    with open(os.path.join(TEST_FOLDER, "f1.txt"), "w") as f:
        f.write("hello")
    return pipe, run(pipe, workers=1)


def test_append_only_rows_reject_updates():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        rc = session.query(RunCoordinateStatus).first()
        rc.status = "tampered"
        with pytest.raises(ImmutabilityError, match="append-only"):
            session.commit()
        session.rollback()


def test_append_only_rows_reject_deletes():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        event = session.query(RunEvent).first()
        session.delete(event)
        with pytest.raises(ImmutabilityError, match="cannot be deleted"):
            session.commit()
        session.rollback()


def test_materialization_content_is_immutable():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.output_content_hash = "tampered"
        with pytest.raises(ImmutabilityError, match="immutable"):
            session.commit()
        session.rollback()


def test_materialization_liveness_is_the_only_legal_update():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        mat = session.query(Materialization).first()
        mat.is_live = False
        # Projection column is allowed, but invariant 8 requires the flip to
        # ship a lifecycle row in the same transaction (pairing guard).
        session.add(
            MaterializationLifecycle(
                materialization_id=mat.id,
                action="invalidated",
                reason="test",
                created_at=utcnow_iso(),
            )
        )
        session.commit()


def test_run_identity_is_immutable_but_lifecycle_is_not():
    pipe, summary = run_simple_pipeline()
    with get_session() as session:
        run_row = session.get(Run, summary.run_id)
        run_row.status = "completed"  # lifecycle projection: allowed
        session.commit()

    with get_session() as session:
        run_row = session.get(Run, summary.run_id)
        run_row.source_id = "folder:elsewhere"
        with pytest.raises(ImmutabilityError, match="immutable"):
            session.commit()
        session.rollback()


def test_restore_preserves_invalidation_history():
    pipe, _ = run_simple_pipeline()

    invalidate(Selection(step="read"), reason="looked wrong")
    # Deterministic step: rerun produces identical bytes -> restored, not new row
    summary = run(pipe, workers=1)
    assert summary.created_count == 1

    with get_session() as session:
        mat = session.query(Materialization).one()
        assert mat.is_live is True

        lifecycle = (
            session.query(MaterializationLifecycle)
            .filter_by(materialization_id=mat.id)
            .order_by(MaterializationLifecycle.id)
            .all()
        )
        assert [lc.action for lc in lifecycle] == ["invalidated", "restored"]
        assert lifecycle[0].reason == "looked wrong"
        assert lifecycle[0].run_id is not None, "who invalidated it is preserved"
