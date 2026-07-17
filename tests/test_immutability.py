"""The ledger is append-only: guards reject illegal writes, history survives."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline
from rubedo.db import init_db, get_session
from rubedo import lane_store
from rubedo.models import (
    ImmutabilityError,
    InputHashUsage,
    MaterializationEdge,
    Run,
    RunCoordinateStatus,
    RunEvent,
)
from rubedo.store import init_store

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


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def run_simple_pipeline(pipe_id="imm"):
    @step
    def read(scan):
        return scan["text"].strip()

    pipe = pipeline(name=pipe_id, steps=[scan, read])
    with open(os.path.join(TEST_FOLDER, "f1.txt"), "w") as f:
        f.write("hello")
    return pipe, pipe.run(workers=1)


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


def test_materialization_edge_is_immutable():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        edge = session.query(MaterializationEdge).first()
        edge.parent_address = "tampered"
        with pytest.raises(ImmutabilityError, match="append-only"):
            session.commit()
        session.rollback()


def test_input_hash_usage_liveness_is_mutable():
    pipe, _ = run_simple_pipeline()
    with get_session() as session:
        ihu = session.query(InputHashUsage).first()
        ihu.fulfilled = False
        ihu.last_run_id = "different"
        # InputHashUsage is the one intentionally mutable ledger table:
        # fulfilled/last_run_id legitimately update (claim/fulfill/invalidate).
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
    summary = pipe.run(workers=1)
    assert summary.created_count == 1

    with get_session() as session:
        # Only "read"'s materialization was invalidated; "scan"'s own lane
        # materialization is untouched, so filter to the step under test.
        read_rows = [r for r in lane_store.all_filled_rows() if r.get("step_name") == "read"]
        assert read_rows
        ihu = session.query(InputHashUsage).filter_by(address=read_rows[0]["address"]).first()
        assert ihu is not None and ihu.fulfilled is True
        # No lifecycle rows in the new model — liveness is
        # input_hash_usages.fulfilled.  The invalidation flipped fulfilled=False,
        # the rerun flipped it back to True.
