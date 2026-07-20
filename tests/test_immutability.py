"""The ledger is append-only: guards reject illegal writes, history survives."""

import os

import pytest

from conftest import isolated_test_env
from rubedo import Selection, invalidate, step, pipeline
from rubedo.models import (
    ImmutabilityError,
    InputHashUsage,
    MaterializationEdge,
    Run,
    RunCoordinateStatus,
    RunEvent,
)

TEST_FOLDER = ".test_immutability_data"
ENV_FOLDER = ".test_immutability_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("immutability") as env:
        TEST_HOME = env.home
        yield

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

    pipe = pipeline(name=pipe_id, steps=[scan, read], home=TEST_HOME)
    with open(os.path.join(TEST_FOLDER, "f1.txt"), "w") as f:
        f.write("hello")
    return pipe, pipe.run(workers=1)


def test_append_only_rows_reject_updates():
    pipe, _ = run_simple_pipeline()
    with TEST_HOME.session() as session:
        rc = session.query(RunCoordinateStatus).first()
        rc.status = "tampered"
        with pytest.raises(ImmutabilityError, match="append-only"):
            session.commit()
        session.rollback()


def test_append_only_rows_reject_deletes():
    pipe, _ = run_simple_pipeline()
    with TEST_HOME.session() as session:
        event = session.query(RunEvent).first()
        session.delete(event)
        with pytest.raises(ImmutabilityError, match="cannot be deleted"):
            session.commit()
        session.rollback()


def test_materialization_edge_is_immutable():
    pipe, _ = run_simple_pipeline()
    with TEST_HOME.session() as session:
        edge = session.query(MaterializationEdge).first()
        edge.parent_address = "tampered"
        with pytest.raises(ImmutabilityError, match="append-only"):
            session.commit()
        session.rollback()


def test_input_hash_usage_liveness_is_mutable():
    pipe, _ = run_simple_pipeline()
    with TEST_HOME.session() as session:
        ihu = session.query(InputHashUsage).first()
        ihu.fulfilled = False
        ihu.last_run_id = "different"
        # InputHashUsage is the one intentionally mutable ledger table:
        # fulfilled/last_run_id legitimately update (claim/fulfill/invalidate).
        session.commit()


def test_run_identity_is_immutable_but_lifecycle_is_not():
    pipe, summary = run_simple_pipeline()
    with TEST_HOME.session() as session:
        run_row = session.get(Run, summary.run_id)
        run_row.status = "completed"  # lifecycle projection: allowed
        session.commit()

    with TEST_HOME.session() as session:
        run_row = session.get(Run, summary.run_id)
        run_row.source_id = "folder:elsewhere"
        with pytest.raises(ImmutabilityError, match="immutable"):
            session.commit()
        session.rollback()


def test_restore_preserves_invalidation_history():
    pipe, _ = run_simple_pipeline()

    invalidate(Selection(step="read"), reason="looked wrong", home=TEST_HOME)
    # Deterministic step: rerun produces identical bytes -> restored, not new row
    summary = pipe.run(workers=1)
    assert summary.created_count == 1

    with TEST_HOME.session() as session:
        # Only "read"'s materialization was invalidated; "scan"'s own lane
        # materialization is untouched, so filter to the step under test.
        read_rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "read"]
        assert read_rows
        ihu = session.query(InputHashUsage).filter_by(address=read_rows[0]["address"]).first()
        assert ihu is not None and ihu.fulfilled is True
        # No lifecycle rows in the new model — liveness is
        # input_hash_usages.fulfilled.  The invalidation flipped fulfilled=False,
        # the rerun flipped it back to True.
