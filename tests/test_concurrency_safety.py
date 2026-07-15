import os
import shutil
import pytest
from unittest.mock import patch

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
import rubedo.store as store

TEST_FOLDER = ".test_concurrency_data"
ENV_FOLDER = ".test_concurrency_env"

@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    # Must use a physical file so other sessions can see it.
    os.environ["RUBEDO_DB_PATH"] = f"sqlite:///{abs_env_folder}/rubedo.sqlite"
    init_db()

    with open(os.path.join(abs_test_folder, "a.txt"), "w") as f:
        f.write("A")

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


# A headless map root fed one file's content via params — content, not
# path, is what these tests race on, and
# a param-fed root gives a single, plan()-visible "@root" lane whose
# output_address is knowable up front (unlike an expand root's children).
@step
def my_step(params):
    return params["content"].lower()


def _root_output_address(pipe, params):
    """The address my_step's single @root lane will commit to — computed by
    plan() (a pure dry-run) so the race-injection below can target it
    without reaching into planning internals."""
    p = pipe.plan(params=params)
    (item,) = p.items
    return item.output_address


def test_concurrency_identical_bytes_collision():
    # We patch stage_and_commit to first insert a competing materialization directly into the DB.
    # This simulates another worker completing the same task right before we commit.
    original_stage_and_commit = store.stage_and_commit

    pipe = pipeline(name="p1", steps=[my_step])
    params = {"content": "A"}
    output_address = _root_output_address(pipe, params)

    has_injected = False

    def mock_stage_and_commit(run_id, coordinate, result):
        nonlocal has_injected
        final_path, output_content_hash, content_type = original_stage_and_commit(run_id, coordinate, result)

        if not has_injected:
            has_injected = True
            # Simulate a competing run inserting a live materialization
            with get_session() as session:
                from rubedo.ledger import _commit_materialization

                # Insert the identical materialization at the address our
                # own run is about to commit to.
                _commit_materialization(
                    session,
                    pipeline_id="p1",
                    step=my_step,
                    input_hash="",
                    output_address=output_address,
                    output_content_hash=output_content_hash,
                    content_type=content_type,
                    output_path=final_path,
                    metadata_json=None,
                    run_id="run_concurrent",
                )
                session.commit()

        return final_path, output_content_hash, content_type

    with patch("rubedo.ledger.stage_and_commit", side_effect=mock_stage_and_commit):
        summary = pipe.run(params=params)

    if summary.failed_count > 0:
        with get_session() as session:
            from rubedo.models import RunCoordinateStatus
            for s in session.query(RunCoordinateStatus).filter_by(status='failed').all():
                print(f"FAILED TRACE: {s.error_message}")

    # Since bytes were identical, the original run should mark it as "reused"!
    assert summary.reused_count == 1
    assert summary.created_count == 0

def test_concurrency_different_bytes_collision():
    # Same as above, but the competing run inserted DIFFERENT bytes for the same address.
    # This means the current run must supersede it.
    original_stage_and_commit = store.stage_and_commit

    pipe = pipeline(name="p2", steps=[my_step])
    params = {"content": "A"}
    output_address = _root_output_address(pipe, params)

    has_injected = False

    def mock_stage_and_commit(run_id, coordinate, result):
        nonlocal has_injected
        final_path, output_content_hash, content_type = original_stage_and_commit(run_id, coordinate, result)

        if not has_injected:
            has_injected = True
            with get_session() as session:
                from rubedo.ledger import _commit_materialization

                # Insert a competing materialization with DIFFERENT content hash
                _commit_materialization(
                    session,
                    pipeline_id="p2",
                    step=my_step,
                    input_hash="",
                    output_address=output_address,
                    output_content_hash="mocked_different_hash",
                    content_type=content_type,
                    output_path="mocked_different_path",
                    metadata_json=None,
                    run_id="run_concurrent",
                )
                session.commit()

        return final_path, output_content_hash, content_type

    with patch("rubedo.ledger.stage_and_commit", side_effect=mock_stage_and_commit):
        summary = pipe.run(params=params)

    if summary.failed_count > 0:
        with get_session() as session:
            from rubedo.models import RunCoordinateStatus
            for s in session.query(RunCoordinateStatus).filter_by(status='failed').all():
                print(f"FAILED TRACE: {s.error_message}")

    # The current run supersedes the injected one, which counts as "created" (since it made a new live row).
    assert summary.created_count == 1
    assert summary.reused_count == 0
    assert summary.failed_count == 0

def test_sqlite_pragmas():
    from sqlalchemy import text
    with get_session() as session:
        result = session.execute(text("PRAGMA journal_mode")).scalar()
        assert result.upper() == "WAL"

        result2 = session.execute(text("PRAGMA busy_timeout")).scalar()
        assert str(result2) == "5000"
