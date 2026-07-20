import os
import shutil
import pytest
from unittest.mock import patch

from rubedo import step, pipeline
from rubedo.models import InputHashUsage
from conftest import make_home

TEST_FOLDER = ".test_concurrency_data"
ENV_FOLDER = ".test_concurrency_env"

TEST_HOME = None

@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    # Must use a physical file so other sessions can see it.

    with open(os.path.join(abs_test_folder, "a.txt"), "w") as f:
        f.write("A")

    TEST_HOME = make_home(ENV_FOLDER)
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


def _inject_competing(pipeline_id, output_address, output_string, content_type):
    """Simulate another worker completing the same address before our commit.

    Writes an Arrow row and flips IHU ``fulfilled=True`` — the two artifacts
    the ledger's mat_action check looks at.  The Arrow row is flushed to disk
    so ``address_row_index`` (which scans disk files) can see it."""
    from rubedo.ledger import _identity_of
    TEST_HOME.lanes.append_filled(
        pipeline_id=pipeline_id,
        step_name="my_step",
        lane_key="@root",
        address=output_address,
        input_hash="dummy_injected",
        output=output_string,
        content_type=content_type,
        run_id="run_concurrent",
        code_hash="dummy",
        code_version="1",
        output_identity=_identity_of(output_string),
    )
    TEST_HOME.lanes.flush_step(pipeline_id, "my_step")
    with TEST_HOME.session() as session:
        existing = session.query(InputHashUsage).filter_by(address=output_address).first()
        if existing:
            existing.fulfilled = True
            existing.last_run_id = "run_concurrent"
        else:
            session.add(InputHashUsage(
                address=output_address,
                last_run_id="run_concurrent",
                fulfilled=True,
            ))
        session.commit()


def test_concurrency_identical_bytes_collision():
    # Another worker commits identical bytes for the same address before
    # our commit — our run should detect "reused" (same output string, was
    # already fulfilled).
    original_serialize = TEST_HOME.store.serialize_output

    pipe = pipeline(name="p1", steps=[my_step], home=TEST_HOME)
    params = {"content": "A"}
    output_address = _root_output_address(pipe, params)

    has_injected = False

    def mock_serialize(run_id, coordinate, result):
        nonlocal has_injected
        output_string, content_type = original_serialize(run_id, coordinate, result)

        if not has_injected:
            has_injected = True
            _inject_competing("p1", output_address, output_string, content_type)

        return output_string, content_type

    with patch.object(TEST_HOME.store, "serialize_output", side_effect=mock_serialize):
        summary = pipe.run(params=params)

    assert summary.reused_count == 1
    assert summary.created_count == 0
    assert summary.failed_count == 0


def test_concurrency_different_bytes_collision():
    # Another worker commits DIFFERENTENT bytes for the same address before
    # our commit — our run must supersede (counts as "created").
    original_serialize = TEST_HOME.store.serialize_output

    pipe = pipeline(name="p2", steps=[my_step], home=TEST_HOME)
    params = {"content": "A"}
    output_address = _root_output_address(pipe, params)

    has_injected = False

    def mock_serialize(run_id, coordinate, result):
        nonlocal has_injected
        output_string, content_type = original_serialize(run_id, coordinate, result)

        if not has_injected:
            has_injected = True
            _inject_competing("p2", output_address, "objects:mocked_different_hash", content_type)

        return output_string, content_type

    with patch.object(TEST_HOME.store, "serialize_output", side_effect=mock_serialize):
        summary = pipe.run(params=params)

    assert summary.created_count == 1
    assert summary.reused_count == 0
    assert summary.failed_count == 0

def test_sqlite_pragmas():
    from sqlalchemy import text
    with TEST_HOME.session() as session:
        result = session.execute(text("PRAGMA journal_mode")).scalar()
        assert result.upper() in {"WAL", "MEMORY"}

        result2 = session.execute(text("PRAGMA busy_timeout")).scalar()
        assert str(result2) == "5000"
