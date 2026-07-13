"""TODO 16: @step auto-names from the function and defaults version to "0".

Bare `@step` and called `@step()` both mint the same StepSpec; duplicate
step names (whether explicit or defaulted from the function) still die
loudly at pipeline-construction time, naming both definitions.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import RunEvent
from rubedo.store import init_store

TEST_FOLDER = ".test_ergonomics_data"
ENV_FOLDER = ".test_ergonomics_env"


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
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


# ---------- name/version defaults ----------


def test_bare_step_defaults_name_and_version():
    @step
    def parse(row: dict):
        return row

    assert parse.name == "parse"
    assert parse.version == "0"
    assert parse.code_mode == "warn"


def test_called_step_defaults_name_and_version():
    @step()
    def parse(row: dict):
        return row

    assert parse.name == "parse"
    assert parse.version == "0"
    assert parse.code_mode == "warn"


def test_explicit_name_overrides_function_name():
    @step(name="explicit_name")
    def parse(row: dict):
        return row

    assert parse.name == "explicit_name"


def test_explicit_version_still_works_alongside_default_name():
    @step(version="2.0.0")
    def parse(row: dict):
        return row

    assert parse.name == "parse"
    assert parse.version == "2.0.0"


def test_version_auto_still_rejected_with_default_name():
    with pytest.raises(ValueError, match="code='auto'"):

        @step(version="auto")
        def parse(row: dict):
            return row


# ---------- duplicate names ----------


def _make_parse_from_a():
    @step()
    def parse(row: dict):
        return row

    return parse


def _make_parse_from_b():
    @step()
    def parse(row: dict):
        return {"doubled": row}

    return parse


def test_duplicate_auto_names_error_naming_both_definitions():
    a = _make_parse_from_a()
    b = _make_parse_from_b()

    p = pipeline(name="dup-auto", steps=[a, b])
    with pytest.raises(ValueError) as exc_info:
        p.definition()

    message = str(exc_info.value)
    assert "parse" in message
    assert "_make_parse_from_a" in message
    assert "_make_parse_from_b" in message


def test_duplicate_explicit_names_still_error():
    @step(name="dup")
    def one(row: dict):
        return row

    @step(name="dup")
    def two(row: dict):
        return row

    p = pipeline(name="dup-explicit", steps=[one, two])
    with pytest.raises(ValueError, match="Duplicate step name 'dup'"):
        p.definition()


def test_no_duplicate_error_for_distinct_names():
    a = _make_parse_from_a()

    @step()
    def other(row: dict):
        return row

    p = pipeline(name="no-dup", steps=[a, other])
    # Doesn't raise: no duplicate, though this isn't a runnable DAG (no
    # depends_on relating them) — definition() doesn't require that.
    definition = p.definition()
    names = {s["name"] for s in definition["steps"]}
    assert names == {"parse", "other"}


# ---------- default version participates in code-drift warnings ----------


def body_v1(params):
    return open(params["path"]).read().strip()


def body_v2(params):
    return open(params["path"]).read().strip().upper()


def test_default_version_warns_on_code_drift_but_reuses():
    path = create_file("f1.txt", "hello")
    params = {"path": path}

    spec_v1 = step(name="work")(body_v1)  # version defaults to "0"
    assert spec_v1.version == "0"
    pipe = pipeline(name="drift", steps=[spec_v1])
    summary = pipe.run(params=params, workers=1)
    assert summary.created_count == 1

    # Same step name, same (defaulted) version, edited body: code drift,
    # but the default code="warn" reuses instead of recomputing.
    spec_v2 = step(name="work")(body_v2)
    pipe = pipeline(name="drift", steps=[spec_v2])
    with pytest.warns(UserWarning, match="source code changed"):
        summary = pipe.run(params=params, workers=1)
    assert (summary.created_count, summary.reused_count) == (0, 1)

    with get_session() as session:
        drift_events = (
            session.query(RunEvent).filter_by(event_type="code_drift_detected").all()
        )
        assert len(drift_events) == 1
