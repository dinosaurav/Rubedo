"""Code-change detection: version='auto' and drift warnings on manual versions."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from batchbrain import plan, run, step, pipeline
from batchbrain.db import init_db, get_session
from batchbrain.models import Materialization, RunEvent
from batchbrain.store import init_store

TEST_FOLDER = ".test_code_data"
ENV_FOLDER = ".test_code_env"


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


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


# Module-level step bodies so tests can register "the same step, edited".


def body_v1(path):
    return open(path).read().strip()


def body_v2(path):
    return open(path).read().strip().upper()  # the "edit"


def register(fn, version, code="warn"):
    spec = step(name="work", version=version, code=code)(fn)
    pipe = pipeline(id="cd", name="cd", folder=TEST_FOLDER, steps=[spec])
    return pipe, spec


def test_code_auto_recomputes_on_code_change():
    create_file("f1.txt", "hello")

    pipe, _ = register(body_v1, "1.0.0", code="auto")
    s1 = run(pipe, workers=1)
    assert s1.created_count == 1

    # Same code: cache hit
    pipe, _ = register(body_v1, "1.0.0", code="auto")
    s2 = run(pipe, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 1)

    # Edited code: identity changed, recompute without any version bump
    pipe, _ = register(body_v2, "1.0.0", code="auto")
    s3 = run(pipe, workers=1)
    assert (s3.created_count, s3.reused_count) == (1, 0)

    # code='auto' never drift-warns: identity already tracks the source
    import warnings

    pipe, _ = register(body_v2, "1.0.0", code="auto")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        s4 = run(pipe, workers=1)
    assert s4.reused_count == 1


def test_version_and_code_are_independent_axes():
    create_file("f1.txt", "hello")

    pipe, _ = register(body_v1, "1.0.0", code="auto")
    run(pipe, workers=1)

    # Same code, bumped version: version alone changes identity
    pipe, _ = register(body_v1, "2.0.0", code="auto")
    s = run(pipe, workers=1)
    assert (s.created_count, s.reused_count) == (1, 0)


def test_version_auto_is_rejected():
    with pytest.raises(ValueError, match="code='auto'"):
        step(name="work", version="auto")(body_v1)


def test_manual_version_warns_on_drift_but_reuses():
    create_file("f1.txt", "hello")

    pipe, _ = register(body_v1, "v1")
    run(pipe, workers=1)

    # Code edited, version not bumped: reuse stands, but loudly
    pipe, _ = register(body_v2, "v1")
    with pytest.warns(UserWarning, match="source code changed"):
        summary = run(pipe, workers=1)
    assert (summary.created_count, summary.reused_count) == (0, 1)

    with get_session() as session:
        drift_events = (
            session.query(RunEvent).filter_by(event_type="code_drift_detected").all()
        )
        assert len(drift_events) == 1
        assert drift_events[0].level == "warning"
        assert "Bump the version" in drift_events[0].message


def test_no_warning_when_code_unchanged():
    import warnings

    create_file("f1.txt", "hello")
    pipe, _ = register(body_v1, "v1")
    run(pipe, workers=1)

    pipe, _ = register(body_v1, "v1")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        summary = run(pipe, workers=1)
    assert summary.reused_count == 1


def test_plan_surfaces_drift_warning():
    create_file("f1.txt", "hello")
    pipe, _ = register(body_v1, "v1")
    run(pipe, workers=1)

    pipe, _ = register(body_v2, "v1")
    p = plan(pipe)
    assert len(p.warnings) == 1
    assert "source code changed" in p.warnings[0]
    assert p.counts == {"reuse": 1}


def test_code_hash_recorded_on_materialization():
    create_file("f1.txt", "hello")
    pipe, spec = register(body_v1, "v1")
    run(pipe, workers=1)

    with get_session() as session:
        mat = session.query(Materialization).one()
        assert mat.code_hash == spec.code_hash
        assert mat.code_hash is not None
