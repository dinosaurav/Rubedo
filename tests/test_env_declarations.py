"""pipeline(secrets=, env=) — TODO 21.

Fixture shape copied from tests/test_index.py: per-test .test_envdecl_data
(scanned) and .test_envdecl_env (object store) dirs, never nested; an
in-memory shared-cache SQLite with StaticPool.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import pipeline, step
from rubedo.spec import definition

TEST_FOLDER = ".test_envdecl_data"
ENV_FOLDER = ".test_envdecl_env"


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
    from rubedo.db import init_db

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

    from rubedo.store import init_store

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step
def scan():
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


@step
def extract(scan: dict):
    return {"text": scan["text"]}


def test_valid_declarations_construct_and_definition_includes_both_lists():
    p = pipeline(
        name="envdecl-valid",
        steps=[scan, extract],
        secrets=["OPENAI_API_KEY"],
        env=["LOG_LEVEL"],
    )
    snap = definition(p.spec)
    assert snap["secrets"] == ["OPENAI_API_KEY"]
    assert snap["env"] == ["LOG_LEVEL"]


def test_definition_emits_empty_lists_when_undeclared():
    p = pipeline(name="envdecl-empty", steps=[scan, extract])
    snap = definition(p.spec)
    assert snap["secrets"] == []
    assert snap["env"] == []


def test_overlap_between_secrets_and_env_raises():
    with pytest.raises(ValueError, match="unique"):
        pipeline(
            name="envdecl-overlap",
            steps=[scan, extract],
            secrets=["API_KEY"],
            env=["API_KEY"],
        )


def test_duplicate_within_one_list_raises():
    with pytest.raises(ValueError, match="unique"):
        pipeline(
            name="envdecl-dupe",
            steps=[scan, extract],
            secrets=["API_KEY", "API_KEY"],
        )


def test_reserved_rubedo_prefixed_name_raises():
    with pytest.raises(ValueError, match="reserved"):
        pipeline(
            name="envdecl-reserved",
            steps=[scan, extract],
            env=["RUBEDO_HOME"],
        )


def test_empty_name_raises():
    with pytest.raises(ValueError, match="non-empty"):
        pipeline(name="envdecl-emptyname", steps=[scan, extract], secrets=[""])


def test_run_reuse_is_identical_with_and_without_declarations():
    """The acceptance line: a run's reuse behavior is identical with and
    without secrets=/env= — the declarations must never enter per-step
    cache identity."""
    create_file("a.txt", "hello")

    plain = pipeline(name="envdecl-reuse", steps=[scan, extract])
    summary1 = plain.run(workers=1)
    assert summary1.created_count > 0
    assert summary1.failed_count == 0

    # New Pipeline object (same name), only difference: secrets=/env=.
    declared = pipeline(
        name="envdecl-reuse",
        steps=[scan, extract],
        secrets=["OPENAI_API_KEY"],
        env=["LOG_LEVEL"],
    )
    summary2 = declared.run(workers=1)

    # Fully reused: nothing recomputed just because the declarations appeared.
    assert summary2.created_count == 0
    assert summary2.reused_count == summary1.created_count
