"""pipeline(secrets=, env=, home=TEST_HOME) — TODO 21.

Fixture shape copied from tests/test_index.py: per-test .test_envdecl_data
(scanned) and .test_envdecl_env (object store) dirs, never nested; an
in-memory shared-cache SQLite with StaticPool.
"""

import os

import pytest

from rubedo import pipeline, step
from rubedo.spec import definition
from conftest import isolated_test_env

TEST_FOLDER = ".test_envdecl_data"
ENV_FOLDER = ".test_envdecl_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("envdecl") as env:
        TEST_HOME = env.home
        yield

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
    
        home=TEST_HOME,
    )
    snap = definition(p.spec)
    assert snap["secrets"] == ["OPENAI_API_KEY"]
    assert snap["env"] == ["LOG_LEVEL"]


def test_definition_emits_empty_lists_when_undeclared():
    p = pipeline(name="envdecl-empty", steps=[scan, extract], home=TEST_HOME)
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
        
            home=TEST_HOME,
        )


def test_duplicate_within_one_list_raises():
    with pytest.raises(ValueError, match="unique"):
        pipeline(
            name="envdecl-dupe",
            steps=[scan, extract],
            secrets=["API_KEY", "API_KEY"],
        
            home=TEST_HOME,
        )


def test_reserved_rubedo_prefixed_name_raises():
    with pytest.raises(ValueError, match="reserved"):
        pipeline(
            name="envdecl-reserved",
            steps=[scan, extract],
            env=["RUBEDO_HOME"],
        
            home=TEST_HOME,
        )


def test_empty_name_raises():
    with pytest.raises(ValueError, match="non-empty"):
        pipeline(name="envdecl-emptyname", steps=[scan, extract], secrets=[""], home=TEST_HOME)


def test_run_reuse_is_identical_with_and_without_declarations():
    """The acceptance line: a run's reuse behavior is identical with and
    without secrets=/env= — the declarations must never enter per-step
    cache identity."""
    create_file("a.txt", "hello")

    plain = pipeline(name="envdecl-reuse", steps=[scan, extract], home=TEST_HOME)
    summary1 = plain.run(workers=1)
    assert summary1.created_count > 0
    assert summary1.failed_count == 0

    # New Pipeline object (same name), only difference: secrets=/env=.
    declared = pipeline(
        name="envdecl-reuse",
        steps=[scan, extract],
        secrets=["OPENAI_API_KEY"],
        env=["LOG_LEVEL"],
    
        home=TEST_HOME,
    )
    summary2 = declared.run(workers=1)

    # Fully reused: nothing recomputed just because the declarations appeared.
    assert summary2.created_count == 0
    assert summary2.reused_count == summary1.created_count
