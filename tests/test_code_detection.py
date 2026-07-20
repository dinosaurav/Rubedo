"""Code-change detection: version='auto' and drift warnings on manual versions."""

import os

import pytest

from rubedo import step, pipeline
from rubedo.models import RunEvent
from conftest import isolated_test_env

TEST_FOLDER = ".test_code_data"
ENV_FOLDER = ".test_code_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("code") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


# Module-level step bodies so tests can register "the same step, edited". A
# single file, fed by path via params to a headless map root — no
# folder-scanning multiplicity is needed here, just one lane whose identity
# tracks code/version, so a param-fed root keeps plan()'s per-lane
# visibility (an expand root's downstream lanes are opaque to plan(), see
# test_plan.py).


def body_v1(params):
    return open(params["path"]).read().strip()


def body_v2(params):
    return open(params["path"]).read().strip().upper()  # the "edit"


def register(fn, version, code="warn"):
    spec = step(name="work", version=version, code=code)(fn)
    pipe = pipeline(name="cd", steps=[spec], home=TEST_HOME)
    return pipe, spec


def test_code_auto_recomputes_on_code_change():
    path = create_file("f1.txt", "hello")
    p = {"path": path}

    pipe, _ = register(body_v1, "1.0.0", code="auto")
    s1 = pipe.run(params=p, workers=1)
    assert s1.created_count == 1

    # Same code: cache hit
    pipe, _ = register(body_v1, "1.0.0", code="auto")
    s2 = pipe.run(params=p, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 1)

    # Edited code: identity changed, recompute without any version bump
    pipe, _ = register(body_v2, "1.0.0", code="auto")
    s3 = pipe.run(params=p, workers=1)
    assert (s3.created_count, s3.reused_count) == (1, 0)

    # code='auto' never drift-warns: identity already tracks the source
    import warnings

    pipe, _ = register(body_v2, "1.0.0", code="auto")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        s4 = pipe.run(params=p, workers=1)
    assert s4.reused_count == 1


def test_version_and_code_are_independent_axes():
    path = create_file("f1.txt", "hello")
    p = {"path": path}

    pipe, _ = register(body_v1, "1.0.0", code="auto")
    pipe.run(params=p, workers=1)

    # Same code, bumped version: version alone changes identity
    pipe, _ = register(body_v1, "2.0.0", code="auto")
    s = pipe.run(params=p, workers=1)
    assert (s.created_count, s.reused_count) == (1, 0)


def test_version_auto_is_rejected():
    with pytest.raises(ValueError, match="code='auto'"):
        step(name="work", version="auto")(body_v1)


def test_manual_version_warns_on_drift_but_reuses():
    path = create_file("f1.txt", "hello")
    p = {"path": path}

    pipe, _ = register(body_v1, "v1")
    pipe.run(params=p, workers=1)

    # Code edited, version not bumped: reuse stands, but loudly
    pipe, _ = register(body_v2, "v1")
    with pytest.warns(UserWarning, match="source code changed"):
        summary = pipe.run(params=p, workers=1)
    assert (summary.created_count, summary.reused_count) == (0, 1)

    with TEST_HOME.session() as session:
        drift_events = (
            session.query(RunEvent).filter_by(event_type="code_drift_detected").all()
        )
        assert len(drift_events) == 1
        assert drift_events[0].level == "warning"
        assert "Bump the version" in drift_events[0].message


def test_no_warning_when_code_unchanged():
    import warnings

    path = create_file("f1.txt", "hello")
    p = {"path": path}
    pipe, _ = register(body_v1, "v1")
    pipe.run(params=p, workers=1)

    pipe, _ = register(body_v1, "v1")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        summary = pipe.run(params=p, workers=1)
    assert summary.reused_count == 1


def test_plan_surfaces_drift_warning():
    path = create_file("f1.txt", "hello")
    params = {"path": path}
    pipe, _ = register(body_v1, "v1")
    pipe.run(params=params, workers=1)

    pipe, _ = register(body_v2, "v1")
    p = pipe.plan(params=params)
    assert len(p.warnings) == 1
    assert "source code changed" in p.warnings[0]
    assert p.counts == {"reuse": 1}


def test_code_hash_recorded_on_materialization():
    path = create_file("f1.txt", "hello")
    pipe, spec = register(body_v1, "v1")
    pipe.run(params={"path": path}, workers=1)

    with TEST_HOME.session():
        rows = TEST_HOME.lanes.all_filled_rows()
        assert len(rows) == 1
        row = rows[0]
        assert row.get("code_hash") == spec.code_hash
        assert row.get("code_hash") is not None
