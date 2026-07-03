"""skip_cache: inline utils fused into their consumers' cache identity."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from batchbrain import plan, run, step, pipeline
from batchbrain.db import init_db, get_session
from batchbrain.models import (
    Materialization,
    MaterializationEdge,
    RunCoordinateStatus,
)
from batchbrain.store import init_store

TEST_FOLDER = ".test_skipcache_data"
ENV_FOLDER = ".test_skipcache_env"


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


def build_pipeline(calls, util_version="1"):
    """read (materialized) -> parse (skip_cache util) -> report (materialized)."""

    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    @step(name="parse", version=util_version, depends_on=["read"], skip_cache=True)
    def parse(read):
        calls.append("parse")
        return read.strip().lower()

    @step(name="report", version="1", depends_on=["parse"])
    def report(parse):
        return f"report: {parse}"

    return pipeline(
        id="sc", name="sc", folder=TEST_FOLDER, steps=[read, parse, report]
    )


def test_util_never_materialized_or_recorded():
    calls = []
    create_file("f1.txt", "  HELLO  ")
    pipe = build_pipeline(calls)

    summary = run(pipe, workers=1)
    assert summary.created_count == 2  # read + report; parse invisible
    assert calls == ["parse"]

    with get_session() as session:
        step_names = {m.step_name for m in session.query(Materialization).all()}
        assert step_names == {"read", "report"}
        rc_steps = {c.step_name for c in session.query(RunCoordinateStatus).all()}
        assert rc_steps == {"read", "report"}

        # Value flowed through the util correctly
        report_mat = session.query(Materialization).filter_by(step_name="report").one()
        from batchbrain.store import read_materialization_output

        assert read_materialization_output(report_mat) == "report: hello"

        # Lineage skips through: report's parent is read
        read_mat = session.query(Materialization).filter_by(step_name="read").one()
        edge = session.query(MaterializationEdge).one()
        assert (edge.parent_id, edge.child_id) == (read_mat.id, report_mat.id)


def test_fully_cached_run_skips_util_entirely():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    run(pipe, workers=1)
    assert calls == ["parse"]

    summary = run(pipe, workers=1)
    assert summary.reused_count == 2
    assert calls == ["parse"], "cached run must not execute the util at all"


def test_util_identity_change_recomputes_consumer():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)
    run(pipe, workers=1)

    calls2 = []
    pipe = build_pipeline(calls2, util_version="2")
    summary = run(pipe, workers=1)
    # read reused; report recomputed because the util's identity is in its key
    assert (summary.created_count, summary.reused_count) == (1, 1)
    assert calls2 == ["parse"]


def test_util_shared_by_two_consumers_runs_once():
    calls = []
    create_file("f1.txt", "hello")

    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    @step(name="norm", version="1", depends_on=["read"], skip_cache=True)
    def norm(read):
        calls.append("norm")
        return read.strip()

    @step(name="upper", version="1", depends_on=["norm"])
    def upper(norm):
        return norm.upper()

    @step(name="length", version="1", depends_on=["norm"])
    def length(norm):
        return {"len": len(norm)}

    pipe = pipeline(
id="fan", name="fan", folder=TEST_FOLDER, steps=[read, norm, upper, length])
    summary = run(pipe, workers=2)
    assert summary.failed_count == 0
    assert calls == ["norm"], "memoized per run despite two consumers"


def test_util_failure_fails_the_consumer():
    create_file("f1.txt", "hello")

    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    @step(name="boom", version="1", depends_on=["read"], skip_cache=True)
    def boom(read):
        raise RuntimeError("util exploded")

    @step(name="use", version="1", depends_on=["boom"])
    def use(boom):
        return boom

    pipe = pipeline(
id="fail", name="fail", folder=TEST_FOLDER, steps=[read, boom, use])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "failed"
        assert "util exploded" in rc.error_message


def test_blocked_propagates_through_util():
    create_file("f1.txt", "hello")

    @step(name="read", version="1")
    def read(path):
        raise ValueError("root fails")

    @step(name="mid", version="1", depends_on=["read"], skip_cache=True)
    def mid(read):
        return read

    @step(name="use", version="1", depends_on=["mid"])
    def use(mid):
        return mid

    pipe = pipeline(
id="blk", name="blk", folder=TEST_FOLDER, steps=[read, mid, use])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "blocked"


def test_chained_utils():
    create_file("f1.txt", "  HELLO  ")

    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    @step(name="strip", version="1", depends_on=["read"], skip_cache=True)
    def strip(read):
        return read.strip()

    @step(name="lower", version="1", depends_on=["strip"], skip_cache=True)
    def lower(strip):
        return strip.lower()

    @step(name="out", version="1", depends_on=["lower"])
    def out(lower):
        return lower

    pipe = pipeline(
id="chain", name="chain", folder=TEST_FOLDER, steps=[read, strip, lower, out])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 0

    from batchbrain.store import read_materialization_output

    with get_session() as session:
        mat = session.query(Materialization).filter_by(step_name="out").one()
        assert read_materialization_output(mat) == "hello"


def test_plan_omits_utils():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    p = plan(pipe)
    step_names = {i.step_name for i in p.items}
    assert step_names == {"read", "report"}
    assert calls == [], "planning must not execute the util"


def test_registration_validations():
    with pytest.raises(ValueError, match="stale_after is meaningless"):

        @step(name="x", version="1", skip_cache=True, stale_after="1h")
        def x(path):
            pass

    @step(name="orphan", version="1", skip_cache=True)
    def orphan(path):
        pass

    with pytest.raises(ValueError, match="no consumer"):
        pipe = pipeline(
id="bad", name="bad", folder=TEST_FOLDER, steps=[orphan])
