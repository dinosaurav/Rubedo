"""skip_cache: inline utils fused into their consumers' cache identity."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import plan, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import (
    Materialization,
    MaterializationEdge,
    RunCoordinateStatus,
)
from rubedo.store import init_store

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
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content — the
    replacement for the old folder=TEST_FOLDER source sugar (TODO 14)."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def build_pipeline(calls, util_version="1"):
    """read (materialized) -> parse (skip_cache util) -> report (materialized)."""

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"]

    @step(name="parse", version=util_version, depends_on=["read"], skip_cache=True)
    def parse(read):
        calls.append("parse")
        return read.strip().lower()

    @step(name="report", version="1", depends_on=["parse"])
    def report(parse):
        return f"report: {parse}"

    return pipeline(
        id="sc", name="sc", steps=[scan, read, parse, report]
    )


def test_util_never_materialized_or_recorded():
    calls = []
    create_file("f1.txt", "  HELLO  ")
    pipe = build_pipeline(calls)

    summary = run(pipe, workers=1)
    assert summary.created_count == 3  # scan + read + report; parse invisible
    assert calls == ["parse"]

    with get_session() as session:
        step_names = {m.step_name for m in session.query(Materialization).all()}
        assert step_names == {"scan", "read", "report"}
        rc_steps = {c.step_name for c in session.query(RunCoordinateStatus).all()}
        assert rc_steps == {"scan", "read", "report"}

        # Value flowed through the util correctly
        report_mat = session.query(Materialization).filter_by(step_name="report").one()
        from rubedo.store import read_materialization_output

        assert read_materialization_output(report_mat) == "report: hello"

        # Lineage skips through: report's parent is read (not scan, and not
        # the fused-away parse util)
        read_mat = session.query(Materialization).filter_by(step_name="read").one()
        edge = session.query(MaterializationEdge).filter_by(
            parent_id=read_mat.id, child_id=report_mat.id
        ).one()
        assert (edge.parent_id, edge.child_id) == (read_mat.id, report_mat.id)


def test_fully_cached_run_skips_util_entirely():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    run(pipe, workers=1)
    assert calls == ["parse"]

    summary = run(pipe, workers=1)
    assert summary.reused_count == 3
    assert calls == ["parse"], "cached run must not execute the util at all"


def test_util_identity_change_recomputes_consumer():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)
    run(pipe, workers=1)

    calls2 = []
    pipe = build_pipeline(calls2, util_version="2")
    summary = run(pipe, workers=1)
    # scan + read reused; report recomputed because the util's identity is
    # in its key
    assert (summary.created_count, summary.reused_count) == (1, 2)
    assert calls2 == ["parse"]


def test_util_shared_by_two_consumers_runs_once():
    calls = []
    create_file("f1.txt", "hello")

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"]

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
id="fan", name="fan", steps=[scan, read, norm, upper, length])
    summary = run(pipe, workers=2)
    assert summary.failed_count == 0
    assert calls == ["norm"], "memoized per run despite two consumers"


def test_util_failure_fails_the_consumer():
    create_file("f1.txt", "hello")

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"]

    @step(name="boom", version="1", depends_on=["read"], skip_cache=True)
    def boom(read):
        raise RuntimeError("util exploded")

    @step(name="use", version="1", depends_on=["boom"])
    def use(boom):
        return boom

    pipe = pipeline(
id="fail", name="fail", steps=[scan, read, boom, use])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "failed"
        assert "util exploded" in rc.error_message


def test_blocked_propagates_through_util():
    create_file("f1.txt", "hello")

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        raise ValueError("root fails")

    @step(name="mid", version="1", depends_on=["read"], skip_cache=True)
    def mid(read):
        return read

    @step(name="use", version="1", depends_on=["mid"])
    def use(mid):
        return mid

    pipe = pipeline(
id="blk", name="blk", steps=[scan, read, mid, use])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "blocked"


def test_chained_utils():
    create_file("f1.txt", "  HELLO  ")

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"]

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
id="chain", name="chain", steps=[scan, read, strip, lower, out])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 0

    from rubedo.store import read_materialization_output

    with get_session() as session:
        mat = session.query(Materialization).filter_by(step_name="out").one()
        assert read_materialization_output(mat) == "hello"


def test_plan_omits_utils():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    p = plan(pipe)
    step_names = {i.step_name for i in p.items}
    assert step_names == {"scan", "read", "report"}
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
        pipeline(id="bad", name="bad", steps=[orphan])
