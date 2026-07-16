"""skip_cache: inline utils fused into their consumers' cache identity."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import (
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


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def build_pipeline(calls, util_version="1"):
    """read (materialized) -> parse (skip_cache util) -> report (materialized)."""

    @step
    def read(scan):
        return scan["text"]

    @step(version=util_version, skip_cache=True)
    def parse(read):
        calls.append("parse")
        return read.strip().lower()

    @step
    def report(parse):
        return f"report: {parse}"

    return pipeline(
        name="sc", steps=[scan, read, parse, report]
    )


def test_util_never_materialized_or_recorded():
    calls = []
    create_file("f1.txt", "  HELLO  ")
    pipe = build_pipeline(calls)

    summary = pipe.run(workers=1)
    assert summary.created_count == 3  # scan + read + report; parse invisible
    assert calls == ["parse"]

    with get_session() as session:
        from rubedo import lane_store
        from rubedo.planning import _ArrowRowRef
        from rubedo.store import read_materialization_output

        step_names = {r.get("step_name") for r in lane_store.all_filled_rows()}
        assert step_names == {"scan", "read", "report"}
        rc_steps = {c.step_name for c in session.query(RunCoordinateStatus).all()}
        assert rc_steps == {"scan", "read", "report"}

        # Value flowed through the util correctly
        lane_store.address_row_index()
        report_row = next(r for r in lane_store.all_filled_rows() if r.get("step_name") == "report")
        assert read_materialization_output(_ArrowRowRef(report_row)) == "report: hello"

        # Lineage skips through: report's parent is read (not scan, and not
        # the fused-away parse util)
        read_row = next(r for r in lane_store.all_filled_rows() if r.get("step_name") == "read")
        edge = session.query(MaterializationEdge).filter_by(
            parent_address=read_row.get("address"), child_address=report_row.get("address")
        ).one()
        assert (edge.parent_address, edge.child_address) == (read_row.get("address"), report_row.get("address"))


def test_fully_cached_run_skips_util_entirely():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    pipe.run(workers=1)
    assert calls == ["parse"]

    summary = pipe.run(workers=1)
    assert summary.reused_count == 3
    assert calls == ["parse"], "cached run must not execute the util at all"


def test_util_identity_change_recomputes_consumer():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)
    pipe.run(workers=1)

    calls2 = []
    pipe = build_pipeline(calls2, util_version="2")
    summary = pipe.run(workers=1)
    # scan + read reused; report recomputed because the util's identity is
    # in its key
    assert (summary.created_count, summary.reused_count) == (1, 2)
    assert calls2 == ["parse"]


def test_util_shared_by_two_consumers_runs_once():
    calls = []
    create_file("f1.txt", "hello")

    @step
    def read(scan):
        return scan["text"]

    @step(skip_cache=True)
    def norm(read):
        calls.append("norm")
        return read.strip()

    @step
    def upper(norm):
        return norm.upper()

    @step
    def length(norm):
        return {"len": len(norm)}

    pipe = pipeline(
name="fan", steps=[scan, read, norm, upper, length])
    summary = pipe.run(workers=2)
    assert summary.failed_count == 0
    assert calls == ["norm"], "memoized per run despite two consumers"


def test_util_failure_fails_the_consumer():
    create_file("f1.txt", "hello")

    @step
    def read(scan):
        return scan["text"]

    @step(skip_cache=True)
    def boom(read):
        raise RuntimeError("util exploded")

    @step
    def use(boom):
        return boom

    pipe = pipeline(
name="fail", steps=[scan, read, boom, use])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "failed"
        assert "util exploded" in rc.error_message


def test_blocked_propagates_through_util():
    create_file("f1.txt", "hello")

    @step
    def read(scan):
        raise ValueError("root fails")

    @step(skip_cache=True)
    def mid(read):
        return read

    @step
    def use(mid):
        return mid

    pipe = pipeline(
name="blk", steps=[scan, read, mid, use])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert rc.status == "blocked"


def test_chained_utils():
    create_file("f1.txt", "  HELLO  ")

    @step
    def read(scan):
        return scan["text"]

    @step(skip_cache=True)
    def strip(read):
        return read.strip()

    @step(skip_cache=True)
    def lower(strip):
        return strip.lower()

    @step
    def out(lower):
        return lower

    pipe = pipeline(
name="chain", steps=[scan, read, strip, lower, out])
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    from rubedo.store import read_materialization_output

    with get_session():
        from rubedo import lane_store
        from rubedo.planning import _ArrowRowRef

        out_row = next(r for r in lane_store.all_filled_rows() if r.get("step_name") == "out")
        assert read_materialization_output(_ArrowRowRef(out_row)) == "hello"


def test_plan_omits_utils():
    calls = []
    create_file("f1.txt", "hello")
    pipe = build_pipeline(calls)

    p = pipe.plan()
    step_names = {i.step_name for i in p.items}
    assert step_names == {"scan", "read", "report"}
    assert calls == [], "planning must not execute the util"


def test_registration_validations():
    with pytest.raises(ValueError, match="stale_after is meaningless"):

        @step(skip_cache=True, stale_after="1h")
        def x(path):
            pass

    @step(skip_cache=True)
    def orphan():
        pass

    # skip_cache-has-no-consumer validation runs lazily on first `.spec`
    # access.
    with pytest.raises(ValueError, match="no consumer"):
        pipeline(name="bad", steps=[orphan]).spec
