"""skip_cache: inline utils fused into their consumers' cache identity."""

import os

import pytest

from conftest import isolated_test_env
from rubedo import pipeline, step
from rubedo.models import MaterializationEdge, RunCoordinateStatus

TEST_FOLDER = ".test_skipcache_data"
ENV_FOLDER = ".test_skipcache_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("skipcache") as env:
        TEST_HOME = env.home
        yield

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

    return pipeline(name="sc", steps=[scan, read, parse, report], home=TEST_HOME)


def test_util_never_materialized_or_recorded():
    calls = []
    create_file("f1.txt", "  HELLO  ")
    pipe = build_pipeline(calls)

    summary = pipe.run(workers=1)
    assert summary.created_count == 3  # scan + read + report; parse invisible
    assert calls == ["parse"]

    with TEST_HOME.session() as session:
        step_names = {r.get("step_name") for r in TEST_HOME.lanes.all_filled_rows()}
        assert step_names == {"scan", "read", "report"}
        rc_steps = {c.step_name for c in session.query(RunCoordinateStatus).all()}
        assert rc_steps == {"scan", "read", "report"}

        # Value flowed through the util correctly
        (report_cell,) = summary.cells("report", resolve_output=True)
        report_row = next(r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "report")
        assert report_cell.output == "report: hello"

        # Lineage skips through: report's parent is read (not scan, and not
        # the fused-away parse util)
        read_row = next(r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "read")
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

    pipe = pipeline(name="fan", steps=[scan, read, norm, upper, length], home=TEST_HOME)
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

    pipe = pipeline(name="fail", steps=[scan, read, boom, use], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
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

    pipe = pipeline(name="blk", steps=[scan, read, mid, use], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
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

    pipe = pipeline(name="chain", steps=[scan, read, strip, lower, out], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    (out_cell,) = summary.cells("out", resolve_output=True)
    assert out_cell.output == "hello"


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
        pipeline(name="bad", steps=[orphan], home=TEST_HOME).spec
