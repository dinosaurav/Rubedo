"""schedule="broad" | "deep": scheduling changes order, never results.

broad (default) stages step by step — every lane of step N completes
before any lane starts step N+1. deep pipelines each lane through
consecutive 1:1 (map) steps as soon as its own inputs commit; reduce/join
(and, for now, expand and multi-parent maps) remain barriers that
synchronize on all lanes. Ledger rows — statuses, addresses, lifecycle —
must be identical across modes.
"""

import os
import shutil
import threading
import time
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Filtered, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, RunCoordinateStatus
from rubedo.store import init_store, read_materialization_output

TEST_FOLDER = ".test_schedule_data"
ENV_FOLDER = ".test_schedule_env"


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


def _chain_pipe(pid: str):
    """The 3-step map chain used by the equivalence tests."""

    @step(name="s1", version="1")
    def s1(path):
        return open(path).read().strip()

    @step(name="s2", version="1", depends_on=["s1"])
    def s2(s1):
        return s1.upper()

    @step(name="s3", version="1", depends_on=["s2"])
    def s3(s2):
        return s2 + "!"

    return pipeline(id=pid, name=pid, folder=TEST_FOLDER, steps=[s1, s2, s3])


def _status_rows(run_id):
    """The (step, coordinate, address, status) facts a run recorded."""
    with get_session() as session:
        rows = (
            session.query(RunCoordinateStatus).filter_by(run_id=run_id).all()
        )
        return {
            (r.step_name, r.coordinate, r.output_address, r.status) for r in rows
        }


def _mat_hashes():
    with get_session() as session:
        return {
            m.output_content_hash for m in session.query(Materialization).all()
        }


# (a) Mode equivalence: fresh broad vs fresh deep produce identical facts.
def test_mode_equivalence_fresh_stores():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    create_file("c.txt", "gamma")
    pipe = _chain_pipe("equiv")

    home_a = os.path.join(os.path.abspath(ENV_FOLDER), "homeA")
    home_b = os.path.join(os.path.abspath(ENV_FOLDER), "homeB")

    s_broad = run(pipe, home=home_a, schedule="broad")
    facts_broad = _status_rows(s_broad.run_id)  # read before re-pointing at B
    hashes_broad = _mat_hashes()

    s_deep = run(pipe, home=home_b, schedule="deep")
    facts_deep = _status_rows(s_deep.run_id)
    hashes_deep = _mat_hashes()

    assert s_broad.status == s_deep.status == "completed"
    assert (s_broad.created_count, s_deep.created_count) == (9, 9)
    assert facts_broad == facts_deep
    assert hashes_broad == hashes_deep


# (b) Cross-mode reuse: either mode fully reuses the other's store.
def test_broad_then_deep_reuses_everything():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    pipe = _chain_pipe("cross1")
    home = os.path.join(os.path.abspath(ENV_FOLDER), "homeC")

    s1 = run(pipe, home=home, schedule="broad")
    assert (s1.created_count, s1.reused_count) == (6, 0)
    s2 = run(pipe, home=home, schedule="deep")
    assert (s2.created_count, s2.reused_count) == (0, 6)


def test_deep_then_broad_reuses_everything():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    pipe = _chain_pipe("cross2")
    home = os.path.join(os.path.abspath(ENV_FOLDER), "homeD")

    s1 = run(pipe, home=home, schedule="deep")
    assert (s1.created_count, s1.reused_count) == (6, 0)
    s2 = run(pipe, home=home, schedule="broad")
    assert (s2.created_count, s2.reused_count) == (0, 6)


# (c) Deep actually pipelines: step1(B) can only finish if step2(A) runs
# while step1(B) is still in flight. Deterministic — an Event, no sleeps.
# Do NOT run this pipeline under broad: it deadlocks by construction there
# (broad never starts step2 before every step1 lane is done).
def test_deep_pipelines_lanes_across_steps():
    create_file("a.txt", "A")
    create_file("b.txt", "B")
    gate = threading.Event()

    @step(name="s1", version="1")
    def s1(path):
        if os.path.basename(path) == "b.txt":
            if not gate.wait(timeout=30):
                raise RuntimeError(
                    "gate never opened: s2(a) did not run while s1(b) was in flight"
                )
        return os.path.basename(path)

    @step(name="s2", version="1", depends_on=["s1"])
    def s2(s1):
        if s1 == "a.txt":
            gate.set()  # proves s2(A) ran before s1(B) completed
        return s1.upper()

    pipe = pipeline(id="deep_pipe", name="deep_pipe", folder=TEST_FOLDER, steps=[s1, s2])
    summary = run(pipe, schedule="deep")

    assert gate.is_set()
    assert summary.status == "completed"
    assert (summary.created_count, summary.failed_count, summary.blocked_count) == (
        4,
        0,
        0,
    )
    assert all(
        status == "created"
        for (_, _, _, status) in _status_rows(summary.run_id)
    )


# (c) broad counterpart: staging shown via completion timestamps — every
# s1 finishes before any s2 starts.
def test_broad_stages_whole_steps():
    create_file("a.txt", "1")
    create_file("b.txt", "2")
    s1_finished, s2_started = [], []

    @step(name="s1", version="1")
    def s1(path):
        out = open(path).read()
        s1_finished.append(time.monotonic())
        return out

    @step(name="s2", version="1", depends_on=["s1"])
    def s2(s1):
        s2_started.append(time.monotonic())
        return s1 * 2

    pipe = pipeline(id="staged", name="staged", folder=TEST_FOLDER, steps=[s1, s2])
    summary = run(pipe)  # broad is the default

    assert summary.created_count == 4
    assert len(s1_finished) == 2 and len(s2_started) == 2
    assert max(s1_finished) < min(s2_started)


# (d) Failure cascade under deep: a lane failing at step 1 blocks its own
# downstream cells; the sibling lane completes fully.
def test_deep_failure_cascades_to_downstream_cells():
    create_file("a.txt", "good")
    create_file("b.txt", "boom")

    @step(name="s1", version="1")
    def s1(path):
        text = open(path).read()
        if text == "boom":
            raise ValueError("bad lane")
        return text

    @step(name="s2", version="1", depends_on=["s1"])
    def s2(s1):
        return s1.upper()

    @step(name="s3", version="1", depends_on=["s2"])
    def s3(s2):
        return s2 + "!"

    pipe = pipeline(id="cascade", name="cascade", folder=TEST_FOLDER, steps=[s1, s2, s3])
    summary = run(pipe, schedule="deep")

    assert summary.status == "completed_with_failures"
    assert (summary.created_count, summary.failed_count, summary.blocked_count) == (
        3,
        1,
        2,
    )
    by_cell = {
        (s, c): status for (s, c, _, status) in _status_rows(summary.run_id)
    }
    assert by_cell[("s1", "b.txt")] == "failed"
    assert by_cell[("s2", "b.txt")] == "blocked"
    assert by_cell[("s3", "b.txt")] == "blocked"
    assert by_cell[("s1", "a.txt")] == "created"
    assert by_cell[("s2", "a.txt")] == "created"
    assert by_cell[("s3", "a.txt")] == "created"


# (e) Filtered mid-chain under deep: the verdict stops that lane with
# filtered statuses downstream; the sibling is untouched.
def test_deep_filtered_mid_chain():
    create_file("a.txt", "keep")
    create_file("b.txt", "drop")

    @step(name="s1", version="1")
    def s1(path):
        return open(path).read()

    @step(name="s2", version="1", depends_on=["s1"])
    def s2(s1):
        if s1 == "drop":
            return Filtered("not wanted")
        return s1.upper()

    @step(name="s3", version="1", depends_on=["s2"])
    def s3(s2):
        return s2 + "!"

    pipe = pipeline(id="filt", name="filt", folder=TEST_FOLDER, steps=[s1, s2, s3])
    summary = run(pipe, schedule="deep")

    assert summary.status == "completed"
    assert (summary.created_count, summary.filtered_count) == (4, 2)
    by_cell = {
        (s, c): status for (s, c, _, status) in _status_rows(summary.run_id)
    }
    assert by_cell[("s2", "b.txt")] == "filtered"
    assert by_cell[("s3", "b.txt")] == "filtered"
    assert by_cell[("s2", "a.txt")] == "created"
    assert by_cell[("s3", "a.txt")] == "created"


# (f) Barrier correctness under deep: the reduce sees every lane — deep
# never lets it start on a partial set.
def test_deep_reduce_barrier_receives_all_lanes():
    create_file("a.txt", "1")
    create_file("b.txt", "2")
    create_file("c.txt", "3")

    @step(name="parse", version="1")
    def parse(path):
        return int(open(path).read())

    @step(name="dbl", version="1", depends_on=["parse"])
    def dbl(parse):
        return parse * 2

    @step(name="total", version="1", depends_on=["dbl"], shape="reduce")
    def total(dbl):
        return {"n": len(dbl), "total": sum(dbl.values())}

    pipe = pipeline(id="barrier", name="barrier", folder=TEST_FOLDER, steps=[parse, dbl, total])
    summary = run(pipe, schedule="deep")

    assert summary.status == "completed"
    assert summary.created_count == 7  # 3 parse + 3 dbl + 1 total
    with get_session() as session:
        mat = (
            session.query(Materialization)
            .filter_by(step_name="total", is_live=True)
            .one()
        )
        assert read_materialization_output(mat) == {"n": 3, "total": 12}


# (g) Anything but broad/deep is rejected loudly.
def test_invalid_schedule_raises():
    create_file("a.txt", "x")

    @step(name="s1", version="1")
    def s1(path):
        return open(path).read()

    pipe = pipeline(id="bad", name="bad", folder=TEST_FOLDER, steps=[s1])
    with pytest.raises(ValueError, match="schedule"):
        run(pipe, schedule="sideways")
