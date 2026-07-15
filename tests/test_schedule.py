"""schedule="broad" | "deep": scheduling changes order, never results.

broad (default) stages step by step — every lane of step N completes
before any lane starts step N+1. deep pipelines each lane through
consecutive 1:1 (map) steps as soon as its own inputs commit; reduce/join
(and, for now, expand and multi-parent maps) remain barriers that
synchronize on all lanes. Ledger rows — statuses, addresses, lifecycle —
must be identical across modes.

schedule=/home= are Pipeline construction-time settings: a cross-mode
comparison builds two Pipeline wrappers over the *same*
underlying step objects (identical addresses/hashes) rather than reusing
one Pipeline instance with different settings per call.
"""

import os
import shutil
import threading
import time
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Filtered, pipeline, step
from rubedo.db import init_db, get_session
from rubedo.models import (
    Materialization,
    MaterializationIndexEntry,
    RunCoordinateStatus,
)
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


@step(index=["path"])
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content. Indexed on
    `path` so tests can find "the lane for x.txt" without the coordinate
    being that literal string."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def coord_for_path(filename):
    """The coordinate scan minted for `filename` — coordinates are
    row-<hash>, not the filename. A dependent 1:1 map step shares its
    ancestor's coordinate unchanged."""
    with get_session() as session:
        rows = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="scan")
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        )
        for rc in rows:
            hit = (
                session.query(MaterializationIndexEntry)
                .filter_by(
                    materialization_id=rc.materialization_id,
                    field="path",
                    value=filename,
                )
                .first()
            )
            if hit:
                return rc.coordinate
    return None


def _chain_steps():
    """The 3-step map chain (on top of the module-level `scan` root) used
    by the equivalence tests."""

    @step
    def s1(scan):
        return scan["text"].strip()

    @step
    def s2(s1):
        return s1.upper()

    @step
    def s3(s2):
        return s2 + "!"

    return [scan, s1, s2, s3]


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
    steps = _chain_steps()

    home_a = os.path.join(os.path.abspath(ENV_FOLDER), "homeA")
    home_b = os.path.join(os.path.abspath(ENV_FOLDER), "homeB")

    pipe_broad = pipeline(name="equiv", steps=steps, schedule="broad", home=home_a)
    pipe_deep = pipeline(name="equiv", steps=steps, schedule="deep", home=home_b)

    s_broad = pipe_broad.run()
    facts_broad = _status_rows(s_broad.run_id)  # read before re-pointing at B
    hashes_broad = _mat_hashes()

    s_deep = pipe_deep.run()
    facts_deep = _status_rows(s_deep.run_id)
    hashes_deep = _mat_hashes()

    assert s_broad.status == s_deep.status == "completed"
    assert (s_broad.created_count, s_deep.created_count) == (12, 12)  # 3 files x (scan+s1+s2+s3)
    assert facts_broad == facts_deep
    assert hashes_broad == hashes_deep


# (b) Cross-mode reuse: either mode fully reuses the other's store.
def test_broad_then_deep_reuses_everything():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    steps = _chain_steps()
    home = os.path.join(os.path.abspath(ENV_FOLDER), "homeC")

    pipe_broad = pipeline(name="cross1", steps=steps, schedule="broad", home=home)
    pipe_deep = pipeline(name="cross1", steps=steps, schedule="deep", home=home)

    s1 = pipe_broad.run()
    assert (s1.created_count, s1.reused_count) == (8, 0)  # 2 files x 4 steps
    s2 = pipe_deep.run()
    assert (s2.created_count, s2.reused_count) == (0, 8)


def test_deep_then_broad_reuses_everything():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    steps = _chain_steps()
    home = os.path.join(os.path.abspath(ENV_FOLDER), "homeD")

    pipe_deep = pipeline(name="cross2", steps=steps, schedule="deep", home=home)
    pipe_broad = pipeline(name="cross2", steps=steps, schedule="broad", home=home)

    s1 = pipe_deep.run()
    assert (s1.created_count, s1.reused_count) == (8, 0)
    s2 = pipe_broad.run()
    assert (s2.created_count, s2.reused_count) == (0, 8)


# (c) Deep actually pipelines: step1(B) can only finish if step2(A) runs
# while step1(B) is still in flight. Deterministic — an Event, no sleeps.
# Do NOT run this pipeline under broad: it deadlocks by construction there
# (broad never starts step2 before every step1 lane is done).
def test_deep_pipelines_lanes_across_steps():
    create_file("a.txt", "A")
    create_file("b.txt", "B")
    gate = threading.Event()

    @step
    def s1(scan):
        if scan["path"] == "b.txt":
            if not gate.wait(timeout=30):
                raise RuntimeError(
                    "gate never opened: s2(a) did not run while s1(b) was in flight"
                )
        return scan["path"]

    @step
    def s2(s1):
        if s1 == "a.txt":
            gate.set()  # proves s2(A) ran before s1(B) completed
        return s1.upper()

    pipe = pipeline(name="deep_pipe", steps=[scan, s1, s2], schedule="deep")
    summary = pipe.run()

    assert gate.is_set()
    assert summary.status == "completed"
    # scan is a barrier (expand root) under deep; s1/s2 pipeline through it.
    assert (summary.created_count, summary.failed_count, summary.blocked_count) == (
        6,  # 2 files x (scan + s1 + s2)
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

    @step
    def s1(scan):
        out = scan["text"]
        s1_finished.append(time.monotonic())
        return out

    @step
    def s2(s1):
        s2_started.append(time.monotonic())
        return s1 * 2

    pipe = pipeline(name="staged", steps=[scan, s1, s2])  # broad is the default
    summary = pipe.run()

    assert summary.created_count == 6  # 2 files x (scan + s1 + s2)
    assert len(s1_finished) == 2 and len(s2_started) == 2
    assert max(s1_finished) < min(s2_started)


# (d) Failure cascade under deep: a lane failing at step 1 blocks its own
# downstream cells; the sibling lane completes fully.
def test_deep_failure_cascades_to_downstream_cells():
    create_file("a.txt", "good")
    create_file("b.txt", "boom")

    @step
    def s1(scan):
        text = scan["text"]
        if text == "boom":
            raise ValueError("bad lane")
        return text

    @step
    def s2(s1):
        return s1.upper()

    @step
    def s3(s2):
        return s2 + "!"

    pipe = pipeline(name="cascade", steps=[scan, s1, s2, s3], schedule="deep")
    summary = pipe.run()

    assert summary.status == "completed_with_failures"
    # scan(a) + scan(b) + s1(a) + s2(a) + s3(a); s1(b) fails, s2/s3(b) block.
    assert (summary.created_count, summary.failed_count, summary.blocked_count) == (
        5,
        1,
        2,
    )
    coord_a = coord_for_path("a.txt")
    coord_b = coord_for_path("b.txt")
    by_cell = {
        (s, c): status for (s, c, _, status) in _status_rows(summary.run_id)
    }
    assert by_cell[("s1", coord_b)] == "failed"
    assert by_cell[("s2", coord_b)] == "blocked"
    assert by_cell[("s3", coord_b)] == "blocked"
    assert by_cell[("s1", coord_a)] == "created"
    assert by_cell[("s2", coord_a)] == "created"
    assert by_cell[("s3", coord_a)] == "created"


# (e) Filtered mid-chain under deep: the verdict stops that lane with
# filtered statuses downstream; the sibling is untouched.
def test_deep_filtered_mid_chain():
    create_file("a.txt", "keep")
    create_file("b.txt", "drop")

    @step
    def s1(scan):
        return scan["text"]

    @step
    def s2(s1):
        if s1 == "drop":
            return Filtered("not wanted")
        return s1.upper()

    @step
    def s3(s2):
        return s2 + "!"

    pipe = pipeline(name="filt", steps=[scan, s1, s2, s3], schedule="deep")
    summary = pipe.run()

    assert summary.status == "completed"
    # scan(a)+scan(b)+s1(a)+s1(b) [s1 never filters] + s2(a) + s3(a)
    assert (summary.created_count, summary.filtered_count) == (6, 2)
    coord_a = coord_for_path("a.txt")
    coord_b = coord_for_path("b.txt")
    by_cell = {
        (s, c): status for (s, c, _, status) in _status_rows(summary.run_id)
    }
    assert by_cell[("s2", coord_b)] == "filtered"
    assert by_cell[("s3", coord_b)] == "filtered"
    assert by_cell[("s2", coord_a)] == "created"
    assert by_cell[("s3", coord_a)] == "created"


# (f) Barrier correctness under deep: the reduce sees every lane — deep
# never lets it start on a partial set.
def test_deep_reduce_barrier_receives_all_lanes():
    create_file("a.txt", "1")
    create_file("b.txt", "2")
    create_file("c.txt", "3")

    @step
    def parse(scan):
        return int(scan["text"])

    @step
    def dbl(parse):
        return parse * 2

    @step(depends_on=["dbl"], shape="reduce")
    def total(dbl):
        return {"n": len(dbl), "total": sum(dbl.values())}

    pipe = pipeline(name="barrier", steps=[scan, parse, dbl, total], schedule="deep")
    summary = pipe.run()

    assert summary.status == "completed"
    assert summary.created_count == 10  # 3 scan + 3 parse + 3 dbl + 1 total
    with get_session() as session:
        mat = (
            session.query(Materialization)
            .filter_by(step_name="total", is_live=True)
            .one()
        )
        assert read_materialization_output(mat) == {"n": 3, "total": 12}


# (h) The rate limiter is one instance per step per run, shared across every
# lane deep dispatches. Regression guard for the limiter hoist: if a future
# refactor mints a limiter per (lane) call again, all lanes start at once and
# the gaps collapse to ~0 — silently hammering whatever the limit protected.
def test_deep_shared_rate_limiter_paces_across_lanes():
    create_file("a.txt", "1")
    create_file("b.txt", "2")
    create_file("c.txt", "3")
    starts: list = []  # appended post-acquire, at step-fn entry

    @step(workers=4)
    def fetch(scan):
        return scan["text"]

    # 2/s → min_interval 0.5s between permitted starts.
    @step(rate_limit="2/s", workers=4)
    def enrich(fetch):
        starts.append(time.monotonic())
        return fetch * 2

    pipe = pipeline(name="paced", steps=[scan, fetch, enrich], schedule="deep")
    summary = pipe.run()

    assert summary.status == "completed"
    assert len(starts) == 3
    gaps = [b - a for a, b in zip(sorted(starts), sorted(starts)[1:])]
    # Nominal spacing is 0.5s; allow scheduling jitter but catch the failure
    # mode (unshared limiters ⇒ gaps ≈ 0) with a wide margin.
    assert all(g >= 0.4 for g in gaps), f"lanes not paced by a shared limiter: {gaps}"


# (h) Deep hides other work inside a rate-limited stage's dead time: a lane
# that has passed the limiter proceeds through its downstream cells while
# later lanes are still waiting for permits — so some `post` starts before
# the LAST `mid` starts. The limiter makes this deterministic: mid starts
# are ≥0.5s apart, and a lane's post follows its own mid within
# milliseconds. (Broad's counterpart — no overlap, ever — is
# test_broad_stages_whole_steps above.)
def test_deep_overlaps_downstream_with_rate_limited_stage():
    create_file("a.txt", "1")
    create_file("b.txt", "2")
    create_file("c.txt", "3")
    mid_starts, post_starts = [], []

    @step(workers=4)
    def src(scan):
        return scan["text"]

    @step(rate_limit="2/s", workers=4)
    def mid(src):
        mid_starts.append(time.monotonic())
        return src * 2

    @step(workers=4)
    def post(mid):
        post_starts.append(time.monotonic())
        return mid + "!"

    pipe = pipeline(name="overlap", steps=[scan, src, mid, post], schedule="deep")
    summary = pipe.run()

    assert summary.status == "completed"
    assert len(mid_starts) == len(post_starts) == 3
    # The overlap: at least one post ran while the limiter still had lanes
    # queued (before the last mid start, which is ≥1s after the first).
    assert min(post_starts) < max(mid_starts), (
        "deep never overlapped downstream work with the rate-limited stage"
    )


# (g) Anything but broad/deep is rejected loudly at pipeline() construction
# time.
def test_invalid_schedule_raises():
    @step
    def s1(scan):
        return scan["text"]

    with pytest.raises(ValueError, match="schedule"):
        pipeline(name="bad", steps=[scan, s1], schedule="sideways")
