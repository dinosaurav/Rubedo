"""Retention GC: demote by run recency, sweep unreferenced bytes.

Fixture shape copied from tests/test_index.py: per-test .test_gc_data (scanned)
and .test_gc_env (object store) dirs, never nested; an in-memory shared-cache
SQLite with StaticPool.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import get_session
from rubedo.gc import (
    _anchor_mat_ids,
    _retention_demote_ids,
    auto_prune,
    gc,
)
from rubedo.models import (
    Materialization,
    ObjectReclamation,
    Run,
    RunCoordinateStatus,
)
from rubedo.store import _get_object_path
from rubedo.util import utcnow_iso

TEST_FOLDER = ".test_gc_data"
ENV_FOLDER = ".test_gc_env"


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


def write(name, content, folder=TEST_FOLDER):
    with open(os.path.join(folder, name), "w") as f:
        f.write(content)


def _shout(folder=TEST_FOLDER):
    # Single-step expand root that reads and transforms in the same
    # generator — retention/gc keys off
    # "which of the pipeline's last N runs referenced this materialization",
    # not off address stability, so a plain content-addressed expand root
    # (no separate scan step) is enough here.
    @step
    def shout():
        for name in sorted(os.listdir(folder)):
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                yield open(path).read().upper()

    return shout


def _norm_chain(folder=TEST_FOLDER):
    """scan -> norm: a two-step chain, needed (not the single-step
    _shout() shape above) so pre-strip bytes — not post-strip text — drive
    the address. Same as test_du.py's shared-object case: two inputs that
    differ before the transform ("SHARED" vs "SHARED ") but normalize to
    the same bytes get different addresses sharing one physical object,
    exactly the "same object, different address" case under test."""

    @step
    def scan():
        for name in sorted(os.listdir(folder)):
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def norm(scan):
        return scan["text"].strip()

    return [scan, norm]


def make_pipe(retention=None, folder=TEST_FOLDER, pid="gcp"):
    return pipeline(
        name=pid,
        steps=[_shout(folder)], retention=retention,
    )


def live_hashes():
    with get_session() as s:
        return {
            m.output_content_hash
            for m in s.query(Materialization).filter_by(is_live=True).all()
        }


# ---------------------------------------------------------------------------
# 0. retention= validation and definition() recording
# ---------------------------------------------------------------------------


def test_retention_validation_and_definition_snapshot():
    assert make_pipe(retention=3).definition()["retention"] == 3
    assert "retention" not in make_pipe().definition()  # None -> omitted

    for bad in (0, -1, True, 2.5):
        with pytest.raises(ValueError):
            make_pipe(retention=bad)


# ---------------------------------------------------------------------------
# 1. Core acceptance: retention=2 over three generations
# ---------------------------------------------------------------------------


def test_retention_demotes_only_the_oldest_generation():
    write("a.txt", "gen1")
    make_pipe().run(workers=1)
    write("a.txt", "gen2")
    make_pipe().run(workers=1)
    write("a.txt", "gen3")
    s3 = make_pipe(retention=2).run(workers=1)  # auto-prune fires here

    with get_session() as s:
        mats = {m.output_content_hash[:10]: m for m in s.query(Materialization).all()}
        assert len(mats) == 3
        live = [m for m in mats.values() if m.is_live]
        assert len(live) == 2  # gen2, gen3 kept; gen1 demoted

        # The demoted one is the generation only run 1 referenced.
        demoted = [m for m in mats.values() if not m.is_live]
        assert len(demoted) == 1
        gen1 = demoted[0]

        # Lifecycle rows gone in the new model — liveness is
        # input_hash_usages.fulfilled.

        # Freed object deleted from disk and logged in object_reclamations.
        recl = s.query(ObjectReclamation).all()
        assert len(recl) == 1
        assert recl[0].content_hash == gen1.output_content_hash
        assert recl[0].trigger == "auto_prune"
        assert not os.path.exists(_get_object_path(gen1.output_content_hash))

    # The two kept objects survive on disk.
    for h in live_hashes():
        assert os.path.exists(_get_object_path(h))
    # Latest output byte-identical (coordinates are row-<hash>, not "a.txt").
    assert list(s3.output_for("shout").values()) == ["GEN3"]


# ---------------------------------------------------------------------------
# 2. Shared object: one live reference elsewhere keeps the bytes
# ---------------------------------------------------------------------------


def test_shared_object_with_live_reference_in_another_pipeline_survives():
    other = os.path.abspath(".test_gc_data_other")
    if os.path.exists(other):
        shutil.rmtree(other)
    os.makedirs(other)
    try:
        # Pipeline B produces "SHARED" and keeps it live forever.
        write("b.txt", "SHARED", folder=other)
        pb = pipeline(name="pb", steps=_norm_chain(other))
        pb.run(workers=1)

        # Pipeline A: gen1 also normalizes to "SHARED" (same object, different
        # address), gen2 is different. retention=1 prunes gen1.
        write("a.txt", "SHARED ")  # strips to SHARED -> shares B's object
        make_pipe_norm(retention=1).run(workers=1)
        write("a.txt", "OTHER")
        make_pipe_norm(retention=1).run(workers=1)  # auto-prune demotes gen1

        with get_session() as s:
            shared_hash = None
            for m in s.query(Materialization).all():
                if m.pipeline_id == "pb":
                    shared_hash = m.output_content_hash
            assert shared_hash is not None
            # gen1 (pipeline A, "SHARED") was demoted...
            a_shared = [
                m for m in s.query(Materialization).filter_by(pipeline_id="gcp").all()
                if m.output_content_hash == shared_hash
            ]
            assert a_shared and not a_shared[0].is_live
            # ...but the object was NOT reclaimed: B's live reference keeps it.
            recl_hashes = {r.content_hash for r in s.query(ObjectReclamation).all()}
            assert shared_hash not in recl_hashes
        assert os.path.exists(_get_object_path(shared_hash))
    finally:
        shutil.rmtree(other)


def make_pipe_norm(retention=None):
    return pipeline(
        name="gcp",
        steps=_norm_chain(TEST_FOLDER), retention=retention,
    )


# ---------------------------------------------------------------------------
# 3. Lazy heal: a pruned lane whose input reappears recomputes and restores
# ---------------------------------------------------------------------------


def test_pruned_lane_reappears_and_lazily_heals():
    write("a.txt", "gen1")
    make_pipe().run(workers=1)
    write("a.txt", "gen2")
    make_pipe().run(workers=1)
    write("a.txt", "gen3")
    make_pipe(retention=2).run(workers=1)  # gen1 pruned + object deleted

    with get_session() as s:
        gen1 = [
            m for m in s.query(Materialization).all() if not m.is_live
        ][0]
        gen1_hash = gen1.output_content_hash
        gen1_id = gen1.id
    assert not os.path.exists(_get_object_path(gen1_hash))

    # The input reappears: recompute rewrites the bytes and restores the row.
    write("a.txt", "gen1")
    make_pipe(retention=2).run(workers=1)

    with get_session() as s:
        healed = s.get(Materialization, gen1_id)
        assert healed.is_live  # restored
        # Lifecycle rows gone in the new model
    assert os.path.exists(_get_object_path(gen1_hash))  # bytes back on disk


# ---------------------------------------------------------------------------
# 4. Refuse/skip while another run's heartbeat is live (restore race, trap 3)
# ---------------------------------------------------------------------------


def test_gc_refuses_and_auto_prune_skips_while_a_run_is_live():
    write("a.txt", "gen1")
    make_pipe().run(workers=1)
    write("a.txt", "gen2")
    make_pipe().run(workers=1)
    write("a.txt", "gen3")
    make_pipe().run(workers=1)  # no retention -> nothing pruned yet

    # Inject a run whose heartbeat is fresh (effective status == running).
    with get_session() as s:
        s.add(
            Run(
                id="run_live",
                kind="process",
                pipeline_id="gcp",
                started_at=utcnow_iso(),
                last_heartbeat_at=utcnow_iso(),
            )
        )
        s.commit()

    # gc --delete refuses; nothing is written or deleted.
    report = gc(delete=True, max_bytes=1)
    assert report.refused is not None
    assert not report.applied
    with get_session() as s:
        assert s.query(ObjectReclamation).count() == 0
        assert s.query(Materialization).filter_by(is_live=False).count() == 0

    # A dry-run is still allowed (touches nothing).
    dry = gc(delete=False, max_bytes=1)
    assert dry.refused is None

    # auto_prune skips (returns None) for a *different* run while run_live beats.
    with get_session() as s:
        assert auto_prune(s, "gcp", "run_other", 1) is None


# ---------------------------------------------------------------------------
# 5. Dry-run lists exactly what --delete does, and deletes nothing (+ budget)
# ---------------------------------------------------------------------------


def test_dry_run_matches_delete_and_budget_prunes_oldest_first():
    # No retention -> no auto-prune; three distinct generations on disk.
    write("a.txt", "AAAA")  # -> "AAAA" (4 bytes)
    make_pipe().run(workers=1)
    write("a.txt", "BBBBB")  # -> 5 bytes
    make_pipe().run(workers=1)
    write("a.txt", "CCCCCC")  # -> 6 bytes (latest terminal run)
    make_pipe().run(workers=1)

    # Map each generation's output text -> content hash while all are present.
    from rubedo.store import read_materialization_output

    with get_session() as s:
        hash_of = {
            read_materialization_output(m): m.output_content_hash
            for m in s.query(Materialization).all()
        }

    # Total = 15 bytes; budget 11 forces dropping the oldest 4-byte object.
    dry = gc(delete=False, max_bytes=11)
    assert not dry.applied
    assert len(dry.demoted_mat_ids) == 1
    assert dry.reclaimed_bytes == 4  # the "AAAA" generation
    dry_ids = list(dry.demoted_mat_ids)
    dry_reclaimed = list(dry.reclaimed)

    # Dry-run wrote nothing: all still live, all objects present.
    with get_session() as s:
        assert s.query(Materialization).filter_by(is_live=False).count() == 0
        assert s.query(ObjectReclamation).count() == 0

    # --delete performs exactly what the dry-run listed.
    done = gc(delete=True, max_bytes=11)
    assert done.applied
    assert done.demoted_mat_ids == dry_ids
    assert done.reclaimed == dry_reclaimed

    with get_session() as s:
        demoted = s.query(Materialization).filter_by(is_live=False).all()
        assert [m.id for m in demoted] == dry_ids
        assert demoted[0].output_content_hash == hash_of["AAAA"]  # oldest
        # Latest terminal run's output (CCCCCC) is never a candidate.
        live = {m.output_content_hash for m in s.query(Materialization).filter_by(is_live=True).all()}
        assert hash_of["CCCCCC"] in live
    assert not os.path.exists(_get_object_path(dry_reclaimed[0][0]))


# ---------------------------------------------------------------------------
# 6. Expand anchor survives a prune and is still reused (trap 5)
# ---------------------------------------------------------------------------


def _scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""

    @step
    def scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    return scan


def _read():
    @step
    def read(scan):
        return scan["text"]

    return read


_split_calls = {"n": 0}


def _split():
    @step
    def split(read):
        _split_calls["n"] += 1
        for line in read.splitlines():
            yield {"line": line}

    return split


def _upper():
    @step
    def upper(split):
        return split["line"].upper()

    return upper


def make_expand_pipe(retention=None):
    return pipeline(
        name="xp",
        steps=[_scan(), _read(), _split(), _upper()], retention=retention,
    )


def test_expand_anchor_kept_by_widened_keepset_and_still_reuses():
    write("a.txt", "alpha\nbeta")
    # Three runs, content unchanged: the anchor is needed by every run for
    # reuse but never appears in RunCoordinateStatus.
    make_expand_pipe(retention=2).run(workers=1)
    make_expand_pipe(retention=2).run(workers=1)
    make_expand_pipe(retention=2).run(workers=1)  # auto-prune fires

    with get_session() as s:
        # The anchor = the split-step materialization with no status row.
        rcs_ids = {
            r.materialization_id
            for r in s.query(RunCoordinateStatus).all()
            if r.materialization_id
        }
        split_mats = s.query(Materialization).filter_by(step_name="split").all()
        anchors = [m for m in split_mats if m.id not in rcs_ids]
        assert len(anchors) == 1
        anchor = anchors[0]

        # It survived the prune.
        assert anchor.is_live

        # The widening is load-bearing: _anchor_mat_ids finds it, the keep-set
        # protects it, and WITHOUT the widening it would be demoted.
        anchor_ids = _anchor_mat_ids(s)
        assert anchor.id in anchor_ids
        assert anchor.id not in _retention_demote_ids(s, {"xp": 2}, anchor_ids)
        assert anchor.id in _retention_demote_ids(s, {"xp": 2}, set())

    # A subsequent run still reuses via the anchor: the expand fn is NOT called.
    calls_before = _split_calls["n"]
    summary = make_expand_pipe(retention=2).run(workers=1)
    assert _split_calls["n"] == calls_before  # anchor reuse: fn skipped
    assert summary.created_count == 0


# ---------------------------------------------------------------------------
# 7. Manual gc applies recorded retention policy
# ---------------------------------------------------------------------------


def test_gc_applies_recorded_retention_policy_manually():
    # Runs carry retention=2 but we can still call gc() to reconcile; here the
    # auto-prune already trimmed to the window, so a manual gc is a no-op.
    write("a.txt", "gen1")
    make_pipe(retention=2).run(workers=1)
    write("a.txt", "gen2")
    make_pipe(retention=2).run(workers=1)
    write("a.txt", "gen3")
    make_pipe(retention=2).run(workers=1)

    report = gc(delete=True)  # reads retention=2 from latest run's definition
    assert report.applied
    # gen1 already auto-pruned; nothing left to do.
    assert report.demoted_count == 0
    with get_session() as s:
        assert s.query(Materialization).filter_by(is_live=True).count() == 2
