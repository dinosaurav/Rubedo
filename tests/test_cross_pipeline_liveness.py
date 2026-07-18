"""Cross-pipeline liveness scoping (TODO 33).

Before this fix, `compute_output_address` hashed `(step, version,
input_hash[, params][, code])` with no pipeline identity, so two
pipelines with an identically named+versioned step and identical input
(the copy-a-pipeline-to-experiment case) shared one
`input_hash_usages` liveness row: invalidating or retention-pruning one
pipeline silently flipped the other's `fulfilled` bit too. The fix folds
the pipeline name into the address at the single mint point
(`compute_output_address`, `pipeline` is a required, always-last
segment) — every address consumer (IHU, Arrow lookups,
`RunCoordinateStatus`, `MaterializationEdge`) inherits the scoping for
free.

What must NOT change (and is pinned here): `input_hash` and expand
lane-key minting (`row-<hash>`) stay pipeline-free, so identical payload
content still mints the identical lane key in any pipeline — the
containment property (docs/concepts/sources.md) and cross-pipeline byte
dedup depend on it.

Fixture shape copied from tests/test_index.py: per-test .test_cpl_data
(scanned) and .test_cpl_env (object store) dirs, never nested; an
in-memory shared-cache SQLite with StaticPool.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline
from rubedo import lane_store
from rubedo.db import get_session, init_db
from rubedo.models import InputHashUsage, ObjectReclamation
from rubedo.store import _get_object_path, init_store

FOLDER_A = ".test_cpl_data_a"
FOLDER_B = ".test_cpl_data_b"
ENV_FOLDER = ".test_cpl_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_a = os.path.abspath(FOLDER_A)
    abs_b = os.path.abspath(FOLDER_B)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_a, abs_b, abs_env_folder):
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

    for d in (abs_a, abs_b, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def write(folder, name, content):
    with open(os.path.join(folder, name), "w") as f:
        f.write(content)


def _hash_of(row):
    """Parse the content hash from a row's `output` ref string, or None
    for inline (native Arrow) values."""
    out = row.get("output")
    if isinstance(out, str) and out.startswith("objects:"):
        return out[len("objects:"):]
    return None


def make_scan(folder):
    """A rescanning expand root (check_cache=False — its anchor address is
    otherwise keyed on the constant ROOT_LANE, so without this it would
    never notice the folder's content actually changed between runs)."""

    @step(check_cache=False)
    def scan():
        for name in sorted(os.listdir(folder)):
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    return scan


@step
def extract(scan: dict):
    return {"upper": scan["text"].upper()}


def make_pipe(name, folder, retention=None):
    return pipeline(name=name, steps=[make_scan(folder), extract], retention=retention)


def make_norm_scan(folder):
    @step(check_cache=False)
    def scan():
        for name in sorted(os.listdir(folder)):
            path = os.path.join(folder, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    return scan


def make_norm_step():
    @step
    def norm(scan: dict):
        # Bytes output spills to the content-addressed object store —
        # needed to exercise the shared-bytes GC scenario.
        return scan["text"].strip().encode("utf-8")

    return norm


def make_norm_pipe(name, folder, retention=None):
    return pipeline(
        name=name, steps=[make_norm_scan(folder), make_norm_step()], retention=retention
    )


# ---------------------------------------------------------------------------
# 1. Identical payloads mint identical lane keys across pipelines (pinned)
# ---------------------------------------------------------------------------


def test_identical_payloads_mint_identical_lane_keys_across_pipelines():
    """The one thing TODO 33 must NOT change: `input_hash`/lane-key
    minting stays pipeline-free. Two pipelines scanning identically-named,
    identically-content'd files land on the same `row-<hash>` lane key —
    only the *output address* differs by pipeline."""
    write(FOLDER_A, "a.txt", "hello")
    write(FOLDER_B, "a.txt", "hello")

    pa = make_pipe("pipe-a", FOLDER_A)
    pb = make_pipe("pipe-b", FOLDER_B)
    pa.run(workers=1)
    pb.run(workers=1)

    # Exclude each root's cache anchor (lane_key "@root") — a bookkeeping
    # row, not a lane; only the content-addressed child lane matters here.
    scan_a = [r for r in lane_store.all_filled_rows()
              if r["pipeline_id"] == "pipe-a" and r["step_name"] == "scan"
              and r["lane_key"] != "@root"]
    scan_b = [r for r in lane_store.all_filled_rows()
              if r["pipeline_id"] == "pipe-b" and r["step_name"] == "scan"
              and r["lane_key"] != "@root"]
    assert len(scan_a) == 1 and len(scan_b) == 1

    # Lane key (coordinate): identical content -> identical row-<hash> key,
    # regardless of which pipeline scanned it.
    assert scan_a[0]["lane_key"] == scan_b[0]["lane_key"]
    assert scan_a[0]["lane_key"].startswith("row-")

    # Output address: pipeline-scoped -> different, even though step name,
    # version, and input_hash all match.
    assert scan_a[0]["address"] != scan_b[0]["address"]
    assert scan_a[0]["input_hash"] == scan_b[0]["input_hash"]

    # Same story one hop downstream through a map step.
    extract_a = [r for r in lane_store.all_filled_rows()
                 if r["pipeline_id"] == "pipe-a" and r["step_name"] == "extract"]
    extract_b = [r for r in lane_store.all_filled_rows()
                 if r["pipeline_id"] == "pipe-b" and r["step_name"] == "extract"]
    assert extract_a[0]["lane_key"] == extract_b[0]["lane_key"]
    assert extract_a[0]["address"] != extract_b[0]["address"]


# ---------------------------------------------------------------------------
# 2. Invalidating pipeline A leaves pipeline B's reuse intact
# ---------------------------------------------------------------------------


def test_invalidating_pipeline_a_leaves_pipeline_b_reuse_intact():
    write(FOLDER_A, "a.txt", "hello")
    write(FOLDER_B, "a.txt", "hello")

    pa = make_pipe("pipe-a", FOLDER_A)
    pb = make_pipe("pipe-b", FOLDER_B)
    pa.run(workers=1)
    pb.run(workers=1)

    res = invalidate(Selection(pipeline_id="pipe-a"), reason="redo pipe-a")
    # scan's child lane + scan's cache anchor + extract, pipe-a only.
    assert res["invalidated_count"] == 3

    with get_session() as s:
        b_addrs = {
            r["address"] for r in lane_store.all_filled_rows()
            if r["pipeline_id"] == "pipe-b"
        }
        unfulfilled = {
            str(u.address) for u in s.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(False)).all()
        }
        # None of B's addresses were touched by A's invalidation.
        assert not (b_addrs & unfulfilled)

    # Rerun both with unchanged input: A recomputes (was invalidated), B
    # fully reuses (was never touched).
    summary_a = pa.run(workers=1)
    summary_b = pb.run(workers=1)
    assert summary_a.created_count == 2
    assert summary_a.reused_count == 0
    assert summary_b.created_count == 0
    assert summary_b.reused_count == 2


# ---------------------------------------------------------------------------
# 3. Retention-pruning pipeline A doesn't force pipeline B to recompute
# ---------------------------------------------------------------------------


def test_retention_pruning_pipeline_a_does_not_recompute_pipeline_b():
    # pipe-b runs once, content matching pipe-a's first generation.
    write(FOLDER_B, "b.txt", "gen1")
    pb = make_pipe("pipe-b", FOLDER_B)
    pb.run(workers=1)

    # pipe-a churns through three generations; retention=2 on the third
    # run auto-prunes (demotes) the first.
    write(FOLDER_A, "a.txt", "gen1")
    make_pipe("pipe-a", FOLDER_A).run(workers=1)
    write(FOLDER_A, "a.txt", "gen2")
    make_pipe("pipe-a", FOLDER_A).run(workers=1)
    write(FOLDER_A, "a.txt", "gen3")
    make_pipe("pipe-a", FOLDER_A, retention=2).run(workers=1)

    with get_session() as s:
        # Sanity: pipe-a's gen1 really was demoted (retention actually ran).
        fulfilled = {
            str(u.address) for u in s.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True)).all()
        }
        a_rows = [r for r in lane_store.all_filled_rows() if r["pipeline_id"] == "pipe-a"]
        a_live = [r for r in a_rows if r["address"] in fulfilled]
        a_demoted = [r for r in a_rows if r["address"] not in fulfilled]
        assert len(a_demoted) == 2  # gen1's scan + extract lanes

        # pipe-b's own address for the *same content* ("gen1") is a
        # different row and was never referenced by pipe-a's prune, so it
        # stays live throughout.
        b_rows = [r for r in lane_store.all_filled_rows() if r["pipeline_id"] == "pipe-b"]
        assert all(r["address"] in fulfilled for r in b_rows)
        assert len(a_live) >= 2

    # Rerunning pipe-b with unchanged content ("gen1") fully reuses —
    # pipe-a's retention sweep never touched it.
    summary_b2 = pb.run(workers=1)
    assert summary_b2.created_count == 0
    assert summary_b2.reused_count == 2


# ---------------------------------------------------------------------------
# 4. GC: shared content-store bytes survive while either pipeline
#    references them, even though their addresses now differ (TODO 33
#    salts the address, not the content hash the GC sweep refcounts).
# ---------------------------------------------------------------------------


def test_shared_object_bytes_survive_while_either_pipeline_references_them():
    # Both pipelines normalize to the identical bytes b"SHARED", so they
    # share one physical object in the content-addressed store — but their
    # *addresses* differ (different pipeline), so they are two independent
    # liveness rows over the one object.
    write(FOLDER_A, "a.txt", "SHARED ")  # strips to b"SHARED"
    write(FOLDER_B, "b.txt", "SHARED")   # already b"SHARED"

    pa = make_norm_pipe("pipe-a", FOLDER_A, retention=1)
    pb = make_norm_pipe("pipe-b", FOLDER_B)  # no retention: never pruned
    pa.run(workers=1)
    pb.run(workers=1)

    # A second, different generation for pipe-a: retention=1 auto-prunes
    # its "SHARED" generation on this run.
    write(FOLDER_A, "a.txt", "OTHER")
    pa.run(workers=1)

    with get_session() as s:
        shared_hash = None
        for r in lane_store.all_filled_rows():
            if r.get("pipeline_id") == "pipe-b":
                h = _hash_of(r)
                if h is not None:
                    shared_hash = h
        assert shared_hash is not None

        a_shared = [
            r for r in lane_store.all_filled_rows()
            if r.get("pipeline_id") == "pipe-a" and _hash_of(r) == shared_hash
        ]
        assert a_shared
        ihu = s.query(InputHashUsage).filter_by(address=a_shared[0]["address"]).first()
        assert ihu is not None and ihu.fulfilled is False  # pipe-a's ref demoted

        # ...but the bytes were NOT reclaimed: pipe-b's live reference
        # (a different address, same content hash) keeps them.
        recl_hashes = {r.content_hash for r in s.query(ObjectReclamation).all()}
        assert shared_hash not in recl_hashes
    assert os.path.exists(_get_object_path(shared_hash))
