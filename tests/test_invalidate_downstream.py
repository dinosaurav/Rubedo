"""invalidate(..., downstream=True): lane-level (downstream) invalidation.

Semantics under test: seeds are the selection's *live* matches (mirroring
trace's default seeding); the flipped set is seeds plus the full downstream
closure over MaterializationEdge (trace's own _bfs); already-non-live nodes
are passed through but never re-flipped; upstream is never touched; every
flip pairs with exactly one lifecycle row; and trace() is the exact preview
of the blast radius. downstream=False keeps the pre-existing behavior.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline, trace
from rubedo.db import get_session, init_db
from rubedo.models import Materialization, MaterializationLifecycle
from rubedo.store import init_store

TEST_FOLDER = ".test_invalidate_downstream_data"
ENV_FOLDER = ".test_invalidate_downstream_env"


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
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def make_pipeline():
    """scan -> extract (indexed) -> summarize -> total (reduce): a 4-step chain."""

    @step(name="extract", version="1", depends_on=["scan"], index=["company"])
    def extract(scan):
        company, amount = scan["text"].strip().split(",")
        return {"company": company, "amount": int(amount)}

    @step(name="summarize", version="1", depends_on=["extract"])
    def summarize(extract):
        return {"company": extract["company"], "double": extract["amount"] * 2}

    @step(name="total", version="1", depends_on=["summarize"], shape="reduce")
    def total(summarize):
        return {"sum": sum(v["double"] for v in summarize.values())}

    return pipeline(
        name="invd", steps=[scan, extract, summarize, total]
    )


def _liveness_by_id():
    with get_session() as session:
        return {
            int(m.id): (str(m.step_name), bool(m.is_live))
            for m in session.query(Materialization).all()
        }


def _invalidated_lifecycle_counts(mat_ids):
    with get_session() as session:
        rows = (
            session.query(MaterializationLifecycle)
            .filter(
                MaterializationLifecycle.materialization_id.in_(mat_ids),
                MaterializationLifecycle.action == "invalidated",
            )
            .all()
        )
    counts: dict = {}
    for r in rows:
        counts[int(r.materialization_id)] = counts.get(int(r.materialization_id), 0) + 1
    return counts


def test_downstream_flips_seed_and_descendants_then_heals():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    result = invalidate(
        Selection(index={"company": "acme"}), reason="bad extract", downstream=True
    )

    # acme's extract (seed) + acme's summarize + the reduce output.
    assert result["invalidated_count"] == 3
    assert result["seed_count"] == 1
    assert result["downstream_count"] == 2

    flipped = set(result["materialization_ids"])
    liveness = _liveness_by_id()
    for mat_id in flipped:
        assert liveness[mat_id][1] is False
    # The sibling lane (globex extract/summarize) is untouched, and so are
    # both scan lanes (scan is upstream of the seed, never touched by
    # downstream invalidation).
    survivors = {step_name for mid, (step_name, live) in liveness.items() if live}
    assert survivors == {"scan", "extract", "summarize"}
    assert sum(1 for _, (_, live) in liveness.items() if live) == 4

    # Every flip ships exactly one "invalidated" lifecycle row (the pairing
    # guard requires it — see notes/invariants.md).
    assert _invalidated_lifecycle_counts(flipped) == {m: 1 for m in flipped}

    # Lazy heal: the next run recomputes exactly the invalidated set; both
    # scan lanes plus the surviving globex extract/summarize are reused.
    summary = make_pipeline().run()
    assert summary.created_count == 3
    assert summary.reused_count == 4


def test_downstream_flipped_set_equals_trace_preview():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    sel = Selection(index={"company": "acme"})
    # trace() is the preview: capture its live nodes BEFORE invalidating,
    # excluding upstream context (scan) — invalidate(downstream=True) never
    # touches upstream, only the seed and its downstream closure.
    preview = {
        n.materialization_id
        for n in trace(sel).nodes
        if n.is_live and n.relation != "upstream"
    }

    result = invalidate(sel, reason="preview parity", downstream=True)

    assert set(result["materialization_ids"]) == preview


def test_downstream_invalidation_is_idempotent():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    sel = Selection(index={"company": "acme"})
    first = invalidate(sel, reason="first", downstream=True)
    affected = set(first["materialization_ids"])
    counts_after_first = _invalidated_lifecycle_counts(affected)

    second = invalidate(sel, reason="second", downstream=True)

    # Nothing left to flip: no re-flips, no new lifecycle rows.
    assert second["invalidated_count"] == 0
    assert second["seed_count"] == 0
    assert second["downstream_count"] == 0
    assert second["materialization_ids"] == []
    assert _invalidated_lifecycle_counts(affected) == counts_after_first


def test_default_invalidation_touches_only_direct_matches():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    result = invalidate(Selection(index={"company": "acme"}), reason="just the seed")

    assert result["invalidated_count"] == 1
    assert result["seed_count"] == 1
    assert result["downstream_count"] == 0

    liveness = _liveness_by_id()
    dead = [step_name for _, (step_name, live) in liveness.items() if not live]
    assert dead == ["extract"]
    # Downstream summarize/total stay live, and so do both scan lanes
    # (upstream of extract, never touched).
    live_steps = sorted(step_name for _, (step_name, live) in liveness.items() if live)
    assert live_steps == [
        "extract", "scan", "scan", "summarize", "summarize", "total",
    ]
