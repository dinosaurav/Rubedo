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
from rubedo.models import InputHashUsage
from rubedo import lane_store
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


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def make_pipeline():
    """scan -> extract -> summarize -> total (reduce): a 4-step chain."""

    @step
    def extract(scan):
        company, amount = scan["text"].strip().split(",")
        return {"company": company, "amount": int(amount)}

    @step
    def summarize(extract):
        return {"company": extract["company"], "double": extract["amount"] * 2}

    @step(depends_on=["summarize"], shape="reduce")
    def total(summarize):
        return {"sum": sum(v["double"] for v in summarize.values())}

    return pipeline(
        name="invd", steps=[scan, extract, summarize, total]
    )


def _liveness_by_id():
    """Returns {address: (step_name, is_fulfilled)} for all outputs."""
    with get_session() as session:
        idx = lane_store.address_row_index()
        result = {}
        for addr, row in idx.items():
            usage = session.query(InputHashUsage).filter_by(address=addr).first()
            result[addr] = (str(row.get("step_name", "")), bool(usage and usage.fulfilled))
        return result


def test_downstream_flips_seed_and_descendants_then_heals():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    result = invalidate(
        Selection(step="extract", index={"company": "acme"}), reason="bad extract", downstream=True
    )

    # acme's extract (seed) + acme's summarize + the reduce output.
    assert result["invalidated_count"] == 3
    assert result["seed_count"] == 1
    assert result["downstream_count"] == 2

    flipped = set(result["addresses"])
    liveness = _liveness_by_id()
    # Check via IHU that all flipped addresses are unfulfilled
    with get_session() as s:
        for addr in flipped:
            usage = s.query(InputHashUsage).filter_by(address=addr).first()
            assert usage is not None
            assert usage.fulfilled is False
    # The sibling lane (globex extract/summarize) is untouched, and so are
    # both scan lanes (scan is upstream of the seed, never touched by
    # downstream invalidation).
    survivors = {step_name for mid, (step_name, live) in liveness.items() if live}
    assert survivors == {"scan", "extract", "summarize"}
    assert sum(1 for _, (_, live) in liveness.items() if live) == 4

    # Lazy heal: the next run recomputes exactly the invalidated set; both
    # scan lanes plus the surviving globex extract/summarize are reused.
    summary = make_pipeline().run()
    assert summary.created_count == 3
    assert summary.reused_count == 4


def test_downstream_flipped_set_equals_trace_preview():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    sel = Selection(step="extract", index={"company": "acme"})
    # trace() is the preview: capture its live nodes BEFORE invalidating,
    # excluding upstream context (scan) — invalidate(downstream=True) never
    # touches upstream, only the seed and its downstream closure.
    preview = {
        n.output_address
        for n in trace(sel).nodes
        if n.is_live and n.relation != "upstream"
    }

    result = invalidate(sel, reason="preview parity", downstream=True)

    assert set(result["addresses"]) == preview


def test_downstream_invalidation_is_idempotent():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    sel = Selection(step="extract", index={"company": "acme"})
    invalidate(sel, reason="first", downstream=True)

    second = invalidate(sel, reason="second", downstream=True)

    # Nothing left to flip: no re-flips.
    assert second["invalidated_count"] == 0
    assert second["seed_count"] == 0
    assert second["downstream_count"] == 0
    assert second["addresses"] == []


def test_default_invalidation_touches_only_direct_matches():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    result = invalidate(Selection(step="extract", index={"company": "acme"}), reason="just the seed")

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


def test_invalidate_then_plan_sees_recompute():
    """invalidate() followed by .plan() (no run in between) must report
    the invalidated lane as 'execute', not stale 'reuse'.  Regression
    guard for the _FULFILLED_CACHE not being evicted on invalidation.

    Uses a map-root pipeline (not an expand root) so .plan() resolves
    every lane from history — the root expand always re-runs, making
    its downstream 'pending' and hiding the cache staleness.
    """
    @step
    def root():
        return {"items": ["acme,10", "globex,5"]}

    def fan_fn(parent: dict):
        for item in parent["items"]:
            company, amount = item.split(",")
            yield {"company": company, "amount": int(amount)}
    fan_fn.__name__ = "fan"
    fan = step(fn=fan_fn, name="fan", shape="expand", depends_on={"parent": "root"})

    @step
    def summarize(fan: dict):
        return {"company": fan["company"], "double": fan["amount"] * 2}

    pipe = pipeline(name="invd_plan", steps=[root, fan, summarize])
    pipe.run()

    # Prime the fulfilled cache by planning once — the map root and
    # dependent expand resolve from history, so summarize should reuse
    plan_before = pipe.plan()
    assert plan_before.counts.get("reuse", 0) > 0, (
        f"baseline plan should show reuse: {plan_before.counts}"
    )

    # Invalidate the acme summarize lane
    invalidate(Selection(step="summarize", index={"company": "acme"}), reason="test")

    # Plan again WITHOUT running in between — the cache must reflect
    # the invalidation, not stale fulfilled=True
    plan_after = pipe.plan()

    # The invalidated summarize lane should be 'execute', NOT 'reuse'
    assert plan_after.counts.get("execute", 0) > 0, (
        f"stale cache: plan after invalidate shows {plan_after.counts}"
    )
