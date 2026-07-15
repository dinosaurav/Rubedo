"""trace(): lane-following over lineage edges, seeded by a selection.

Semantics under test: live-only seeding by default (include_superseded=True
widens it), traversal follows real edges regardless of liveness, lineage
roots resolve their stored payload for display, and no auto-indexing exists —
seeding rides on @step(index=[...]) declarations.
"""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline, trace
from rubedo.db import init_db
from rubedo.store import init_store

TEST_FOLDER = ".test_trace_data"
ENV_FOLDER = ".test_trace_env"


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
    """Folder recipe: walk TEST_FOLDER, yield each file's content. scan is
    the lineage root: extract's own upstream."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def make_pipeline():
    """scan -> extract (indexed) -> summarize -> total (reduce): a 4-step chain."""

    @step(index=["company"])
    def extract(scan):
        company, amount = scan["text"].strip().split(",")
        return {"company": company, "amount": int(amount)}

    @step
    def summarize(extract):
        return {"company": extract["company"], "double": extract["amount"] * 2}

    @step(depends_on=["summarize"], shape="reduce")
    def total(summarize):
        return {"sum": sum(v["double"] for v in summarize.values())}

    return pipeline(name="tr", steps=[scan, extract, summarize, total])


def _by_step(result):
    return {s: nodes for s, nodes in result.by_step().items()}


def test_trace_downstream_from_indexed_seed():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    make_pipeline().run()

    result = trace(Selection(index={"company": "acme"}))

    steps = _by_step(result)
    # Seeded at extract; downstream reaches acme's summarize and the reduce,
    # and upstream reaches acme's scan lane (scan is extract's own
    # upstream).
    assert [n.relation for n in steps["extract"]] == ["seed"]
    assert [n.relation for n in steps["summarize"]] == ["downstream"]
    assert [n.relation for n in steps["total"]] == ["downstream"]
    assert [n.relation for n in steps["scan"]] == ["upstream"]
    assert steps["summarize"][0].depth == 1
    assert steps["total"][0].depth == 2
    # globex's scan/extract/summarize lanes are not connected to the acme
    # seed except through the reduce — they must not appear.
    assert len(result.nodes) == 4
    assert all(n.is_live for n in result.nodes)
    # Edges cover the three hops (scan->extract, extract->summarize,
    # summarize->total).
    assert len(result.edges) == 3


def test_trace_upstream_resolves_root_payload():
    create_file("a.txt", "acme,10")
    make_pipeline().run()

    result = trace(Selection(step="summarize"))

    steps = _by_step(result)
    assert [n.relation for n in steps["extract"]] == ["upstream"]
    # scan (not extract) is the lineage root — extract has its own upstream,
    # so it doesn't resolve a root payload itself.
    assert [n.relation for n in steps["scan"]] == ["upstream"]
    root = steps["scan"][0]
    # Root resolution reads the stored payload — no auto-indexing involved.
    assert root.root_value == {"path": "a.txt", "text": "acme,10"}
    # Non-roots don't carry payloads.
    assert steps["summarize"][0].root_value is None
    assert steps["extract"][0].root_value is None


def test_trace_live_only_seeding_and_include_superseded():
    create_file("a.txt", "acme,10")
    make_pipeline().run()

    invalidate(Selection(index={"company": "acme"}), reason="test")

    # Default: the invalidated materialization no longer seeds a trace.
    assert trace(Selection(index={"company": "acme"})).nodes == []

    # include_superseded seeds it; traversal still reaches live descendants,
    # and the non-live seed is marked, not hidden.
    result = trace(Selection(index={"company": "acme"}), include_superseded=True)
    steps = _by_step(result)
    assert steps["extract"][0].is_live is False
    assert steps["total"][0].relation == "downstream"


def test_trace_coordinates_present():
    create_file("a.txt", "acme,10")
    make_pipeline().run()

    result = trace(Selection(index={"company": "acme"}))
    coords = {n.step_name: n.coordinate for n in result.nodes}
    # Coordinates are content-addressed (row-<hash>), not "a.txt" — but a
    # 1:1 map chain propagates its parent's coordinate unchanged, so
    # scan/extract/summarize all share one lane coordinate.
    assert coords["extract"] == coords["scan"]
    assert coords["extract"].startswith("row-")
    assert coords["total"] == "@all"
