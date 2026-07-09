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

from rubedo import Selection, invalidate, run, step, pipeline, trace
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


def make_pipeline():
    """extract (indexed) -> summarize -> total (reduce): a 3-step chain."""

    @step(name="extract", version="1", index=["company"])
    def extract(path):
        company, amount = open(path).read().strip().split(",")
        return {"company": company, "amount": int(amount)}

    @step(name="summarize", version="1", depends_on=["extract"])
    def summarize(extract):
        return {"company": extract["company"], "double": extract["amount"] * 2}

    @step(name="total", version="1", depends_on=["summarize"], shape="reduce")
    def total(summarize):
        return {"sum": sum(v["double"] for v in summarize.values())}

    return pipeline(
        id="tr", name="tr", folder=TEST_FOLDER, steps=[extract, summarize, total]
    )


def _by_step(result):
    return {s: nodes for s, nodes in result.by_step().items()}


def test_trace_downstream_from_indexed_seed():
    create_file("a.txt", "acme,10")
    create_file("b.txt", "globex,5")
    run(make_pipeline())

    result = trace(Selection(index={"company": "acme"}))

    steps = _by_step(result)
    # Seeded at extract; downstream reaches acme's summarize and the reduce.
    assert [n.relation for n in steps["extract"]] == ["seed"]
    assert [n.relation for n in steps["summarize"]] == ["downstream"]
    assert [n.relation for n in steps["total"]] == ["downstream"]
    assert steps["summarize"][0].depth == 1
    assert steps["total"][0].depth == 2
    # globex's extract/summarize lanes are not connected to the acme seed
    # except through the reduce — they must not appear.
    assert len(result.nodes) == 3
    assert all(n.is_live for n in result.nodes)
    # Edges cover the two hops.
    assert len(result.edges) == 2


def test_trace_upstream_resolves_root_payload():
    create_file("a.txt", "acme,10")
    run(make_pipeline())

    result = trace(Selection(step="summarize"))

    steps = _by_step(result)
    assert [n.relation for n in steps["extract"]] == ["upstream"]
    root = steps["extract"][0]
    # Root resolution reads the stored payload — no auto-indexing involved.
    assert root.root_value == {"company": "acme", "amount": 10}
    # Non-roots don't carry payloads.
    assert steps["summarize"][0].root_value is None


def test_trace_live_only_seeding_and_include_superseded():
    create_file("a.txt", "acme,10")
    run(make_pipeline())

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
    run(make_pipeline())

    result = trace(Selection(index={"company": "acme"}))
    coords = {n.step_name: n.coordinate for n in result.nodes}
    assert coords["extract"] == "a.txt"
    assert coords["total"] == "@all"
