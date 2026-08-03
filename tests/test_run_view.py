"""Grouped Run View projection — definition-first scopes + edge lineage."""

from __future__ import annotations

import pytest

from conftest import isolated_test_env
from rubedo import pipeline, step
from rubedo.run_view import (
    PARENT_SCOPE,
    Scope,
    build_run_view,
    derive_step_metas,
    project_run_view,
    step_shape,
    topological_steps,
)

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("run_view", with_data=False) as env:
        TEST_HOME = env.home
        yield


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_step_shape_defaults_and_aliases():
    assert step_shape({}) == "map"
    assert step_shape({"out_shape": "many"}) == "expand"
    assert step_shape({"in_shape": "aggregate"}) == "aggregate"
    assert step_shape({"in_shape": "fold"}) == "aggregate"
    assert step_shape({"in_shape": "join", "out_shape": "many"}) == "join"


def test_topo_and_scope_derivation_nested_expand_aggregate():
    definition = {
        "steps": [
            {"name": "scan", "depends_on": [], "out_shape": "many"},
            {"name": "parse", "depends_on": ["scan"]},
            {"name": "items", "depends_on": ["parse"], "out_shape": "many"},
            {"name": "score", "depends_on": ["items"]},
            {
                "name": "fold",
                "depends_on": ["score"],
                "in_shape": "aggregate",
            },
            {
                "name": "total",
                "depends_on": ["parse"],
                "in_shape": "aggregate",
            },
        ]
    }
    ordered = [s["name"] for s in topological_steps(definition)]
    assert ordered == ["scan", "parse", "items", "score", "fold", "total"]

    metas = {m.name: m for m in derive_step_metas(definition)}
    assert metas["scan"].shape == "expand"
    assert metas["scan"].scope == PARENT_SCOPE
    assert metas["parse"].scope == PARENT_SCOPE
    assert metas["items"].scope == Scope("child", "items")
    assert metas["items"].source_scope == PARENT_SCOPE
    assert metas["score"].scope == Scope("child", "items")
    assert metas["fold"].scope == Scope("summary", "items")
    assert metas["total"].scope == Scope("summary", None)


# ---------------------------------------------------------------------------
# Fixture pipelines → build_run_view
# ---------------------------------------------------------------------------


def test_map_only_root():
    @step
    def seed():
        return {"n": 3}

    @step
    def double(seed: dict):
        return seed["n"] * 2

    pipe = pipeline(name="map_only", steps=[seed, double], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert [s["name"] for s in view["steps"]] == ["seed", "double"]
    assert len(view["groups"]) == 1
    g = view["groups"][0]
    assert g["coordinate"] == "@root"
    assert g["cells"]["seed"]["status"] == "created"
    assert g["cells"]["double"]["status"] == "created"
    assert g["cells"]["double"]["preview"] == 6
    assert g["children"] == []
    assert view["run_summary"] == {}


def test_single_expand_source_with_map():
    @step
    def scan():
        yield {"path": "a", "n": 1}
        yield {"path": "b", "n": 2}

    @step
    def bump(scan: dict):
        return {"path": scan["path"], "n": scan["n"] + 1}

    pipe = pipeline(name="one_expand", steps=[scan, bump], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.created_count == 4  # 2 scan + 2 bump

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert len(view["groups"]) == 2
    paths = set()
    for g in view["groups"]:
        assert "scan" in g["cells"]
        assert "bump" in g["cells"]
        assert g["cells"]["scan"]["coordinate"] == g["coordinate"]
        assert g["children"] == []
        preview = g["cells"]["bump"]["preview"]
        assert isinstance(preview, dict)
        paths.add(preview["path"])
    assert paths == {"a", "b"}


def test_nested_expand_groups_children_under_parent():
    @step
    def source():
        yield {"batch": "A"}
        yield {"batch": "B"}

    @step
    def expand_batch(source: dict):
        n = 2 if source["batch"] == "A" else 1
        for i in range(n):
            yield {"item": f"{source['batch']}_{i}", "batch": source["batch"]}

    @step
    def label(expand_batch: dict):
        return expand_batch["item"].lower()

    pipe = pipeline(
        name="nested_expand",
        steps=[source, expand_batch, label],
        home=TEST_HOME,
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    # 2 source + 3 expand children + 3 label = 8
    assert summary.created_count == 8

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert len(view["groups"]) == 2

    by_batch = {}
    for g in view["groups"]:
        batch = g["cells"]["source"]["preview"]["batch"]
        assert len(g["children"]) == 1
        block = g["children"][0]
        assert block["expand_step"] == "expand_batch"
        by_batch[batch] = block["rows"]

    assert len(by_batch["A"]) == 2
    assert len(by_batch["B"]) == 1
    a_items = {r["cells"]["expand_batch"]["preview"]["item"] for r in by_batch["A"]}
    assert a_items == {"A_0", "A_1"}
    for row in by_batch["A"]:
        assert "label" in row["cells"]
        assert row["cells"]["label"]["preview"] in {"a_0", "a_1"}


def test_expand_plus_aggregate_run_summary():
    @step
    def scan():
        yield {"path": "a", "n": 10}
        yield {"path": "b", "n": 20}

    @step
    def parse(scan: dict):
        return {"path": scan["path"], "n": scan["n"]}

    @step(depends_on=["parse"], in_shape="aggregate")
    def total(parse: dict):
        return sum(v["n"] for v in parse.values())

    pipe = pipeline(
        name="expand_agg", steps=[scan, parse, total], home=TEST_HOME
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert len(view["groups"]) == 2
    # @all aggregate over both parents → run-level summary, not duplicated
    assert "total" in view["run_summary"]
    assert view["run_summary"]["total"]["preview"] == 30
    assert view["run_summary"]["total"]["coordinate"] == "@all"
    for g in view["groups"]:
        assert "total" not in g["summary"]
        assert "total" not in g["cells"]


def test_failed_lane_is_prominent_in_cells():
    @step
    def scan():
        yield {"path": "ok", "n": 1}
        yield {"path": "boom", "n": 2}

    @step
    def risky(scan: dict):
        if scan["path"] == "boom":
            raise ValueError("kaboom")
        return scan["n"]

    pipe = pipeline(name="with_fail", steps=[scan, risky], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert view["totals"]["failed"] == 1
    statuses = {
        g["cells"]["scan"]["preview"]["path"]: g["cells"].get("risky", {}).get("status")
        for g in view["groups"]
    }
    assert statuses["ok"] == "created"
    assert statuses["boom"] == "failed"
    boom = next(
        g for g in view["groups"] if g["cells"]["scan"]["preview"]["path"] == "boom"
    )
    assert boom["cells"]["risky"]["error_message"]
    assert "kaboom" in boom["cells"]["risky"]["error_message"]


def test_cache_reuse_distinguishes_created_vs_reused():
    @step
    def scan():
        yield {"path": "a"}
        yield {"path": "b"}

    @step
    def tag(scan: dict):
        return scan["path"].upper()

    pipe = pipeline(name="reuse_view", steps=[scan, tag], home=TEST_HOME)
    s1 = pipe.run(workers=1)
    assert s1.created_count == 4
    s2 = pipe.run(workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 4

    view = build_run_view(TEST_HOME, s2.run_id)
    assert view is not None
    assert view["totals"]["reused"] == 4
    assert view["totals"]["created"] == 0
    for g in view["groups"]:
        assert g["cells"]["scan"]["status"] == "reused"
        assert g["cells"]["tag"]["status"] == "reused"


def test_project_run_view_is_pure_no_io():
    """Synthetic coords/edges — no Home required."""
    definition = {
        "steps": [
            {"name": "src", "depends_on": [], "out_shape": "many"},
            {"name": "fan", "depends_on": ["src"], "out_shape": "many"},
            {"name": "leaf", "depends_on": ["fan"]},
        ]
    }
    from rubedo.run_view import CoordRecord

    coords = [
        CoordRecord("p1", "src", "created", output_address="a1", preview={"id": 1}),
        CoordRecord("p2", "src", "created", output_address="a2", preview={"id": 2}),
        CoordRecord("c1", "fan", "created", output_address="b1", preview={"x": 1}),
        CoordRecord("c2", "fan", "created", output_address="b2", preview={"x": 2}),
        CoordRecord("c1", "leaf", "reused", output_address="d1", preview="ok"),
        CoordRecord("c2", "leaf", "created", output_address="d2", preview="ok2"),
    ]
    edges = [("a1", "b1"), ("a2", "b2"), ("b1", "d1"), ("b2", "d2")]
    view = project_run_view(definition, coords, edges)
    assert len(view.groups) == 2
    g1 = next(g for g in view.groups if g.coordinate == "p1")
    assert len(g1.children) == 1
    assert g1.children[0].expand_step == "fan"
    assert len(g1.children[0].rows) == 1
    child = g1.children[0].rows[0]
    assert child.coordinate == "c1"
    assert child.cells["fan"].status == "created"
    assert child.cells["leaf"].status == "reused"
