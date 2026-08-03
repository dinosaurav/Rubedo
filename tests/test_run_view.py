"""Grouped Run View projection — multi-root sections + join tables."""

from __future__ import annotations

import csv
import os
import tempfile

import pytest

from conftest import isolated_test_env
from rubedo import pipeline, step
from rubedo.run_view import (
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
    assert metas["scan"].scope == Scope("branch", "scan")
    assert metas["parse"].scope == Scope("branch", "scan")
    assert metas["items"].scope == Scope("child", "items")
    assert metas["items"].source_scope == Scope("branch", "scan")
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
    assert len(view["sections"]) == 1
    sec = view["sections"][0]
    assert sec["kind"] == "branch"
    assert sec["title"] == "seed"
    assert sec["column_steps"] == ["seed", "double"]
    assert len(sec["groups"]) == 1
    g = sec["groups"][0]
    assert g["coordinate"] == "@root"
    assert g["cells"]["double"]["preview"] == 6


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
    assert summary.created_count == 4

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    assert len(view["sections"]) == 1
    groups = view["sections"][0]["groups"]
    assert len(groups) == 2
    paths = {g["cells"]["bump"]["preview"]["path"] for g in groups}
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
    assert summary.created_count == 8

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    sec = view["sections"][0]
    assert len(sec["groups"]) == 2

    by_batch = {}
    for g in sec["groups"]:
        batch = g["cells"]["source"]["preview"]["batch"]
        assert len(g["children"]) == 1
        block = g["children"][0]
        assert block["expand_step"] == "expand_batch"
        by_batch[batch] = block["rows"]

    assert len(by_batch["A"]) == 2
    assert len(by_batch["B"]) == 1


def test_expand_plus_aggregate_section_summary():
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
    sec = view["sections"][0]
    assert len(sec["groups"]) == 2
    assert len(sec["summary"]) == 1
    assert sec["summary"][0]["step_name"] == "total"
    assert sec["summary"][0]["preview"] == 30
    assert view["run_summary"] == []


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
    groups = view["sections"][0]["groups"]
    boom = next(
        g for g in groups if g["cells"]["scan"]["preview"]["path"] == "boom"
    )
    assert boom["cells"]["risky"]["status"] == "failed"
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
    assert s2.reused_count == 4

    view = build_run_view(TEST_HOME, s2.run_id)
    assert view is not None
    assert view["totals"]["reused"] == 4
    for g in view["sections"][0]["groups"]:
        assert g["cells"]["scan"]["status"] == "reused"
        assert g["cells"]["tag"]["status"] == "reused"


def test_project_run_view_is_pure_no_io():
    from rubedo.run_view import CoordRecord

    definition = {
        "steps": [
            {"name": "src", "depends_on": [], "out_shape": "many"},
            {"name": "fan", "depends_on": ["src"], "out_shape": "many"},
            {"name": "leaf", "depends_on": ["fan"]},
        ]
    }
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
    assert len(view.sections) == 1
    g1 = next(g for g in view.sections[0].groups if g.coordinate == "p1")
    assert g1.children[0].expand_step == "fan"
    assert g1.children[0].rows[0].cells["leaf"].status == "reused"


def test_newsroom_multi_root_join_sections():
    """Parallel roots get their own tables; join is separate; both digests show."""
    folder = os.path.join(tempfile.gettempdir(), "rubedo_newsroom_view_test")
    os.makedirs(folder, exist_ok=True)
    for name, header, rows in [
        (
            "feeds.csv",
            ["feed_id", "publisher"],
            [("f1", "TechCorp"), ("f2", "BizWire"), ("f3", "TechCorp")],
        ),
        (
            "publishers.csv",
            ["publisher", "region"],
            [("TechCorp", "US"), ("BizWire", "EU")],
        ),
    ]:
        with open(os.path.join(folder, name), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)

    feed_articles = {
        "f1": ["GPU prices fall", "Chip roadmap leaks"],
        "f2": ["Markets rally", "IPO filed"],
        "f3": ["New language ships", "Framework 2.0 lands"],
    }

    p = pipeline(name="newsroom_view", home=TEST_HOME)

    @p.step
    def feeds():
        with open(os.path.join(folder, "feeds.csv")) as f:
            for row in csv.DictReader(f):
                yield row

    @p.step
    def publishers():
        with open(os.path.join(folder, "publishers.csv")) as f:
            for row in csv.DictReader(f):
                yield row

    @p.step
    def feed(feeds: dict):
        return {"feed_id": feeds["feed_id"], "publisher": feeds["publisher"]}

    @p.step
    def publisher(publishers: dict):
        return {
            "publisher": publishers["publisher"],
            "region": publishers["region"],
        }

    @p.step(join_on={"feed": "publisher", "publisher": "publisher"})
    def feed_meta(feed: dict, publisher: dict):
        return {"feed_id": feed["feed_id"], "region": publisher["region"]}

    @p.step
    def articles(feed_meta: dict):
        for title in feed_articles[feed_meta["feed_id"]]:
            yield {"title": title, "region": feed_meta["region"]}

    @p.step(group_key="region")
    def digest(articles: dict):
        titles = sorted(a["title"] for a in articles.values())
        return {"count": len(titles), "headlines": titles}

    summary = p.run(workers=1)
    assert summary.failed_count == 0

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    by_id = {s["id"]: s for s in view["sections"]}
    assert set(by_id) == {"feeds", "publishers", "feed_meta"}

    feeds_sec = by_id["feeds"]
    assert feeds_sec["kind"] == "branch"
    assert feeds_sec["column_steps"] == ["feeds", "feed"]
    assert len(feeds_sec["groups"]) == 3
    for g in feeds_sec["groups"]:
        assert "feeds" in g["cells"] and "feed" in g["cells"]
        assert "publishers" not in g["cells"]
        # Join is its own section — not nested under feeds.
        assert g["children"] == []

    pubs_sec = by_id["publishers"]
    assert pubs_sec["kind"] == "branch"
    assert pubs_sec["column_steps"] == ["publishers", "publisher"]
    assert len(pubs_sec["groups"]) == 2
    regions = {g["cells"]["publisher"]["preview"]["region"] for g in pubs_sec["groups"]}
    assert regions == {"US", "EU"}

    join_sec = by_id["feed_meta"]
    assert join_sec["kind"] == "join"
    assert "feed_meta" in join_sec["column_steps"]
    assert len(join_sec["groups"]) == 3
    # Each join row nests an articles expand with 2 children.
    for g in join_sec["groups"]:
        assert len(g["children"]) == 1
        assert g["children"][0]["expand_step"] == "articles"
        assert len(g["children"][0]["rows"]) == 2

    # Both region digests appear — not collapsed / dropped.
    digests = join_sec["summary"]
    assert {c["coordinate"] for c in digests} == {"US", "EU"}
    by_region = {c["coordinate"]: c["preview"] for c in digests}
    assert by_region["US"]["count"] == 4
    assert by_region["EU"]["count"] == 2


def test_post_aggregate_maps_form_fold_table():
    """Maps after an aggregate continue as a normal table, not summary chips."""

    @step
    def items():
        yield {"n": 1}
        yield {"n": 2}

    @step
    def bump(items: dict):
        return items["n"] * 10

    @step(depends_on=["bump"], in_shape="aggregate")
    def total(bump: dict):
        return sum(bump.values())

    @step
    def label(total: int):
        return f"sum={total}"

    @step
    def shout(label: str):
        return label.upper()

    pipe = pipeline(
        name="post_agg",
        steps=[items, bump, total, label, shout],
        home=TEST_HOME,
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0

    view = build_run_view(TEST_HOME, summary.run_id)
    assert view is not None
    by_id = {s["id"]: s for s in view["sections"]}
    assert "items" in by_id
    assert "fold:total" in by_id

    # Aggregate alone is not a summary chip when a fold table exists.
    assert by_id["items"]["summary"] == []
    assert view["run_summary"] == []

    fold = by_id["fold:total"]
    assert fold["kind"] == "fold"
    assert fold["column_steps"] == ["total", "label", "shout"]
    assert len(fold["groups"]) == 1
    row = fold["groups"][0]
    assert row["coordinate"] == "@all"
    assert row["cells"]["total"]["preview"] == 30
    assert row["cells"]["label"]["preview"] == "sum=30"
    assert row["cells"]["shout"]["preview"] == "SUM=30"

    metas = {s["name"]: s for s in view["steps"]}
    assert metas["label"]["scope"] == {"kind": "fold", "expand_step": "total"}
    assert metas["shout"]["scope"] == {"kind": "fold", "expand_step": "total"}
