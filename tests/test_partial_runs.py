"""Partial execution / sampling MVP.

Fixture shape from tests/test_index.py: per-test data + env dirs, never
nested; Home via isolated_test_env.
"""

from __future__ import annotations

import json
import os

import pytest

from rubedo import RunScope, pipeline, step
from rubedo.gc import _retention_demote_addresses, retention_policies
from rubedo.models import Run, RunCoordinateStatus
from rubedo.scope import sample_fraction_coordinates, sample_n_coordinates
from conftest import isolated_test_env, make_home

TEST_FOLDER = ".test_partial_data"
ENV_FOLDER = ".test_partial_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("partial") as env:
        TEST_HOME = env.home
        yield


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step
def scan():
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def _enrich_step(version: str):
    @step(name="enrich", version=version)
    def enrich(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"]), "v": version}

    return enrich


def _tag_step():
    @step(name="tag")
    def tag(enrich: dict):
        return {"path": enrich["path"], "tagged": enrich["n"]}

    return tag


def _coord_for(path: str, step_name: str = "scan") -> str:
    cells = TEST_HOME.select(f"step:{step_name} path:{path}", resolve_output=True)
    assert cells, f"no lane for path={path}"
    return cells[0].coordinate


def _rcs(run_id, *, step=None, home=None):
    home = home or TEST_HOME
    with home.session() as session:
        q = session.query(RunCoordinateStatus).filter_by(run_id=run_id)
        if step is not None:
            q = q.filter_by(step_name=step)
        return q.all()


def _run_row(run_id, home=None):
    home = home or TEST_HOME
    with home.session() as session:
        return session.query(Run).filter_by(id=run_id).one()


# ---------------------------------------------------------------------------
# 1 + 2: scoped map executes N; full reuses; addresses identical
# ---------------------------------------------------------------------------


def test_scoped_map_executes_n_then_full_reuses():
    for i in range(5):
        create_file(f"f{i}.txt", f"body-{i}")

    # Seed scan lanes with a v1 enrich so candidates exist.
    pipe_v1 = pipeline(
        name="partial-reuse",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    baseline = pipe_v1.run(workers=1)
    scan_cells = baseline.cells("scan", resolve_output=True)
    assert len(scan_cells) == 5

    # Trial v2 on a 2-lane cohort.
    enrich_v2 = _enrich_step("2")
    pipe_v2 = pipeline(
        name="partial-reuse",
        steps=[scan, enrich_v2, _tag_step()],
        home=TEST_HOME,
    )
    scope = RunScope.sample_n(
        anchor=enrich_v2, cells=scan_cells, n=2, seed="pilot"
    )
    assert len(scope.lanes) == 2

    trial = pipe_v2.run(scope=scope, targets=["enrich"], workers=1)
    assert trial.kind == "partial"
    assert trial.scope_requested == 2
    assert trial.scope_reached == 2
    assert trial.scope_missing == 0
    enrich_rcs = _rcs(trial.run_id, step="enrich")
    assert len(enrich_rcs) == 2
    assert {r.status for r in enrich_rcs} == {"created"}
    assert _rcs(trial.run_id, step="tag") == []
    assert _run_row(trial.run_id).kind == "partial"
    addrs_trial = {r.output_address for r in enrich_rcs}

    full = pipe_v2.run(workers=1)
    assert full.kind == "process"
    assert _run_row(full.run_id).kind == "process"
    enrich_full = _rcs(full.run_id, step="enrich")
    assert len(enrich_full) == 5
    reused = [r for r in enrich_full if r.output_address in addrs_trial]
    assert len(reused) == 2
    assert all(r.status == "reused" for r in reused)
    assert sum(1 for r in enrich_full if r.status == "created") == 3


def test_scoped_and_unscoped_addresses_identical():
    create_file("a.txt", "alpha")
    create_file("b.txt", "beta")
    pipe = pipeline(
        name="addr-parity",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    full = pipe.run(workers=1)
    coord_a = _coord_for("a.txt")
    addr_full = next(
        r.output_address
        for r in _rcs(full.run_id, step="enrich")
        if r.coordinate == coord_a
    )

    scope = RunScope.explicit(anchor="enrich", lanes=[coord_a])
    trial = pipe.run(scope=scope, targets=["enrich"], workers=1)
    addr_scoped = _rcs(trial.run_id, step="enrich")[0].output_address
    assert addr_scoped == addr_full


# ---------------------------------------------------------------------------
# 3: sampled aggregate address differs from full
# ---------------------------------------------------------------------------


def test_sampled_aggregate_address_differs_from_full():
    for i in range(4):
        create_file(f"r{i}.txt", f"x{i}")

    @step
    def parse(scan: dict):
        return {"path": scan["path"], "n": 1}

    @step(name="total", depends_on=["parse"], shape="reduce")
    def total(parse: dict):
        return {"sum": sum(v["n"] for v in parse.values())}

    pipe = pipeline(name="agg-sample", steps=[scan, parse, total], home=TEST_HOME)
    full = pipe.run(workers=1)
    full_addr = _rcs(full.run_id, step="total")[0].output_address

    scan_cells = full.cells("scan")
    scope = RunScope.sample_n(anchor="parse", cells=scan_cells, n=2, seed="agg")
    trial = pipe.run(scope=scope, targets=["total"], workers=1)
    trial_addr = _rcs(trial.run_id, step="total")[0].output_address
    assert trial_addr != full_addr
    assert len(_rcs(trial.run_id, step="parse")) == 2


# ---------------------------------------------------------------------------
# 4: out-of-scope → no RCS, no filtered count
# ---------------------------------------------------------------------------


def test_out_of_scope_no_rcs_no_filtered_count():
    for i in range(4):
        create_file(f"o{i}.txt", f"o{i}")

    pipe_v1 = pipeline(
        name="oos",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    seed = pipe_v1.run(workers=1)
    coords = sorted({c.coordinate for c in seed.cells("scan")})

    pipe_v2 = pipeline(
        name="oos",
        steps=[scan, _enrich_step("2"), _tag_step()],
        home=TEST_HOME,
    )
    scope = RunScope.explicit(anchor="enrich", lanes=coords[:1])
    trial = pipe_v2.run(scope=scope, targets=["enrich"], workers=1)
    assert trial.filtered_count == 0
    assert len(_rcs(trial.run_id, step="enrich")) == 1
    assert len(_rcs(trial.run_id, step="scan")) == 4


# ---------------------------------------------------------------------------
# 5: targets omit downstream RCS
# ---------------------------------------------------------------------------


def test_targets_omit_downstream_rcs():
    create_file("t.txt", "hello")
    pipe = pipeline(
        name="targets",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    pipe.run(workers=1)
    coord = _coord_for("t.txt")
    scope = RunScope.explicit(anchor="enrich", lanes=[coord])
    trial = pipe.run(scope=scope, targets=["enrich"], workers=1)
    steps_seen = {r.step_name for r in _rcs(trial.run_id)}
    assert "tag" not in steps_seen
    assert "enrich" in steps_seen


# ---------------------------------------------------------------------------
# 6: broad / deep parity
# ---------------------------------------------------------------------------


def test_broad_deep_parity_partial():
    for i in range(3):
        create_file(f"p{i}.txt", f"p{i}")

    home_b = make_home(os.path.join(os.path.abspath(ENV_FOLDER), "broad"))
    home_d = make_home(os.path.join(os.path.abspath(ENV_FOLDER), "deep"))

    def build_steps(version: str):
        @step(check_cache=False)
        def scan_h():
            for name in sorted(os.listdir(TEST_FOLDER)):
                path = os.path.join(TEST_FOLDER, name)
                if os.path.isfile(path):
                    yield {"path": name, "text": open(path).read()}

        @step(name="enrich", version=version)
        def enrich(scan_h: dict):
            return {"path": scan_h["path"], "n": len(scan_h["text"]), "v": version}

        @step(name="tag")
        def tag(enrich: dict):
            return enrich

        return [scan_h, enrich, tag]

    seed_b = pipeline(
        name="parity", steps=build_steps("1"), schedule="broad", home=home_b
    ).run(workers=1)
    seed_d = pipeline(
        name="parity", steps=build_steps("1"), schedule="deep", home=home_d
    ).run(workers=1)
    lanes = sorted({c.coordinate for c in seed_b.cells("scan_h")})[:2]
    assert lanes == sorted({c.coordinate for c in seed_d.cells("scan_h")})[:2]

    def trial(home, schedule):
        steps = build_steps("9")
        pipe = pipeline(
            name="parity", steps=steps, schedule=schedule, home=home
        )
        scope = RunScope.explicit(anchor="enrich", lanes=lanes)
        return pipe.run(scope=scope, targets=["enrich"], workers=1)

    tb = trial(home_b, "broad")
    td = trial(home_d, "deep")

    def facts(home, run_id):
        with home.session() as session:
            rows = (
                session.query(RunCoordinateStatus).filter_by(run_id=run_id).all()
            )
            return {
                (r.step_name, r.coordinate, r.output_address, r.status)
                for r in rows
            }

    assert facts(home_b, tb.run_id) == facts(home_d, td.run_id)


# ---------------------------------------------------------------------------
# 7: exact cohort persisted
# ---------------------------------------------------------------------------


def test_exact_cohort_persisted_in_selection_json():
    for i in range(3):
        create_file(f"c{i}.txt", f"c{i}")
    pipe = pipeline(
        name="cohort",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    seed = pipe.run(workers=1)
    lanes = sorted({c.coordinate for c in seed.cells("scan")})[:2]
    scope = RunScope.sample_n(
        anchor="enrich", lanes=lanes, n=2, seed="persist-me"
    )
    trial = pipe.run(scope=scope, targets=["enrich"], workers=1)
    payload = json.loads(_run_row(trial.run_id).selection_json)
    assert payload["type"] == "run_scope"
    assert payload["anchor"] == "enrich"
    assert payload["lanes"] == sorted(lanes)
    assert payload["targets"] == ["enrich"]
    assert payload["origin"]["strategy"] == "sample_n"
    assert payload["origin"]["seed"] == "persist-me"


# ---------------------------------------------------------------------------
# 8: current remains latest full after partial
# ---------------------------------------------------------------------------


def test_current_remains_latest_full_after_partial():
    create_file("cur.txt", "x")
    pipe_v1 = pipeline(
        name="cur-pipe",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    pipe_v1.run(workers=1)
    full_enrich = {
        c.coordinate: c.output_address
        for c in TEST_HOME.current(pipeline="cur-pipe", step="enrich")
    }
    assert len(full_enrich) == 1
    coord = next(iter(full_enrich))

    pipe_v2 = pipeline(
        name="cur-pipe",
        steps=[scan, _enrich_step("99"), _tag_step()],
        home=TEST_HOME,
    )
    scope = RunScope.explicit(anchor="enrich", lanes=[coord])
    trial = pipe_v2.run(scope=scope, targets=["enrich"], workers=1)
    assert _run_row(trial.run_id).kind == "partial"

    current = TEST_HOME.current(pipeline="cur-pipe", step="enrich")
    assert {c.output_address for c in current} == set(full_enrich.values())


# ---------------------------------------------------------------------------
# 9: retention latest-full safety
# ---------------------------------------------------------------------------


def test_retention_protects_latest_full_against_partial():
    create_file("ret.txt", "keep-me")

    @step(check_cache=False)
    def scan_bytes():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield open(path).read().encode("utf-8")

    @step(name="upper", version="1")
    def upper(scan_bytes):
        return scan_bytes.upper()

    pipe = pipeline(
        name="ret-partial",
        steps=[scan_bytes, upper],
        retention=1,
        home=TEST_HOME,
    )
    full = pipe.run(workers=1)
    full_addrs = {
        r.output_address for r in _rcs(full.run_id) if r.output_address
    }
    coord = _rcs(full.run_id, step="upper")[0].coordinate

    @step(name="upper", version="2")
    def upper2(scan_bytes):
        return scan_bytes.upper() + b"!"

    pipe2 = pipeline(
        name="ret-partial",
        steps=[scan_bytes, upper2],
        retention=1,
        home=TEST_HOME,
    )
    scope = RunScope.explicit(anchor="upper", lanes=[coord])
    pipe2.run(scope=scope, targets=["upper"], workers=1)

    with TEST_HOME.session() as session:
        policies = retention_policies(session)
        demote = _retention_demote_addresses(
            session, TEST_HOME, policies, set()
        )
    assert full_addrs.isdisjoint(demote)


# ---------------------------------------------------------------------------
# 10: invalid anchors / targets
# ---------------------------------------------------------------------------


def test_invalid_anchors_and_targets():
    create_file("inv.txt", "z")

    @step
    def parse(scan: dict):
        return scan

    @step(name="sum", depends_on=["parse"], shape="reduce")
    def sum_step(parse: dict):
        return {"n": len(parse)}

    @step(skip_cache=True)
    def util(parse: dict):
        return parse

    @step
    def after(util: dict):
        return util

    pipe = pipeline(
        name="invalid",
        steps=[scan, parse, sum_step, util, after],
        home=TEST_HOME,
    )
    pipe.run(workers=1)
    coord = _coord_for("inv.txt")

    with pytest.raises(ValueError, match="root"):
        pipe.run(scope=RunScope.explicit(anchor="scan", lanes=[coord]))

    with pytest.raises(ValueError, match="aggregate|in_shape|map anchors"):
        pipe.run(scope=RunScope.explicit(anchor="sum", lanes=["@all"]))

    with pytest.raises(ValueError, match="skip_cache"):
        pipe.run(scope=RunScope.explicit(anchor="util", lanes=[coord]))

    with pytest.raises(ValueError, match="unknown target"):
        pipe.run(
            scope=RunScope.explicit(anchor="parse", lanes=[coord]),
            targets=["nope"],
        )

    with pytest.raises(ValueError, match="not the anchor|descendants"):
        pipe.run(
            scope=RunScope.explicit(anchor="after", lanes=[coord]),
            targets=["sum"],
        )


# ---------------------------------------------------------------------------
# 11: deterministic sample helpers + nested fractions
# ---------------------------------------------------------------------------


def test_deterministic_sample_helpers_and_nested_fractions():
    coords = [f"row-{i:03d}" for i in range(200)]
    a = sample_n_coordinates(coords, n=10, seed="s")
    b = sample_n_coordinates(coords, n=10, seed="s")
    assert a == b
    assert len(a) == 10
    assert sample_n_coordinates(coords, n=10, seed="other") != a

    small = set(sample_fraction_coordinates(coords, fraction=0.05, seed="nest"))
    large = set(sample_fraction_coordinates(coords, fraction=0.25, seed="nest"))
    assert small <= large
    assert len(small) < len(large)

    scope = RunScope.sample_fraction(
        anchor="enrich", lanes=coords, fraction=0.1, seed="nest"
    )
    assert set(scope.lanes) == set(
        sample_fraction_coordinates(coords, fraction=0.1, seed="nest")
    )


# ---------------------------------------------------------------------------
# 12: plan read-only and scoped
# ---------------------------------------------------------------------------


def test_plan_read_only_and_scoped():
    for i in range(3):
        create_file(f"pl{i}.txt", f"pl{i}")
    pipe = pipeline(
        name="plan-scope",
        steps=[scan, _enrich_step("1"), _tag_step()],
        home=TEST_HOME,
    )
    seed = pipe.run(workers=1)
    with TEST_HOME.session() as session:
        runs_before = session.query(Run).count()
        rcs_before = session.query(RunCoordinateStatus).count()

    lanes = sorted({c.coordinate for c in seed.cells("scan")})[:1]
    scope = RunScope.explicit(anchor="enrich", lanes=lanes)
    plan = pipe.plan(scope=scope, targets=["enrich"])
    assert plan.kind == "partial"
    assert plan.targets == ["enrich"]
    assert plan.scope_counts["scope_requested"] == 1
    assert plan.scope_counts["scope_reached"] == 1
    assert {it.step_name for it in plan.items} <= {"scan", "enrich"}
    assert "tag" not in {it.step_name for it in plan.items}
    enrich_items = [it for it in plan.items if it.step_name == "enrich"]
    assert len(enrich_items) == 1
    assert enrich_items[0].coordinate == lanes[0]

    with TEST_HOME.session() as session:
        assert session.query(Run).count() == runs_before
        assert session.query(RunCoordinateStatus).count() == rcs_before


def test_plan_scope_stays_pending_when_source_must_enumerate():
    for i in range(2):
        create_file(f"pending{i}.txt", f"pending{i}")

    @step(check_cache=False)
    def live_scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step
    def classify(live_scan: dict):
        return live_scan

    pipe = pipeline(
        name="plan-pending-scope",
        steps=[live_scan, classify],
        home=TEST_HOME,
    )
    baseline = pipe.run(workers=1)
    scope = RunScope.sample_n(
        anchor=classify,
        cells=baseline.cells("live_scan"),
        n=1,
        seed="pending",
    )

    plan = pipe.plan(scope=scope, targets=[classify])

    classify_items = [item for item in plan.items if item.step_name == "classify"]
    assert [(item.coordinate, item.action) for item in classify_items] == [
        (next(iter(scope.lanes)), "pending")
    ]
    assert plan.scope_counts == {
        "scope_requested": 1,
        "scope_reached": 1,
        "scope_missing": 0,
        "missing_lanes": [],
    }
