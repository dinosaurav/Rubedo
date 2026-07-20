"""Run history (Home.runs) and run-to-run diff (Home.diff / RunSummary.diff).

Fixture shape from tests/test_index.py: per-test data + env dirs, never
nested; Home via isolated_test_env.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from rubedo import (
    Filtered,
    RunScope,
    RunSummary,
    pipeline,
    step,
)
from rubedo.diff import resolve_run_id
from rubedo.models import (
    RUN_HEARTBEAT_STALE_SECONDS,
    InputHashUsage,
    Run,
    RunCoordinateStatus,
    RunEvent,
)
from rubedo.util import utcnow_iso
from conftest import isolated_test_env

TEST_FOLDER = ".test_run_diff_data"
ENV_FOLDER = ".test_run_diff_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("run_diff") as env:
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


def _coord_for(path: str, step_name: str = "scan") -> str:
    cells = TEST_HOME.select(f"step:{step_name} path:{path}", resolve_output=True)
    assert cells, f"no lane for path={path}"
    return cells[0].coordinate


def _stale_iso() -> str:
    then = datetime.now(timezone.utc) - timedelta(
        seconds=RUN_HEARTBEAT_STALE_SECONDS + 60
    )
    return then.isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Home.runs
# ---------------------------------------------------------------------------


def test_home_runs_ordering_and_filters_include_partial():
    for i in range(3):
        create_file(f"f{i}.txt", f"body-{i}")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"]), "v": "1"}

    pipe = pipeline(name="runs-hist", steps=[scan, enrich_v1], home=TEST_HOME)
    full_a = pipe.run(workers=1)

    @step
    def other_scan():
        yield {"path": "x", "text": "x"}

    @step
    def other_map(other_scan: dict):
        return other_scan

    other = pipeline(
        name="runs-other",
        steps=[other_scan, other_map],
        home=TEST_HOME,
    ).run(workers=1)

    coord = _coord_for("f0.txt")
    scope = RunScope.explicit(anchor="enrich", lanes=[coord])
    trial = pipe.run(scope=scope, targets=["enrich"], workers=1)

    listed = TEST_HOME.runs(limit=10)
    ids = [item.id for item in listed]
    assert ids == sorted(
        ids,
        key=lambda rid: next(i.started_at for i in listed if i.id == rid),
        reverse=True,
    )
    assert full_a.run_id in ids
    assert trial.run_id in ids
    assert other.run_id in ids

    by_pipe = TEST_HOME.runs(pipeline="runs-hist")
    assert {i.id for i in by_pipe} == {full_a.run_id, trial.run_id}
    assert all(i.pipeline_id == "runs-hist" for i in by_pipe)

    partials = TEST_HOME.runs(pipeline="runs-hist", kind="partial")
    assert [i.id for i in partials] == [trial.run_id]
    assert partials[0].kind == "partial"

    completed = TEST_HOME.runs(pipeline="runs-hist", status="completed")
    assert {i.id for i in completed} == {full_a.run_id, trial.run_id}

    limited = TEST_HOME.runs(pipeline="runs-hist", limit=1)
    assert len(limited) == 1
    assert limited[0].id == trial.run_id

    # Effective status: unfinished → interrupted when heartbeat is stale.
    with TEST_HOME.session() as session:
        session.add(
            Run(
                id="unfinished-run",
                kind="process",
                pipeline_id="runs-hist",
                started_at=_stale_iso(),
                last_heartbeat_at=_stale_iso(),
            )
        )
        session.commit()
    interrupted = TEST_HOME.runs(pipeline="runs-hist", status="interrupted")
    assert [i.id for i in interrupted] == ["unfinished-run"]
    assert interrupted[0].status == "interrupted"

    with pytest.raises(ValueError, match="limit must be >= 1"):
        TEST_HOME.runs(limit=0)


# ---------------------------------------------------------------------------
# Run refs
# ---------------------------------------------------------------------------


def test_run_ref_forms():
    create_file("a.txt", "hello")

    @step(name="enrich", version="1")
    def enrich(scan: dict):
        return {"path": scan["path"], "text": scan["text"]}

    summary = pipeline(
        name="ref-forms", steps=[scan, enrich], home=TEST_HOME
    ).run(workers=1)
    item = TEST_HOME.runs(pipeline="ref-forms", limit=1)[0]

    assert resolve_run_id(summary.run_id) == summary.run_id
    assert resolve_run_id(summary) == summary.run_id
    assert resolve_run_id(item) == summary.run_id

    with pytest.raises(TypeError, match="run ref must be"):
        resolve_run_id(123)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="non-empty"):
        resolve_run_id("")


# ---------------------------------------------------------------------------
# Cohort-aware default + later full union
# ---------------------------------------------------------------------------


def test_partial_trial_defaults_to_cohort_full_uses_union():
    for i in range(4):
        create_file(f"f{i}.txt", f"body-{i}")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"]), "label": "v1"}

    pipe_v1 = pipeline(
        name="cohort-diff", steps=[scan, enrich_v1], home=TEST_HOME
    )
    baseline = pipe_v1.run(workers=1)
    coords = sorted(_coord_for(f"f{i}.txt") for i in range(4))
    sample = coords[:2]

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"]), "label": "v2"}

    pipe_v2 = pipeline(
        name="cohort-diff", steps=[scan, enrich_v2], home=TEST_HOME
    )
    scope = RunScope.explicit(anchor="enrich", lanes=sample)
    trial = pipe_v2.run(scope=scope, targets=["enrich"], workers=1)

    trial_diff = TEST_HOME.diff(
        step="enrich", before=baseline, after=trial
    )
    assert sorted(item.coordinate for item in trial_diff.items) == sorted(sample)
    assert trial_diff.counts["removed"] == 0
    assert trial_diff.counts["added"] == 0
    assert trial_diff.counts["changed"] == 2
    assert trial_diff.counts["unchanged"] == 0

    full_v2 = pipe_v2.run(workers=1)
    full_diff = TEST_HOME.diff(
        step="enrich", before=baseline, after=full_v2
    )
    assert sorted(item.coordinate for item in full_diff.items) == coords
    assert full_diff.counts["changed"] == 4
    assert full_diff.counts["removed"] == 0
    assert full_diff.counts["added"] == 0


def test_missing_scoped_lane_preserved_as_removed():
    import json
    import uuid

    for i in range(3):
        create_file(f"f{i}.txt", f"body-{i}")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"])}

    pipe = pipeline(
        name="missing-scope", steps=[scan, enrich_v1], home=TEST_HOME
    )
    baseline = pipe.run(workers=1)
    coords = [_coord_for(f"f{i}.txt") for i in range(3)]
    # Real 1-lane partial, then an append-only synthetic partial whose
    # selection_json lists the full cohort but only one RCS cell exists —
    # the ledger shape of scope_missing without mutating immutable columns.
    scope = RunScope.explicit(anchor="enrich", lanes=coords[:1])
    trial = pipe.run(scope=scope, targets=["enrich"], workers=1)

    synthetic_id = f"partial-missing-{uuid.uuid4().hex[:8]}"
    with TEST_HOME.session() as session:
        baseline_row = session.query(Run).filter_by(id=baseline.run_id).one()
        trial_cell = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=trial.run_id, step_name="enrich", coordinate=coords[0])
            .one()
        )
        session.add(
            Run(
                id=synthetic_id,
                kind="partial",
                status="completed",
                pipeline_id="missing-scope",
                started_at=utcnow_iso(),
                finished_at=utcnow_iso(),
                last_heartbeat_at=utcnow_iso(),
                definition_json=baseline_row.definition_json,
                selection_json=json.dumps(
                    {
                        "type": "run_scope",
                        "anchor": "enrich",
                        "lanes": coords,
                    },
                    sort_keys=True,
                ),
                summary_json=json.dumps(
                    {
                        "created": 0,
                        "reused": 1,
                        "failed": 0,
                        "blocked": 0,
                        "filtered": 0,
                    }
                ),
            )
        )
        session.add(
            RunCoordinateStatus(
                run_id=synthetic_id,
                pipeline_id="missing-scope",
                step_name="enrich",
                source_id=trial_cell.source_id,
                coordinate=coords[0],
                input_hash=trial_cell.input_hash,
                output_address=trial_cell.output_address,
                status=trial_cell.status,
                created_at=utcnow_iso(),
            )
        )
        session.commit()

    diff = TEST_HOME.diff(step="enrich", before=baseline, after=synthetic_id)
    assert sorted(item.coordinate for item in diff.items) == sorted(coords)
    by_coord = {item.coordinate: item for item in diff.items}
    assert by_coord[coords[0]].outcome == "unchanged"
    assert by_coord[coords[1]].outcome == "removed"
    assert by_coord[coords[2]].outcome == "removed"


# ---------------------------------------------------------------------------
# Identity / value / text / outcomes
# ---------------------------------------------------------------------------


def test_same_output_across_version_bump_is_unchanged():
    create_file("a.txt", "same")
    create_file("b.txt", "same-too")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "n": len(scan["text"])}

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        # Identical payload shape/values — only version (address) changes.
        return {"path": scan["path"], "n": len(scan["text"])}

    baseline = pipeline(
        name="ident-same", steps=[scan, enrich_v1], home=TEST_HOME
    ).run(workers=1)
    after = pipeline(
        name="ident-same", steps=[scan, enrich_v2], home=TEST_HOME
    ).run(workers=1)

    diff = TEST_HOME.diff(step="enrich", before=baseline, after=after)
    assert diff.counts["unchanged"] == 2
    assert diff.counts["changed"] == 0
    for item in diff.items:
        assert item.before_output_address != item.after_output_address
        assert item.before_output_identity == item.after_output_identity


def test_nested_dict_field_add_remove_change_and_text_diff():
    create_file("a.txt", "alpha")
    create_file("b.txt", "line1\nline2\n")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        if scan["path"] == "a.txt":
            return {"path": "a.txt", "meta": {"color": "red", "size": 1}}
        return "line1\nline2\n"

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        if scan["path"] == "a.txt":
            return {
                "path": "a.txt",
                "meta": {"color": "blue", "weight": 2},  # size removed, weight added
            }
        return "line1\nline2 changed\n"

    baseline = pipeline(
        name="value-diff", steps=[scan, enrich_v1], home=TEST_HOME
    ).run(workers=1)
    after = pipeline(
        name="value-diff", steps=[scan, enrich_v2], home=TEST_HOME
    ).run(workers=1)

    diff = TEST_HOME.diff(step="enrich", before=baseline, after=after)
    by_path = {}
    for item in diff.items:
        out = item.after_output if item.after_output is not None else item.before_output
        if isinstance(out, dict):
            by_path[out.get("path") or (item.before_output or {}).get("path")] = item
        else:
            by_path["b.txt"] = item

    nested = by_path["a.txt"]
    assert nested.outcome == "changed"
    change_paths = {c.path: c for c in nested.changes}
    assert change_paths["meta.color"].outcome == "changed"
    assert change_paths["meta.color"].old == "red"
    assert change_paths["meta.color"].new == "blue"
    assert change_paths["meta.size"].outcome == "removed"
    assert change_paths["meta.weight"].outcome == "added"

    text_item = by_path["b.txt"]
    assert text_item.outcome == "changed"
    assert len(text_item.changes) == 1
    assert text_item.changes[0].path == ""
    assert text_item.changes[0].text_diff is not None
    assert text_item.changes[0].text_diff.startswith("--- before\n+++ after\n")
    assert "line2 changed" in text_item.changes[0].text_diff


def test_added_removed_failed_filtered_and_explicit_lanes():
    create_file("keep.txt", "long-enough")
    create_file("drop.txt", "x")
    create_file("fail.txt", "will-fail")

    @step(check_cache=False)
    def live_scan():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"path": name, "text": open(path).read()}

    @step(name="screen", version="1")
    def screen_v1(live_scan: dict):
        if live_scan["path"] == "drop.txt":
            return Filtered(reason="short")
        if live_scan["path"] == "fail.txt":
            return {"path": live_scan["path"], "ok": True}
        return {"path": live_scan["path"], "ok": True}

    baseline = pipeline(
        name="outcomes", steps=[live_scan, screen_v1], home=TEST_HOME
    ).run(workers=1)

    @step(name="screen", version="2")
    def screen_v2(live_scan: dict):
        if live_scan["path"] == "drop.txt":
            return {"path": live_scan["path"], "ok": True}  # filtered → kept
        if live_scan["path"] == "fail.txt":
            raise RuntimeError("boom")
        if live_scan["path"] == "keep.txt":
            return Filtered(reason="now-filtered")
        return {"path": live_scan["path"], "ok": True}

    # New file only present for the after run → added at screen.
    create_file("new.txt", "brand-new")

    after = pipeline(
        name="outcomes", steps=[live_scan, screen_v2], home=TEST_HOME
    ).run(workers=1)

    keep = _coord_for("keep.txt", step_name="live_scan")
    drop = _coord_for("drop.txt", step_name="live_scan")
    fail = _coord_for("fail.txt", step_name="live_scan")
    new_cells = TEST_HOME.select(
        "step:live_scan path:new.txt", run_id=after.run_id, resolve_output=True
    )
    new_coord = new_cells[0].coordinate
    ghost = "row-ghost-before-only"

    diff = TEST_HOME.diff(
        step="screen",
        before=baseline,
        after=after,
        lanes=[keep, drop, fail, new_coord, ghost],
    )
    by = {item.coordinate: item for item in diff.items}
    assert set(by) == {keep, drop, fail, new_coord, ghost}

    assert by[keep].outcome == "changed"  # success → filtered
    assert by[drop].outcome == "changed"  # filtered → success
    assert by[fail].outcome == "failed"
    assert by[new_coord].outcome == "added"
    assert by[ghost].outcome == "removed"

    union = TEST_HOME.diff(step="screen", before=baseline, after=after)
    coords = {item.coordinate for item in union.items}
    assert keep in coords and drop in coords and fail in coords and new_coord in coords
    assert ghost not in coords


def test_parent_propagated_filtered_is_unchanged():
    create_file("drop.txt", "x")

    @step
    def gate(scan: dict):
        return Filtered("not selected")

    @step
    def child(gate: dict):
        return gate

    pipe = pipeline(
        name="propagated-filter-diff",
        steps=[scan, gate, child],
        home=TEST_HOME,
    )
    before = pipe.run(workers=1)
    after = pipe.run(workers=1)

    child_cells = after.cells("child")
    assert len(child_cells) == 1
    assert child_cells[0].status == "filtered"
    assert child_cells[0].output_address is None

    diff = TEST_HOME.diff(step="child", before=before, after=after)
    assert diff.counts["unchanged"] == 1
    assert diff.counts["changed"] == 0


def test_native_arrow_struct_null_fill_keeps_added_removed_semantics():
    create_file("a.txt", "a")
    create_file("b.txt", "b")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        if scan["path"] == "a.txt":
            return {"path": "a.txt", "meta": {"keep": 1, "removed": 2}}
        return {"path": "b.txt", "meta": {"keep": 1, "other": 9}}

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        if scan["path"] == "a.txt":
            return {"path": "a.txt", "meta": {"keep": 2, "added": 3}}
        return {"path": "b.txt", "meta": {"keep": 1, "other": 9}}

    before = pipeline(
        name="struct-field-diff",
        steps=[scan, enrich_v1],
        home=TEST_HOME,
    ).run(workers=1)
    after = pipeline(
        name="struct-field-diff",
        steps=[scan, enrich_v2],
        home=TEST_HOME,
    ).run(workers=1)

    coordinate = _coord_for("a.txt")
    diff = TEST_HOME.diff(
        step="enrich",
        before=before,
        after=after,
        lanes=[coordinate],
    )
    changes = {change.path: change for change in diff.items[0].changes}
    assert changes["meta.keep"].outcome == "changed"
    assert changes["meta.removed"].outcome == "removed"
    assert changes["meta.added"].outcome == "added"


def test_empty_partial_cohort_reports_zero_compared_lanes():
    create_file("a.txt", "a")

    @step
    def enrich(scan: dict):
        return scan

    pipe = pipeline(
        name="empty-cohort-diff",
        steps=[scan, enrich],
        home=TEST_HOME,
    )
    before = pipe.run(workers=1)
    after = pipe.run(
        scope=RunScope.explicit(anchor=enrich, lanes=[]),
        targets=[enrich],
        workers=1,
    )

    diff = TEST_HOME.diff(step="enrich", before=before, after=after)
    assert diff.items == ()
    assert sum(diff.counts.values()) == 0
    assert "empty coordinate universe; compared 0 lanes" in str(diff)


# ---------------------------------------------------------------------------
# Errors + read-only + RunSummary.diff
# ---------------------------------------------------------------------------


def test_diff_errors_different_pipelines_unknown_run_unknown_step():
    create_file("a.txt", "x")

    @step(name="enrich")
    def enrich(scan: dict):
        return scan

    a = pipeline(name="pipe-a", steps=[scan, enrich], home=TEST_HOME).run(
        workers=1
    )

    @step
    def root():
        yield {"v": 1}

    @step
    def map_root(root: dict):
        return root

    b = pipeline(name="pipe-b", steps=[root, map_root], home=TEST_HOME).run(
        workers=1
    )

    with pytest.raises(ValueError, match="different pipelines"):
        TEST_HOME.diff(step="enrich", before=a, after=b)
    with pytest.raises(ValueError, match="unknown run"):
        TEST_HOME.diff(step="enrich", before=a, after="missing-run-id")
    with pytest.raises(ValueError, match="not found"):
        TEST_HOME.diff(step="no-such-step", before=a, after=a)


def test_diff_is_read_only():
    create_file("a.txt", "hello")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "text": scan["text"]}

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        return {"path": scan["path"], "text": scan["text"].upper()}

    before = pipeline(
        name="ro-diff", steps=[scan, enrich_v1], home=TEST_HOME
    ).run(workers=1)
    after = pipeline(
        name="ro-diff", steps=[scan, enrich_v2], home=TEST_HOME
    ).run(workers=1)

    with TEST_HOME.session() as session:
        counts = {
            "runs": session.query(Run).count(),
            "rcs": session.query(RunCoordinateStatus).count(),
            "events": session.query(RunEvent).count(),
            "ihu": session.query(InputHashUsage).count(),
        }

    diff = TEST_HOME.diff(step="enrich", before=before, after=after)
    assert diff.counts["changed"] == 1

    with TEST_HOME.session() as session:
        assert session.query(Run).count() == counts["runs"]
        assert session.query(RunCoordinateStatus).count() == counts["rcs"]
        assert session.query(RunEvent).count() == counts["events"]
        assert session.query(InputHashUsage).count() == counts["ihu"]


def test_run_summary_diff_uses_bound_home():
    create_file("a.txt", "hello")

    @step(name="enrich", version="1")
    def enrich_v1(scan: dict):
        return {"path": scan["path"], "v": 1}

    @step(name="enrich", version="2")
    def enrich_v2(scan: dict):
        return {"path": scan["path"], "v": 2}

    before = pipeline(
        name="summary-diff", steps=[scan, enrich_v1], home=TEST_HOME
    ).run(workers=1)
    after = pipeline(
        name="summary-diff", steps=[scan, enrich_v2], home=TEST_HOME
    ).run(workers=1)

    assert isinstance(before, RunSummary)
    assert before._home is TEST_HOME

    diff = before.diff(after, step="enrich")
    assert diff.before_run_id == before.run_id
    assert diff.after_run_id == after.run_id
    assert diff.counts["changed"] == 1
    assert "Diff summary-diff/enrich" in str(diff)
