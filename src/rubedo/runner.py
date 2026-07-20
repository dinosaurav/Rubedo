"""Orchestration: run()/plan(), the internals `Pipeline.run()`/`Pipeline.plan()`
delegate to (pipeline.py sits above this module and is the public surface).

The phases live in their own modules — planning.py (decide what to do),
execution.py (run step functions), ledger.py (persist what happened),
scheduler.py (the segment machinery: _partition_segments/_run_segment,
broad/deep) — and this module wires them together end to end: build a
Run row, drive every segment, finish/record retention.

All ledger writes happen in the main thread — workers only run step
functions (see scheduler.py's _run_segment, where the completion loop
actually commits).
"""

import json
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence
import contextlib

from .execution import _RunMemo
from .hashing import hash_json
from .home import Home
from .ledger import _emit_event, _finish_run, _RunContext
from .models import RUN_HEARTBEAT_INTERVAL_SECONDS, Run, RunSummary
from .planning import (
    EphemeralRef,  # noqa: F401  (re-exported: part of the runner's public surface)
    MatRef,  # noqa: F401
    StepDecision,  # noqa: F401
    _plan_step,
    _step_accepts_params,
    topological_sort,
)
from .scheduler import SCHEDULES, _partition_segments, _run_segment, _scanned_for
from .scope import (
    RunScope,
    ScopeCounts,
    StepRef,
    coordinate_preserving_scope_steps,
    invocation_selection_json,
    normalize_partial_invocation,
)
from .spec import PipelineSpec, definition
from .util import utcnow_iso


@contextlib.contextmanager
def _run_heartbeat(run_id: str, home: Home):
    """Bump the run's last_heartbeat_at every RUN_HEARTBEAT_INTERVAL_SECONDS
    from a daemon thread, for as long as the run is executing.

    A timer thread rather than bump-on-commit: a single long step (one slow
    LLM call) can go minutes without a ledger write, and the run must not
    read as interrupted while it is merely busy. Beats are best-effort — a
    missed one is harmless, so presence-keeping never kills the work.
    """
    stop = threading.Event()

    def beat():
        while not stop.wait(RUN_HEARTBEAT_INTERVAL_SECONDS):
            try:
                with home.session() as session:
                    hb_run = session.query(Run).filter_by(id=run_id).first()
                    if hb_run is None:
                        return
                    hb_run.last_heartbeat_at = utcnow_iso()  # type: ignore
                    session.commit()
            except Exception:
                pass

    thread = threading.Thread(
        target=beat, name=f"rubedo-heartbeat-{run_id}", daemon=True
    )
    thread.start()
    try:
        yield
    finally:
        stop.set()


def _root_step_names(pipeline: PipelineSpec) -> List[str]:
    """The pipeline's roots (no `depends_on`) — the producers that mint its
    initial lanes, sorted for a stable, deterministic identity string."""
    return sorted(s.name for s in pipeline.steps if not s.depends_on)


def _run_source_id(pipeline: PipelineSpec) -> str:
    """Combined identity of a run's roots (one name for a single root)."""
    return ",".join(_root_step_names(pipeline))


def _resolve_invocation(pipeline: PipelineSpec, params):
    """Shared by run() and plan(): params validation."""
    if pipeline.params_model:
        params = pipeline.params_model.model_validate(params or {}).model_dump(
            mode="json"
        )
    return pipeline, params


def _post_run_retention(
    session, pipeline: PipelineSpec, ctx: _RunContext, summary: RunSummary
) -> None:
    """End-of-run retention: auto-prune when retention= is set, else a cheap
    warn-threshold check. Never raises — storage hygiene must not fail a
    successful run.

    The auto-prune runs only after a successful run (failed runs may have an
    incomplete keep-set) and *skips* — never errors — if another run's
    heartbeat is live (the restore race, trap 3; the current run is excluded).
    """
    from .gc import (
        DEFAULT_WARN_THRESHOLD_BYTES,
        auto_prune,
        cheap_store_bytes,
    )

    try:
        if pipeline.retention is not None:
            if summary.status == "failed":
                return
            report = auto_prune(
                session, pipeline.name, ctx.run_id, pipeline.retention, home=ctx.home
            )
            if report is None:
                _emit_event(
                    session,
                    ctx.run_id,
                    "info",
                    "retention_skipped",
                    pipeline_id=ctx.pipeline_id,
                    message="auto-prune skipped: another run is in flight",
                )
                session.commit()
            elif report.demoted_count or report.reclaimed:
                _emit_event(
                    session,
                    ctx.run_id,
                    "info",
                    "retention_pruned",
                    pipeline_id=ctx.pipeline_id,
                    message=(
                        f"retention={pipeline.retention}: pruned "
                        f"{report.demoted_count} materialization(s), reclaimed "
                        f"{len(report.reclaimed)} object(s) / "
                        f"{report.reclaimed_bytes} bytes"
                    ),
                    data={
                        "demoted": report.demoted_count,
                        "reclaimed_objects": len(report.reclaimed),
                        "reclaimed_bytes": report.reclaimed_bytes,
                    },
                )
                session.commit()
            return

        # Unconfigured: warn once (cheaply) if the store is getting large.
        if cheap_store_bytes(home=ctx.home) > DEFAULT_WARN_THRESHOLD_BYTES:
            msg = (
                f"Object store exceeds "
                f"{DEFAULT_WARN_THRESHOLD_BYTES // (1024 * 1024)} MiB and "
                f"'{pipeline.name}' has no retention= set. Consider "
                f"pipeline(..., retention=N) or `rubedo gc --max-bytes SIZE`."
            )
            print(f"[rubedo] {msg}")
            _emit_event(
                session,
                ctx.run_id,
                "warning",
                "storage_threshold_exceeded",
                pipeline_id=ctx.pipeline_id,
                message=msg,
            )
            session.commit()
    except Exception:
        # Retention is best-effort hygiene; never let it fail a finished run.
        pass


@dataclass
class PlannedCoordinate:
    """A projected action for a single coordinate in a specific step."""
    coordinate: str
    step_name: str
    action: str  # reuse | execute | pending | filtered
    output_address: Optional[str] = None


@dataclass
class RunPlan:
    """The complete dry-run plan for a pipeline execution."""
    pipeline_id: str
    source_id: str
    items: List[PlannedCoordinate]
    counts: Dict[str, int]
    warnings: List[str] = field(default_factory=list)
    # Partial-invocation metadata (absent / None on a full plan).
    kind: str = "process"
    scope: Optional[Dict[str, Any]] = None
    targets: Optional[List[str]] = None
    scope_counts: Optional[Dict[str, Any]] = None

    def __str__(self) -> str:
        kind_bit = f" [{self.kind}]" if self.kind != "process" else ""
        lines = [
            f"Plan for '{self.pipeline_id}' over {self.source_id}{kind_bit}: "
            + ", ".join(f"{v} {k}" for k, v in sorted(self.counts.items()))
        ]
        if self.scope_counts:
            lines.append(
                "  scope: "
                f"requested={self.scope_counts.get('scope_requested', 0)} "
                f"reached={self.scope_counts.get('scope_reached', 0)} "
                f"missing={self.scope_counts.get('scope_missing', 0)}"
            )
        for w in self.warnings:
            lines.append(f"  ! {w}")
        for it in self.items:
            addr = f" @ {it.output_address[:12]}" if it.output_address else ""
            lines.append(f"  {it.action:<8} {it.step_name:<20} {it.coordinate}{addr}")
        return "\n".join(lines)


def plan(
    pipeline: PipelineSpec,
    *,
    params: Optional[dict] = None,
    force: bool = False,
    home: Optional[Home] = None,
    scope: Optional[RunScope] = None,
    targets: Optional[Sequence[StepRef]] = None,
) -> RunPlan:
    """Dry-run: what would run() do, and why — without writing anything.

    "execute" means the step function would run for that coordinate;
    "pending" means the answer depends on an upstream execution whose output
    (and therefore this coordinate's address) is unknowable without running.
    A root *expand* step (a parentless generator) plans as "reuse" for each
    cached child lane if the ROOT_LANE-keyed anchor is present, or one
    "execute" for @root if it isn't (first run) or the step has
    check_cache=False.

    ``scope`` / ``targets`` restrict the plan the same way ``run()`` would:
    ancestors plan normally; at the scope anchor only requested coordinates
    appear (out-of-scope lanes are absent, not filtered); omitted targets'
    downstream steps are not planned. Neither argument enters cache identity.

    home, if given, is the storage root for this plan. When omitted, the
    default `.rubedo`/RUBEDO_HOME home is used.
    """
    from .planning import _code_drift_message

    home = home or Home.default()

    pipeline, params = _resolve_invocation(pipeline, params)
    inv = normalize_partial_invocation(pipeline, scope=scope, targets=targets)
    topo_steps = topological_sort(pipeline)
    if inv.active_steps is not None:
        topo_steps = [s for s in topo_steps if s.name in inv.active_steps]
    params_hash = hash_json(params or {})

    items: List[PlannedCoordinate] = []
    plan_warnings: List[str] = []
    coord_step_mats: Dict[tuple, Any] = {}
    scope_reached: set = set()
    scope_lanes = (
        frozenset(inv.scope.lanes) if inv.scope is not None else None
    )
    scope_anchor = inv.scope.anchor if inv.scope is not None else None
    scoped_steps = (
        coordinate_preserving_scope_steps(pipeline, scope_anchor)
        if scope_anchor is not None
        else set()
    )

    # Cloud lane stores use a durable per-pipeline writer lease. Local lane
    # stores return a no-op context manager. Acquire before the Run row so
    # contention fails without leaving an orphaned run.
    with home.lanes.writer_lease(ctx.pipeline_id, ctx.run_id), home.session() as session:
        for step in topo_steps:
            accepts_params = _step_accepts_params(step)
            lanes = None
            if step.name in scoped_steps:
                assert scope_lanes is not None
                lanes = sorted(scope_lanes)
            decisions = _plan_step(
                session,
                step,
                _scanned_for(step),
                coord_step_mats,
                params_hash,
                force,
                accepts_params,
                lanes=lanes,
                pipeline_id=pipeline.name,
                home=home,
            )
            if scope_anchor is not None and step.name == scope_anchor:
                # A dry plan cannot enumerate children of an executing expand
                # root. The frozen cohort still tells us which anchor cells
                # are requested, but their parent values are unknowable until
                # run time: represent them as pending instead of falsely
                # reporting the historical coordinates as missing.
                represented = {d.coordinate for d in decisions}
                parent_pending = any(
                    value == "pending" and dep in step.depends_on
                    for (_coordinate, dep), value in coord_step_mats.items()
                )
                if parent_pending:
                    assert scope_lanes is not None
                    decisions.extend(
                        StepDecision(coordinate=coordinate, action="pending")
                        for coordinate in sorted(scope_lanes - represented)
                    )
                for d in decisions:
                    scope_reached.add(d.coordinate)
            drifted = sum(1 for d in decisions if d.code_drift)
            if drifted:
                plan_warnings.append(_code_drift_message(step, drifted))

            for d in decisions:
                items.append(
                    PlannedCoordinate(
                        d.coordinate, step.name, d.action, d.output_address
                    )
                )
                if d.action == "reuse":
                    coord_step_mats[(d.coordinate, step.name)] = d.existing
                elif d.action == "filtered":
                    coord_step_mats[(d.coordinate, step.name)] = "filtered"
                else:  # execute or pending: output unknowable until run
                    coord_step_mats[(d.coordinate, step.name)] = "pending"

    counts: Dict[str, int] = {}
    for it in items:
        counts[it.action] = counts.get(it.action, 0) + 1

    scope_counts_dict: Optional[Dict[str, Any]] = None
    scope_meta: Optional[Dict[str, Any]] = None
    if inv.scope is not None:
        missing = sorted(set(inv.scope.lanes) - scope_reached)
        sc = ScopeCounts(
            requested=len(inv.scope.lanes),
            reached=len(scope_reached),
            missing=len(missing),
            missing_lanes=missing,
        )
        scope_counts_dict = sc.as_dict()
        if missing:
            plan_warnings.append(
                f"scope at '{inv.scope.anchor}': {len(missing)} requested "
                f"lane(s) missing (no parent output): {missing[:5]}"
                + ("…" if len(missing) > 5 else "")
            )
        scope_meta = inv.scope.to_invocation_dict(inv.targets)

    return RunPlan(
        pipeline_id=pipeline.name,
        source_id=_run_source_id(pipeline),
        items=items,
        counts=counts,
        warnings=plan_warnings,
        kind="partial" if inv.is_partial else "process",
        scope=scope_meta,
        targets=list(inv.targets) if inv.targets is not None else None,
        scope_counts=scope_counts_dict,
    )


def run(
    pipeline: PipelineSpec,
    *,
    params: Optional[dict] = None,
    workers: Optional[int] = None,
    force: bool = False,
    home: Optional[Home] = None,
    progress: bool = False,
    progress_cb: Optional[Callable[[str, str, str], None]] = None,
    schedule: str = "broad",
    scope: Optional[RunScope] = None,
    targets: Optional[Sequence[StepRef]] = None,
) -> RunSummary:
    """Run a pipeline — the single entry point.

    Params are validated against the pipeline's params_model whenever
    one is declared. home, if given, is the storage root for this run;
    otherwise the default `.rubedo`/RUBEDO_HOME home is used.

    schedule picks the execution order (never the results — cache identity
    is order-independent): "broad" (default) completes each step across all
    lanes before the next one starts; "deep" lets each lane race ahead
    through consecutive 1:1 steps as soon as its own inputs commit, while
    reduce/join (and, for now, expand and multi-parent maps) still
    synchronize on all lanes.

    ``scope`` / ``targets`` select a partial run (``kind='partial'``). Scope
    never enters cache identity; sampled map outputs remain reusable by a
    later full ``kind='process'`` run.
    """
    pipeline, params = _resolve_invocation(pipeline, params)
    return run_pipeline(
        pipeline=pipeline,
        params=params,
        workers=workers,
        force=force,
        home=home,
        progress=progress,
        progress_cb=progress_cb,
        schedule=schedule,
        scope=scope,
        targets=targets,
    )


def declare_pipeline(
    pipeline: PipelineSpec,
    home: Optional[Home] = None,
) -> str:
    """Write a pipeline's definition snapshot to the ledger without running.

    Creates a Run row with kind='declaration', status='completed', and the
    full definition_json (including step source code) so the pipeline is
    visible in the dashboard and CLI before any execution. Returns the
    declaration run ID.
    """
    home = home or Home.default()

    run_id = f"run_{uuid.uuid4().hex[:12]}"
    now = utcnow_iso()
    source_id = _run_source_id(pipeline)

    with home.session() as session:
        session.add(
            Run(
                id=run_id,
                kind="declaration",
                pipeline_id=pipeline.name,
                source_id=source_id,
                params_json=json.dumps({}, sort_keys=True),
                definition_json=json.dumps(definition(pipeline)),
                started_at=now,
                finished_at=now,
                last_heartbeat_at=now,
                status="completed",
                summary_json=json.dumps({
                    "created": 0, "reused": 0, "failed": 0,
                    "blocked": 0, "filtered": 0, "by_step": {},
                }),
            )
        )
        session.commit()

    return run_id


def run_pipeline(
    pipeline: PipelineSpec,
    workers: Optional[int] = None,
    force: bool = False,
    params: Optional[dict] = None,
    home: Optional[Home] = None,
    progress: bool = False,
    progress_cb: Optional[Callable[[str, str, str], None]] = None,
    schedule: str = "broad",
    scope: Optional[RunScope] = None,
    targets: Optional[Sequence[StepRef]] = None,
) -> RunSummary:
    """
    Execute a pipeline by resolving the DAG, evaluating each coordinate, and committing results.

    Args:
        pipeline (PipelineSpec): The pipeline to run.
        workers (Optional[int]): Number of parallel workers to use.
        force (bool): If True, forces re-execution of cached outputs.
        params (Optional[dict]): Run-level parameters.
        home (Optional[Home]): Custom ledger/object-store root, overriding
            the default `.rubedo`/RUBEDO_HOME for this run.
        schedule (str): "broad" (default) stages step by step; "deep"
            pipelines lanes through consecutive 1:1 steps. Order only —
            results and ledger rows are identical either way.
        scope (Optional[RunScope]): Frozen lane cohort at a map anchor.
        targets (Optional[Sequence]): Restrict execution to the ancestor
            closure of these steps. Scope and targets never enter cache
            identity; either one makes this a ``kind='partial'`` run.

    Returns:
        RunSummary: A summary of the executed run.
    """
    if schedule not in SCHEDULES:
        raise ValueError(
            f"schedule must be one of {SCHEDULES}, got {schedule!r}"
        )

    home = home or Home.default()
    inv = normalize_partial_invocation(pipeline, scope=scope, targets=targets)

    from .progress import TerminalProgress

    topo_steps = topological_sort(pipeline)
    if inv.active_steps is not None:
        topo_steps = [s for s in topo_steps if s.name in inv.active_steps]
    ctx = _RunContext(
        run_id=f"run_{uuid.uuid4().hex[:12]}",
        pipeline_id=pipeline.name,
        source_id=_run_source_id(pipeline),
        home=home,
        totals={"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0},
        by_step={
            s.name: {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
            for s in topo_steps
            if not s.skip_cache
        },
    )

    run_kind = "partial" if inv.is_partial else "process"
    selection_json = invocation_selection_json(inv.scope, inv.targets)

    with home.session() as session:
        now = utcnow_iso()
        session.add(
            Run(
                id=ctx.run_id,
                kind=run_kind,
                pipeline_id=ctx.pipeline_id,
                source_id=ctx.source_id,
                params_json=json.dumps(params or {}, sort_keys=True),
                selection_json=selection_json,
                definition_json=json.dumps(definition(pipeline)),
                started_at=now,
                last_heartbeat_at=now,
            )
        )
        _emit_event(
            session,
            ctx.run_id,
            "info",
            "run_started",
            pipeline_id=ctx.pipeline_id,
            message=f"Starting run {ctx.run_id}",
            data={
                "kind": run_kind,
                "scope": json.loads(selection_json) if selection_json else None,
            },
        )
        session.commit()

        progress_cm = TerminalProgress() if progress else contextlib.nullcontext()
        with progress_cm as prog, _run_heartbeat(ctx.run_id, home):
            if prog is not None:
                progress_cb = prog.update

            # Clear read caches at run start; this home's cache is rebuilt on
            # first lookup and is independent of other Home instances.
            home.lanes.clear_read_caches()

            try:
                params_hash = hash_json(params or {})
                memo = _RunMemo(home)

                for seg_steps in _partition_segments(topo_steps, schedule):
                    _run_segment(
                        session,
                        ctx,
                        seg_steps,
                        pipeline,
                        params,
                        params_hash,
                        force,
                        workers,
                        memo,
                        progress_cb,
                        scope=inv.scope,
                    )

                if inv.scope is not None:
                    missing = sorted(set(inv.scope.lanes) - ctx.scope_reached)
                    sc = ScopeCounts(
                        requested=len(inv.scope.lanes),
                        reached=len(ctx.scope_reached),
                        missing=len(missing),
                        missing_lanes=missing,
                    )
                    ctx.scope_counts = sc.as_dict()
                    if missing:
                        _emit_event(
                            session,
                            ctx.run_id,
                            "warning",
                            "scope_lanes_missing",
                            pipeline_id=ctx.pipeline_id,
                            step_name=inv.scope.anchor,
                            message=(
                                f"{len(missing)} requested scope lane(s) at "
                                f"'{inv.scope.anchor}' had no parent output "
                                f"and were not executed"
                            ),
                            data={"missing_lanes": missing},
                        )
                        session.commit()

                # Cloud flushes are immutable segments. Compact while the
                # writer lease is still held, before recording terminal state.
                home.lanes.flush_all()
                home.lanes.compact_pipeline(ctx.pipeline_id)
                summary = _finish_run(ctx)
                _post_run_retention(session, pipeline, ctx, summary)
                return summary

            except Exception as e:
                # Flush whatever completed to disk — per-segment flush already
                # wrote completed segments; this catches the current segment's
                # successfully committed lanes.
                home.lanes.flush_all()
                try:
                    home.lanes.compact_pipeline(ctx.pipeline_id)
                except Exception:
                    # Segments are already durable; preserve the original
                    # execution error and compact on a later successful run.
                    pass
                with home.session() as err_session:
                    err_run = err_session.query(Run).filter_by(id=ctx.run_id).first()
                    if err_run:
                        err_run.status = "failed"  # type: ignore
                        err_run.error_message = traceback.format_exc()  # type: ignore
                        err_run.finished_at = utcnow_iso()  # type: ignore
                        _emit_event(
                            err_session,
                            ctx.run_id,
                            "error",
                            "run_failed",
                            pipeline_id=ctx.pipeline_id,
                            message=str(e),
                        )
                        err_session.commit()
                raise
