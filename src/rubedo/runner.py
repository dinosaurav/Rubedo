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
import os
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import contextlib

from .db import get_session, init_db
from .execution import _RunMemo
from .hashing import hash_json
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
from .spec import PipelineSpec, definition
from .store import init_store
from .util import utcnow_iso


def _init_home(home: str):
    """Point the DB and object store at a custom root for this call.

    An explicit home always wins over RUBEDO_DB_PATH/RUBEDO_HOME env vars
    (same precedence as passing db_path directly to init_db) — it's only
    applied when the caller actually passes home=, so the no-arg default
    path is untouched.
    """
    init_db(db_path=os.path.join(home, "rubedo.sqlite"))
    init_store(home=home)
    from . import lane_store
    lane_store.init_tables(home=home)


@contextlib.contextmanager
def _run_heartbeat(run_id: str):
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
                with get_session() as session:
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
                session, pipeline.name, ctx.run_id, pipeline.retention
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
        if cheap_store_bytes() > DEFAULT_WARN_THRESHOLD_BYTES:
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

    def __str__(self) -> str:
        lines = [
            f"Plan for '{self.pipeline_id}' over {self.source_id}: "
            + ", ".join(f"{v} {k}" for k, v in sorted(self.counts.items()))
        ]
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
    home: Optional[str] = None,
) -> RunPlan:
    """Dry-run: what would run() do, and why — without writing anything.

    "execute" means the step function would run for that coordinate;
    "pending" means the answer depends on an upstream execution whose output
    (and therefore this coordinate's address) is unknowable without running.
    A root *expand* step (a parentless generator) always plans as one
    "execute" — it has no parent to cache its enumeration against, so its
    lanes are unknowable
    until it actually runs (a second `plan()` sees them via the expand
    anchor without re-running the generator).

    home, if given, points the ledger/object store at a custom root instead
    of the default `.rubedo`/RUBEDO_HOME.
    """
    from .planning import _code_drift_message

    if home is not None:
        _init_home(home)

    pipeline, params = _resolve_invocation(pipeline, params)
    topo_steps = topological_sort(pipeline)
    params_hash = hash_json(params or {})

    items: List[PlannedCoordinate] = []
    plan_warnings: List[str] = []
    coord_step_mats: Dict[tuple, Any] = {}

    with get_session() as session:
        for step in topo_steps:
            accepts_params = _step_accepts_params(step)
            decisions = _plan_step(
                session,
                step,
                _scanned_for(step),
                coord_step_mats,
                params_hash,
                force,
                accepts_params,
            )
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

    return RunPlan(
        pipeline_id=pipeline.name,
        source_id=_run_source_id(pipeline),
        items=items,
        counts=counts,
        warnings=plan_warnings,
    )


def run(
    pipeline: PipelineSpec,
    *,
    params: Optional[dict] = None,
    workers: Optional[int] = None,
    force: bool = False,
    home: Optional[str] = None,
    progress: bool = False,
    progress_cb: Optional[Callable[[str, str, str], None]] = None,
    schedule: str = "broad",
) -> RunSummary:
    """Run a pipeline — the single entry point.

    Params are validated against the pipeline's params_model whenever
    one is declared. home, if given, points the ledger/object store at a
    custom root instead of the default `.rubedo`/RUBEDO_HOME.

    schedule picks the execution order (never the results — cache identity
    is order-independent): "broad" (default) completes each step across all
    lanes before the next one starts; "deep" lets each lane race ahead
    through consecutive 1:1 steps as soon as its own inputs commit, while
    reduce/join (and, for now, expand and multi-parent maps) still
    synchronize on all lanes.
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
    )


def declare_pipeline(
    pipeline: PipelineSpec,
    home: Optional[str] = None,
) -> str:
    """Write a pipeline's definition snapshot to the ledger without running.

    Creates a Run row with kind='declaration', status='completed', and the
    full definition_json (including step source code) so the pipeline is
    visible in the dashboard and CLI before any execution. Returns the
    declaration run ID.
    """
    if home is not None:
        _init_home(home)

    init_db()
    init_store()
    from . import lane_store
    lane_store.init_tables()

    run_id = f"run_{uuid.uuid4().hex[:12]}"
    now = utcnow_iso()
    source_id = _run_source_id(pipeline)

    with get_session() as session:
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
    home: Optional[str] = None,
    progress: bool = False,
    progress_cb: Optional[Callable[[str, str, str], None]] = None,
    schedule: str = "broad",
) -> RunSummary:
    """
    Execute a pipeline by resolving the DAG, evaluating each coordinate, and committing results.

    Args:
        pipeline (PipelineSpec): The pipeline to run.
        workers (Optional[int]): Number of parallel workers to use.
        force (bool): If True, forces re-execution of cached outputs.
        params (Optional[dict]): Run-level parameters.
        home (Optional[str]): Custom ledger/object-store root, overriding
            the default `.rubedo`/RUBEDO_HOME for this run.
        schedule (str): "broad" (default) stages step by step; "deep"
            pipelines lanes through consecutive 1:1 steps. Order only —
            results and ledger rows are identical either way.

    Returns:
        RunSummary: A summary of the executed run.
    """
    if schedule not in SCHEDULES:
        raise ValueError(
            f"schedule must be one of {SCHEDULES}, got {schedule!r}"
        )
    if home is not None:
        _init_home(home)

    from .progress import TerminalProgress

    topo_steps = topological_sort(pipeline)
    ctx = _RunContext(
        run_id=f"run_{uuid.uuid4().hex[:12]}",
        pipeline_id=pipeline.name,
        source_id=_run_source_id(pipeline),
        totals={"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0},
        by_step={
            s.name: {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
            for s in topo_steps
            if not s.skip_cache
        },
    )

    with get_session() as session:
        now = utcnow_iso()
        session.add(
            Run(
                id=ctx.run_id,
                kind="process",
                pipeline_id=ctx.pipeline_id,
                source_id=ctx.source_id,
                params_json=json.dumps(params or {}, sort_keys=True),
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
        )
        session.commit()

        progress_cm = TerminalProgress() if progress else contextlib.nullcontext()
        with progress_cm as prog, _run_heartbeat(ctx.run_id):
            if prog is not None:
                progress_cb = prog.update

            try:
                params_hash = hash_json(params or {})
                memo = _RunMemo()

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
                    )

                summary = _finish_run(ctx)
                _post_run_retention(session, pipeline, ctx, summary)
                return summary

            except Exception as e:
                # Drop any half-written lane_store buffers — the rows
                # the run didn't finish committing are not durable.
                # Disk state from prior flushes (this run's completed
                # steps) is left in place; recovery is "next run sees
                # what did flush + retries the rest" (notes/arrow-storage.md).
                from . import lane_store
                lane_store.clear_run_buffers()
                with get_session() as err_session:
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
