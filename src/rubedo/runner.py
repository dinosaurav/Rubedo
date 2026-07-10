"""Orchestration: the public run()/plan() entry points.

The phases live in their own modules — planning.py (decide what to do),
execution.py (run step functions), ledger.py (persist what happened) —
and this module wires them together.

A run is a set of (lane, step) cells driven segment by segment: the topo
order is partitioned into segments (_partition_segments) and every segment
goes through the one segment executor (_run_segment). Under
schedule="broad" (the default) each step is its own segment, so the
executor degenerates to plan-all → execute-all → commit-each — the classic
staged loop. Under schedule="deep", consecutive 1:1 steps share a segment
and each lane advances through them the moment its own inputs commit;
reduce/join/expand and multi-parent maps stay singleton barrier segments.
All ledger writes happen in the main thread — workers only run step
functions.
"""

import concurrent.futures
import json
import os
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import contextlib

import loky

from .db import get_session, init_db
from .execution import _process_decision, _RateLimiter, _RunMemo
from .hashing import hash_json
from .ledger import (
    _commit_execution_result,
    _emit_event,
    _finish_run,
    _record_planned,
    _RunContext,
)
from .models import RUN_HEARTBEAT_INTERVAL_SECONDS, Filtered, Run, RunSummary
from .planning import (
    ROOT_LANE,
    EphemeralRef,  # noqa: F401  (re-exported: part of the runner's public surface)
    MatRef,  # noqa: F401
    StepDecision,  # noqa: F401
    _plan_step,
    _step_accepts_params,
    topological_sort,
)
from .spec import PipelineSpec, StepSpec, definition
from .sources import Source, SourceItem, coerce_source
from .store import init_store
from .util import utcnow_iso

SCHEDULES = ("broad", "deep")


def _init_home(home: str):
    """Point the DB and object store at a custom root for this call.

    An explicit home always wins over RUBEDO_DB_PATH/RUBEDO_HOME env vars
    (same precedence as passing db_path directly to init_db) — it's only
    applied when the caller actually passes home=, so the no-arg default
    path is untouched.
    """
    init_db(db_path=os.path.join(home, "rubedo.sqlite"))
    init_store(home=home)


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


def _resolve_sources(pipeline: PipelineSpec, source) -> Dict[str, Source]:
    """{name: Source} for this run, applying a single-source override."""
    if source is not None:
        if len(pipeline.sources) != 1:
            raise ValueError(
                "source= override is only valid for single-source pipelines"
            )
        return {next(iter(pipeline.sources)): coerce_source(source)}
    return dict(pipeline.sources)


def _run_source_id(sources: Dict[str, Source]) -> str:
    """Combined identity of all a run's sources (one id for a single source)."""
    return ",".join(sorted(s.id for s in sources.values()))


def _source_name_for(pipeline: PipelineSpec, sources: Dict[str, Source], step):
    """The source name a step reads, or None if it reads none.

    Dependent steps, root *expand* steps (themselves sources), and *source-less*
    root maps (which mint their own single lane) read nothing.
    """
    if step.depends_on or step.shape == "expand":
        return None
    if not sources:
        return None
    return step.source if step.source is not None else next(iter(sources))


def _scanned_for(
    step: StepSpec, sname: Optional[str], items_by_source: Dict[str, list]
) -> list:
    """The items feeding one step's plan.

    A source-backed step gets its source's scan; a source-less non-expand root
    mints a single synthetic ROOT_LANE item (its input is a constant, so it
    runs once and then reuses); everything else (dependent steps, root expands)
    gets nothing.
    """
    if sname:
        return items_by_source[sname]
    if not step.depends_on and step.shape != "expand":
        return [SourceItem(coordinate=ROOT_LANE, content_hash=ROOT_LANE)]
    return []


def _resolve_invocation(pipeline: PipelineSpec, source, params):
    """Shared by run() and plan(): source coercion and param validation."""
    if source is not None:
        source = coerce_source(source)

    if pipeline.params_model:
        params = pipeline.params_model.model_validate(params or {}).model_dump(
            mode="json"
        )
    return pipeline, source, params


def _deep_eligible(step: StepSpec) -> bool:
    """Can a lane flow through this step without waiting for its siblings?

    v1: only 1:1 steps — shape="map" with at most one parent (root maps and
    skip_cache utils included; a skip_cache step never executes eagerly
    anyway, its fusion semantics are untouched). reduce/join consume whole
    lane sets (true barriers); expand and multi-parent maps are treated as
    barriers for now (unlockable later).
    """
    return step.shape == "map" and len(step.depends_on) <= 1


def _partition_segments(
    topo_steps: List[StepSpec], schedule: str
) -> List[List[StepSpec]]:
    """Partition the topo order into the segments _run_segment drives.

    broad: every step is a singleton segment — stage-at-a-time, exactly the
    classic staged loop. deep: maximal runs of consecutive deep-eligible
    steps share a segment (lanes advance through them independently);
    barrier steps stay singletons and wait for the whole previous segment.
    """
    if schedule == "broad":
        return [[s] for s in topo_steps]
    segments: List[List[StepSpec]] = []
    current: List[StepSpec] = []
    for s in topo_steps:
        if _deep_eligible(s):
            current.append(s)
        else:
            if current:
                segments.append(current)
                current = []
            segments.append([s])
    if current:
        segments.append(current)
    return segments


def _run_segment(
    session,
    ctx: _RunContext,
    seg_steps: List[StepSpec],
    pipeline: PipelineSpec,
    sources: Dict[str, Source],
    items_by_source: Dict[str, list],
    step_sources: Dict[str, Source],
    params: Optional[dict],
    params_hash: str,
    force: bool,
    workers: Optional[int],
    memo: _RunMemo,
    progress_cb: Optional[Callable[[str, str, str], None]],
) -> None:
    """Drive one segment of the DAG to completion — the one scheduler.

    Head steps (no parent inside the segment) are planned whole, exactly as
    the staged loop did; execute decisions go to per-step pools; every
    completion is committed here in the main thread (execution stays
    DB-free), and committing or plan-resolving a cell immediately plans the
    lane's in-segment consumers — so on a singleton segment this degenerates
    to plan-all → execute-all → commit-each, and on a multi-step (deep)
    segment each lane races ahead through the chain independently.

    Scheduling changes order only: addresses, statuses, and lifecycle rows
    are computed by the same planning/ledger code either way.
    """
    in_segment = {s.name for s in seg_steps}
    consumers: Dict[str, List[StepSpec]] = {}
    for s in seg_steps:
        for dep in s.depends_on:
            if dep in in_segment:
                consumers.setdefault(dep, []).append(s)

    accepts = {s.name: _step_accepts_params(s) for s in seg_steps}
    # Per-step machinery at run scope: one rate limiter per step, shared by
    # every task submission for that step (retries included).
    limiters = {
        s.name: _RateLimiter(*s.rate_limit) if s.rate_limit else None
        for s in seg_steps
    }
    thread_pools: Dict[str, concurrent.futures.ThreadPoolExecutor] = {}
    process_pools: Dict[str, Any] = {}
    in_flight: Dict[concurrent.futures.Future, StepSpec] = {}

    def dispatch(step: StepSpec, decision: StepDecision) -> None:
        # Two layers, on purpose: the thread pool orchestrates the retry
        # loop and the shared rate limiter for every lane; a loky process
        # pool, when the step asks for one, is only where the CPU-bound
        # step body runs. Pools are per step (created on first use) so
        # steps with different executor= use their respective pools.
        tp = thread_pools.get(step.name)
        if tp is None:
            tp = concurrent.futures.ThreadPoolExecutor(
                max_workers=workers or step.workers
            )
            thread_pools[step.name] = tp
        pp = None
        if step.executor == "process":
            pp = process_pools.get(step.name)
            if pp is None:
                pp = loky.ProcessPoolExecutor(max_workers=workers or step.workers)
                process_pools[step.name] = pp
        fut = tp.submit(
            _process_decision,
            step,
            decision,
            step_sources,
            params,
            accepts[step.name],
            params_hash,
            memo,
            limiters[step.name],
            pp,
        )
        in_flight[fut] = step

    def plan_cells(step: StepSpec, lanes: Optional[List[str]]) -> None:
        """Plan a step (whole, or one lane's cell) and act on the decisions."""
        sname = _source_name_for(pipeline, sources, step)
        decisions = _plan_step(
            session,
            step,
            _scanned_for(step, sname, items_by_source),
            ctx.coord_step_mats,
            params_hash,
            force,
            accepts[step.name],
            lanes=lanes,
        )
        _record_planned(session, ctx, step, decisions)
        session.commit()
        if progress_cb:
            for d in decisions:
                if d.action != "execute":
                    progress_cb(step.name, d.coordinate, d.action)
        for d in decisions:
            if d.action == "execute":
                dispatch(step, d)
        if consumers.get(step.name):
            # Cells that resolved without executing (reuse/blocked/filtered
            # markers from _record_planned, EphemeralRefs a skip_cache plan
            # installed) unblock their consumers right away.
            if lanes is None:
                resolved = [
                    c for (c, s) in list(ctx.coord_step_mats) if s == step.name
                ]
            else:
                resolved = [c for c in lanes if (c, step.name) in ctx.coord_step_mats]
            for c in resolved:
                advance(step, c)

    def advance(step: StepSpec, coord: str) -> None:
        """(coord, step) resolved: plan the lane's in-segment consumers."""
        for child in consumers.get(step.name, []):
            plan_cells(child, [coord])

    try:
        # Segment heads: no parent inside the segment, so their inputs are
        # complete (previous segments finished) — plan them whole.
        for s in seg_steps:
            if not any(dep in in_segment for dep in s.depends_on):
                plan_cells(s, None)

        # Completion loop: all ledger writes stay here in the main thread.
        while in_flight:
            done, _ = concurrent.futures.wait(
                in_flight, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                step = in_flight.pop(fut)
                outcomes = fut.result()
                for outcome in outcomes:
                    status = "failed"
                    if outcome.success:
                        if isinstance(outcome.result, Filtered):
                            status = "filtered"
                        else:
                            status = "created"
                    _commit_execution_result(ctx, step, outcome)
                    if progress_cb:
                        progress_cb(step.name, outcome.decision.coordinate, status)
                for outcome in outcomes:
                    if not outcome.is_anchor:
                        advance(step, outcome.decision.coordinate)
    finally:
        for tp in thread_pools.values():
            tp.shutdown(wait=True)
        for pp in process_pools.values():
            pp.shutdown()


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
    source: Optional[Source | str] = None,
    *,
    params: Optional[dict] = None,
    force: bool = False,
    home: Optional[str] = None,
) -> RunPlan:
    """Dry-run: what would run() do, and why — without writing anything.

    "execute" means the step function would run for that coordinate;
    "pending" means the answer depends on an upstream execution whose output
    (and therefore this coordinate's address) is unknowable without running.

    home, if given, points the ledger/object store at a custom root instead
    of the default `.rubedo`/RUBEDO_HOME (see notes/TODO.md item 1).
    """
    from .planning import _code_drift_message

    if home is not None:
        _init_home(home)

    pipeline, source, params = _resolve_invocation(pipeline, source, params)
    sources = _resolve_sources(pipeline, source)
    topo_steps = topological_sort(pipeline)
    items_by_source = {name: src.scan() for name, src in sources.items()}
    params_hash = hash_json(params or {})

    items: List[PlannedCoordinate] = []
    plan_warnings: List[str] = []
    coord_step_mats: Dict[tuple, Any] = {}

    with get_session() as session:
        for step in topo_steps:
            accepts_params = _step_accepts_params(step)
            sname = _source_name_for(pipeline, sources, step)
            decisions = _plan_step(
                session,
                step,
                _scanned_for(step, sname, items_by_source),
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
        pipeline_id=pipeline.id,
        source_id=_run_source_id(sources),
        items=items,
        counts=counts,
        warnings=plan_warnings,
    )


def run(
    pipeline: PipelineSpec,
    source: Optional[Source | str] = None,
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
    custom root instead of the default `.rubedo`/RUBEDO_HOME (see
    notes/TODO.md item 1).

    schedule picks the execution order (never the results — cache identity
    is order-independent): "broad" (default) completes each step across all
    lanes before the next one starts; "deep" lets each lane race ahead
    through consecutive 1:1 steps as soon as its own inputs commit, while
    reduce/join (and, for now, expand and multi-parent maps) still
    synchronize on all lanes.
    """
    pipeline, source, params = _resolve_invocation(pipeline, source, params)
    return run_pipeline(
        pipeline=pipeline,
        source=source,
        params=params,
        workers=workers,
        force=force,
        home=home,
        progress=progress,
        progress_cb=progress_cb,
        schedule=schedule,
    )


def run_pipeline(
    pipeline: PipelineSpec,
    source: Optional[Source | str] = None,
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
        source (Optional[Source | str]): The source data.
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

    sources = _resolve_sources(pipeline, source)
    topo_steps = topological_sort(pipeline)
    step_sources = {
        s.name: sources[_source_name_for(pipeline, sources, s)]
        for s in topo_steps
        if _source_name_for(pipeline, sources, s) is not None
    }
    ctx = _RunContext(
        run_id=f"run_{uuid.uuid4().hex[:12]}",
        pipeline_id=pipeline.id,
        source_id=_run_source_id(sources),
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
                items_by_source = {name: src.scan() for name, src in sources.items()}

                params_hash = hash_json(params or {})
                memo = _RunMemo()

                for seg_steps in _partition_segments(topo_steps, schedule):
                    _run_segment(
                        session,
                        ctx,
                        seg_steps,
                        pipeline,
                        sources,
                        items_by_source,
                        step_sources,
                        params,
                        params_hash,
                        force,
                        workers,
                        memo,
                        progress_cb,
                    )

                return _finish_run(ctx)

            except Exception as e:
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
