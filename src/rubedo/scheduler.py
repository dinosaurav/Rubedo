"""Segment machinery: the one scheduler that drives (lane, step) cells.

All ledger writes happen in the main thread — workers only run step
functions (execution.py stays DB-free; planning/committing happen here,
called back into from the completion loop below). This module holds the
segment machinery; run/plan orchestration stays in runner.py, which drives
`_partition_segments`/`_run_segment` per run.

A run is a set of (lane, step) cells driven segment by segment: the topo
order is partitioned into segments (_partition_segments) and every segment
goes through the one segment executor (_run_segment). Under
schedule="broad" (the default) each step is its own segment, so the
executor degenerates to plan-all -> execute-all -> commit-each — the classic
staged loop. Under schedule="deep", consecutive deep-eligible steps share a
segment and each lane advances through them the moment its own inputs
commit; aggregate/join/dependent-expand and multi-parent maps stay singleton
barrier segments. Root expands are deep-eligible — independent sources run
concurrently within the same segment.
"""

import concurrent.futures
import os
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

import loky

from .execution import _process_decision, _RateLimiter, _RunMemo
from .ledger import _commit_execution_result, _record_planned, _RunContext
from .models import Filtered
from .planning import ROOT_LANE, RootItem, StepDecision, _plan_step, _step_accepts_params
from .spec import PipelineSpec, StepSpec

if TYPE_CHECKING:
    from .scope import RunScope

SCHEDULES = ("broad", "deep")

#: How many planned ledger rows to accumulate before ``session.commit``.
#: Deep reuse used to commit once per lane (a fsync storm). ``1`` restores
#: that cadence. Execute decisions always flush immediately so a nested
#: ``_commit_execution_result`` writer cannot deadlock SQLite.
_DEFAULT_LEDGER_BATCH = 128


def _ledger_batch_size() -> int:
    raw = (os.environ.get("RUBEDO_LEDGER_BATCH") or str(_DEFAULT_LEDGER_BATCH)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = _DEFAULT_LEDGER_BATCH
    return max(1, n)


def _call_progress(
    cb: Optional[Callable[..., None]],
    step_name: str,
    coordinate: str,
    status: str,
    output: Any = None,
    content_type: Optional[str] = None,
) -> None:
    """Fire ``progress_cb``. Extra payload args are optional.

    3-arg callbacks (tests, ``rubedo serve``) keep working. Product
    engines accept ``(step, coord, status, output, content_type)`` so
    they can skip a nested ledger read — planning already has the Arrow
    value on reuse.
    """
    if cb is None:
        return
    try:
        cb(step_name, coordinate, status, output, content_type)
    except TypeError:
        cb(step_name, coordinate, status)


def _shutdown_worker_pool(pool: Any) -> None:
    """Shut down a process/external pool created by this segment."""
    shutdown = getattr(pool, "shutdown", None)
    if callable(shutdown):
        shutdown(wait=True)
        return
    close = getattr(pool, "close", None)
    if callable(close):
        close()


def _scanned_for(step: StepSpec) -> List[RootItem]:
    """The synthetic items feeding a source-less root's plan.

    A root step (no depends_on) mints a single synthetic '@root' item —
    its input is a constant (map) or the generator's own yields (expand),
    so it runs once and then reuses via the ROOT_LANE-keyed anchor. Shared
    by runner.plan() and _run_segment below.
    """
    if not step.depends_on:
        return [RootItem(coordinate=ROOT_LANE, content_hash=ROOT_LANE)]
    return []


def _deep_eligible(step: StepSpec) -> bool:
    """Can a lane flow through this step without waiting for its siblings?

    1:1 map steps (including root maps and use_cache=False utils) and root
    expands (independent sources that yield their own lanes). aggregate/join
    consume whole lane sets (true barriers).
    """
    if step.shape in ("map", "expand") and len(step.depends_on) <= 1:
        return True
    return False


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
    params: Optional[dict],
    params_hash: str,
    force: bool,
    workers: Optional[int],
    memo: _RunMemo,
    progress_cb: Optional[Callable[[str, str, str], None]],
    scope: Optional["RunScope"] = None,
) -> None:
    """Drive one segment of the DAG to completion — the one scheduler.

    Head steps (no parent inside the segment) are planned whole, exactly as
    the staged loop did; execute decisions go to per-step pools; every
    completion is committed here in the main thread (execution stays
    DB-free), and committing or plan-resolving a cell immediately plans the
    lane's in-segment consumers — so on a singleton segment this degenerates
    to plan-all -> execute-all -> commit-each, and on a multi-step (deep)
    segment each lane races ahead through the chain independently.

    Scheduling changes order only: addresses, statuses, and lifecycle rows
    are computed by the same planning/ledger code either way.

    When ``scope`` is set, the anchor step plans *only* requested
    coordinates (via ``_plan_step(..., lanes=...)``). Out-of-scope lanes are
    absent — no ``filtered`` decisions and no ``RunCoordinateStatus`` rows.
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
    worker_pools: Dict[str, Any] = {}
    in_flight: Dict[concurrent.futures.Future, StepSpec] = {}
    scope_lanes = frozenset(scope.lanes) if scope is not None else None
    scope_anchor = scope.anchor if scope is not None else None
    if scope_anchor is not None:
        from .scope import coordinate_preserving_scope_steps

        scoped_steps = coordinate_preserving_scope_steps(pipeline, scope_anchor)
    else:
        scoped_steps = set()

    pending_ledger = 0
    pending_progress: List[tuple] = []
    ledger_batch = _ledger_batch_size()

    def drain_progress() -> None:
        """Fire queued progress after the rows they describe are committed.

        Product engines resolve outputs from a nested ``home.session()``
        inside ``progress_cb``. Firing before ``commit`` made those reads
        miss reuse/identity rows and mint duplicate child records.
        """
        if not pending_progress:
            return
        events = pending_progress[:]
        pending_progress.clear()
        if not progress_cb:
            return
        for step_name, coordinate, status, output, content_type in events:
            _call_progress(
                progress_cb, step_name, coordinate, status, output, content_type
            )

    def flush_ledger() -> None:
        """Persist accumulated plan rows, then deliver their progress events."""
        nonlocal pending_ledger
        if pending_ledger:
            session.commit()
            pending_ledger = 0
        drain_progress()

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
        worker_pool = None
        if step.executor == "process":
            worker_pool = worker_pools.get(step.name)
            if worker_pool is None:
                worker_pool = loky.ProcessPoolExecutor(
                    max_workers=workers or step.workers
                )
                worker_pools[step.name] = worker_pool
        elif callable(step.executor):
            worker_pool = worker_pools.get(step.name)
            if worker_pool is None:
                worker_pool = step.executor()
                if not callable(getattr(worker_pool, "submit", None)):
                    _shutdown_worker_pool(worker_pool)
                    raise TypeError(
                        f"Step {step.name!r}: executor factory returned "
                        f"{type(worker_pool).__name__}, expected an object "
                        "with submit(fn, *args, **kwargs)"
                    )
                worker_pools[step.name] = worker_pool
        fut = tp.submit(
            _process_decision,
            step,
            decision,
            params,
            accepts[step.name],
            params_hash,
            memo,
            limiters[step.name],
            worker_pool,
            ctx.pipeline_id,
            ctx.run_id,
        )
        in_flight[fut] = step

    def plan_cells(step: StepSpec, lanes: Optional[List[str]]) -> None:
        """Plan a step (whole, or one lane's cell) and act on the decisions."""
        nonlocal pending_ledger
        if step.name in scoped_steps:
            assert scope_lanes is not None
            if lanes is None:
                lanes = sorted(scope_lanes)
            else:
                lanes = [c for c in lanes if c in scope_lanes]
                if not lanes:
                    return
        decisions = _plan_step(
            session,
            step,
            _scanned_for(step),
            ctx.coord_step_mats,
            params_hash,
            force,
            accepts[step.name],
            lanes=lanes,
            pipeline_id=ctx.pipeline_id,
            home=ctx.home,
            singleton_steps=ctx.singleton_steps,
        )
        if scope_anchor is not None and step.name == scope_anchor:
            for d in decisions:
                ctx.scope_reached.add(d.coordinate)
        _record_planned(session, ctx, step, decisions)
        pending_ledger += max(len(decisions), 1)
        if progress_cb:
            for d in decisions:
                if d.action != "execute":
                    existing = getattr(d, "existing", None)
                    pending_progress.append(
                        (
                            step.name,
                            d.coordinate,
                            d.action,
                            getattr(existing, "output", None),
                            getattr(existing, "content_type", None),
                        )
                    )
        # Execute rows must hit disk before workers run *and* before
        # ``_commit_execution_result`` opens a nested writer session.
        # Reuse progress waits for the same commit so nested ledger
        # reads in progress_cb see the rows.
        if any(d.action == "execute" for d in decisions) or pending_ledger >= ledger_batch:
            flush_ledger()
        for d in decisions:
            if d.action == "execute":
                dispatch(step, d)
        if consumers.get(step.name):
            # Cells that resolved without executing (reuse/blocked/filtered
            # markers from _record_planned, EphemeralRefs a use_cache=False plan
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
        # Flush planned rows before nested execute commits (SQLite lock).
        flush_ledger()
        while in_flight:
            done, _ = concurrent.futures.wait(
                in_flight, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for fut in done:
                step = in_flight.pop(fut)
                outcomes = fut.result()
                flush_ledger()
                for outcome in outcomes:
                    status = "failed"
                    if outcome.success:
                        if isinstance(outcome.result, Filtered):
                            status = "filtered"
                        else:
                            status = "created"
                    _commit_execution_result(ctx, step, outcome)
                    if progress_cb:
                        _call_progress(
                            progress_cb,
                            step.name,
                            outcome.decision.coordinate,
                            status,
                        )
                for outcome in outcomes:
                    if not outcome.is_anchor:
                        advance(step, outcome.decision.coordinate)

        flush_ledger()
        # Flush this segment's steps to disk — durability per segment.
        # The flushed table stays in the disk-table cache so downstream
        # lookups get a cache hit (no re-read).  The write buffers are
        # cleared (data is on disk + in cache).
        for s in seg_steps:
            ctx.home.lanes.flush_step(ctx.pipeline_id, s.name)
    finally:
        for tp in thread_pools.values():
            tp.shutdown(wait=True)
        for pool in worker_pools.values():
            _shutdown_worker_pool(pool)
