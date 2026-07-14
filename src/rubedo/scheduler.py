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
staged loop. Under schedule="deep", consecutive 1:1 steps share a segment
and each lane advances through them the moment its own inputs commit;
reduce/join/expand and multi-parent maps stay singleton barrier segments.
"""

import concurrent.futures
from typing import Any, Callable, Dict, List, Optional

import loky

from .execution import _process_decision, _RateLimiter, _RunMemo
from .ledger import _commit_execution_result, _record_planned, _RunContext
from .models import Filtered
from .planning import ROOT_LANE, RootItem, StepDecision, _plan_step, _step_accepts_params
from .spec import PipelineSpec, StepSpec

SCHEDULES = ("broad", "deep")


def _scanned_for(step: StepSpec) -> List[RootItem]:
    """The synthetic items feeding a source-less map root's plan.

    A non-expand root mints a single synthetic '@root' item (its input is a
    constant, so it runs once and then reuses); everything else (dependent
    steps, root expands — which yield their own lanes via the generator)
    gets nothing. Shared by runner.plan() and _run_segment below.
    """
    if not step.depends_on and step.shape != "expand":
        return [RootItem(coordinate=ROOT_LANE, content_hash=ROOT_LANE)]
    return []


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
    to plan-all -> execute-all -> commit-each, and on a multi-step (deep)
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
        decisions = _plan_step(
            session,
            step,
            _scanned_for(step),
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
