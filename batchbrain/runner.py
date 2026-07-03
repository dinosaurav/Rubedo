"""Orchestration: the public run()/plan() entry points.

The phases live in their own modules — planning.py (decide what to do),
execution.py (run step functions), ledger.py (persist what happened) —
and this module wires them together.
"""

import json
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .db import get_session
from .execution import _execute_step, _RunMemo
from .hashing import hash_json
from .ledger import (
    _commit_execution_result,
    _emit_event,
    _finish_run,
    _record_planned,
    _RunContext,
    _snapshot_source,
)
from .models import Manifest, ManifestEntry, Run, RunSummary
from .planning import (
    EphemeralRef,  # noqa: F401  (re-exported: part of the runner's public surface)
    MatRef,  # noqa: F401
    StepDecision,  # noqa: F401
    _plan_step,
    _step_accepts_params,
    topological_sort,
)
from .registry import PipelineSpec
from .sources import Source, coerce_source
from .util import utcnow_iso


def _resolve_invocation(pipeline, source, params):
    """Shared by run() and plan(): id -> spec, source coercion, param validation."""
    if isinstance(pipeline, str):
        from .registry import get_pipeline

        pipeline = get_pipeline(pipeline)

    source = pipeline.source if source is None else coerce_source(source)

    first = pipeline.steps[0] if pipeline.steps else None
    if first and first.params_model:
        params = first.params_model.model_validate(params or {}).model_dump(
            mode="json"
        )
    return pipeline, source, params


@dataclass
class PlannedCoordinate:
    coordinate: str
    step_name: str
    action: str  # reuse | execute | pending | removed
    output_address: Optional[str] = None


@dataclass
class RunPlan:
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
    pipeline: PipelineSpec | str,
    source: Optional[Source | str] = None,
    *,
    params: Optional[dict] = None,
    force: bool = False,
) -> RunPlan:
    """Dry-run: what would run() do, and why — without writing anything.

    "execute" means the step function would run for that coordinate;
    "pending" means the answer depends on an upstream execution whose output
    (and therefore this coordinate's address) is unknowable without running;
    "removed" means the coordinate vanished from the source since last run.
    """
    from .planning import _code_drift_message

    pipeline, source, params = _resolve_invocation(pipeline, source, params)
    topo_steps = topological_sort(pipeline)
    scanned_items = source.scan()
    params_hash = hash_json(params or {})

    items: List[PlannedCoordinate] = []
    plan_warnings: List[str] = []
    coord_step_mats: Dict[tuple, Any] = {}

    with get_session() as session:
        prev_manifest = (
            session.query(Manifest)
            .filter(Manifest.source_id == source.id)
            .order_by(Manifest.created_at.desc())
            .first()
        )
        if prev_manifest:
            scanned_coordinates = {it.coordinate for it in scanned_items}
            prev_entries = (
                session.query(ManifestEntry)
                .filter_by(manifest_id=prev_manifest.id)
                .all()
            )
            for pe in prev_entries:
                if pe.coordinate not in scanned_coordinates:
                    for step in topo_steps:
                        if step.skip_cache:
                            continue
                        items.append(
                            PlannedCoordinate(pe.coordinate, step.name, "removed")
                        )

        for step in topo_steps:
            accepts_params = _step_accepts_params(step)
            decisions = _plan_step(
                session,
                step,
                scanned_items,
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
                else:  # execute or pending: output unknowable until run
                    coord_step_mats[(d.coordinate, step.name)] = "pending"

    counts: Dict[str, int] = {}
    for it in items:
        counts[it.action] = counts.get(it.action, 0) + 1

    return RunPlan(
        pipeline_id=pipeline.id,
        source_id=source.id,
        items=items,
        counts=counts,
        warnings=plan_warnings,
    )


def run(
    pipeline: PipelineSpec | str,
    source: Optional[Source | str] = None,
    *,
    params: Optional[dict] = None,
    workers: Optional[int] = None,
    force: bool = False,
) -> RunSummary:
    """Run a pipeline — the single entry point.

    Accepts a PipelineSpec or a registered pipeline id. Params are
    validated against the first step's params_model whenever one is
    declared, regardless of how the pipeline was obtained.
    """
    pipeline, source, params = _resolve_invocation(pipeline, source, params)
    return run_pipeline(
        pipeline=pipeline, source=source, params=params, workers=workers, force=force
    )


def run_pipeline(
    pipeline: PipelineSpec,
    source: Optional[Source | str] = None,
    workers: Optional[int] = None,
    force: bool = False,
    params: Optional[dict] = None,
) -> RunSummary:
    source = pipeline.source if source is None else coerce_source(source)

    topo_steps = topological_sort(pipeline)
    ctx = _RunContext(
        run_id=f"run_{uuid.uuid4().hex[:12]}",
        pipeline_id=pipeline.id,
        source_id=source.id,
        totals={"created": 0, "reused": 0, "failed": 0, "removed": 0, "blocked": 0},
        by_step={
            s.name: {"created": 0, "reused": 0, "failed": 0, "removed": 0, "blocked": 0}
            for s in topo_steps
            if not s.skip_cache
        },
    )

    with get_session() as session:
        session.add(
            Run(
                id=ctx.run_id,
                kind="process",
                status="running",
                pipeline_id=ctx.pipeline_id,
                source_id=ctx.source_id,
                params_json=json.dumps(params or {}, sort_keys=True),
                started_at=utcnow_iso(),
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

        try:
            scanned_items = source.scan()

            removed_count = _snapshot_source(session, ctx, scanned_items, topo_steps)
            ctx.totals["removed"] = removed_count
            for counts in ctx.by_step.values():
                counts["removed"] = removed_count

            params_hash = hash_json(params or {})
            memo = _RunMemo()

            for step in topo_steps:
                accepts_params = _step_accepts_params(step)

                decisions = _plan_step(
                    session,
                    step,
                    scanned_items,
                    ctx.coord_step_mats,
                    params_hash,
                    force,
                    accepts_params,
                )
                _record_planned(session, ctx, step, decisions)
                session.commit()

                to_execute = [d for d in decisions if d.action == "execute"]
                for outcome in _execute_step(
                    step, to_execute, source, params, accepts_params, workers, memo
                ):
                    _commit_execution_result(ctx, step, outcome)

            return _finish_run(ctx)

        except Exception as e:
            with get_session() as err_session:
                err_run = err_session.query(Run).filter_by(id=ctx.run_id).first()
                if err_run:
                    err_run.status = "failed"
                    err_run.error_message = traceback.format_exc()
                    err_run.finished_at = utcnow_iso()
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
