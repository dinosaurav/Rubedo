"""Orchestration: the public run()/plan() entry points.

The phases live in their own modules — planning.py (decide what to do),
execution.py (run step functions), ledger.py (persist what happened) —
and this module wires them together.
"""

import json
import os
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .db import get_session, init_db
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
from .spec import PipelineSpec, definition
from .sources import Source, coerce_source
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

    Dependent steps and root *expand* steps (themselves sources) read nothing.
    """
    if step.depends_on or step.shape == "expand":
        return None
    return step.source if step.source is not None else next(iter(sources))


def _resolve_invocation(pipeline: PipelineSpec, source, params):
    """Shared by run() and plan(): source coercion and param validation."""
    if source is not None:
        source = coerce_source(source)

    first = pipeline.steps[0] if pipeline.steps else None
    if first and first.params_model:
        params = first.params_model.model_validate(params or {}).model_dump(
            mode="json"
        )
    return pipeline, source, params


@dataclass
class PlannedCoordinate:
    """A projected action for a single coordinate in a specific step."""
    coordinate: str
    step_name: str
    action: str  # reuse | execute | pending | removed
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
    (and therefore this coordinate's address) is unknowable without running;
    "removed" means the coordinate vanished from the source since last run.

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

    single = len(sources) == 1
    with get_session() as session:
        for name, src in sources.items():
            prev_manifest = (
                session.query(Manifest)
                .filter(Manifest.source_id == src.id)
                .order_by(Manifest.created_at.desc())
                .first()
            )
            if not prev_manifest:
                continue
            scanned_coordinates = {it.coordinate for it in items_by_source[name]}
            if single:
                steps_to_mark = [s for s in topo_steps if not s.skip_cache]
            else:
                steps_to_mark = [
                    s for s in topo_steps
                    if _source_name_for(pipeline, sources, s) == name
                ]
            for pe in (
                session.query(ManifestEntry).filter_by(manifest_id=prev_manifest.id).all()
            ):
                if pe.coordinate not in scanned_coordinates:
                    for step in steps_to_mark:
                        items.append(
                            PlannedCoordinate(pe.coordinate, step.name, "removed")
                        )

        for step in topo_steps:
            accepts_params = _step_accepts_params(step)
            sname = _source_name_for(pipeline, sources, step)
            decisions = _plan_step(
                session,
                step,
                items_by_source[sname] if sname else [],
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
) -> RunSummary:
    """Run a pipeline — the single entry point.

    Params are validated against the first step's params_model whenever
    one is declared. home, if given, points the ledger/object store at a
    custom root instead of the default `.rubedo`/RUBEDO_HOME (see
    notes/TODO.md item 1).
    """
    pipeline, source, params = _resolve_invocation(pipeline, source, params)
    return run_pipeline(
        pipeline=pipeline,
        source=source,
        params=params,
        workers=workers,
        force=force,
        home=home,
    )


def run_pipeline(
    pipeline: PipelineSpec,
    source: Optional[Source | str] = None,
    workers: Optional[int] = None,
    force: bool = False,
    params: Optional[dict] = None,
    home: Optional[str] = None,
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

    Returns:
        RunSummary: A summary of the executed run.
    """
    if home is not None:
        _init_home(home)

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
        totals={"created": 0, "reused": 0, "failed": 0, "removed": 0, "blocked": 0, "filtered": 0},
        by_step={
            s.name: {"created": 0, "reused": 0, "failed": 0, "removed": 0, "blocked": 0, "filtered": 0}
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
                definition_json=json.dumps(definition(pipeline)),
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
            items_by_source = {name: src.scan() for name, src in sources.items()}

            single = len(sources) == 1
            removed_count = 0
            for name, src in sources.items():
                steps_to_mark = topo_steps if single else [
                    s for s in topo_steps
                    if _source_name_for(pipeline, sources, s) == name
                ]
                removed_count += _snapshot_source(
                    session, ctx, items_by_source[name], steps_to_mark,
                    source_id=src.id,
                )
            ctx.totals["removed"] = removed_count
            for counts in ctx.by_step.values():
                counts["removed"] = removed_count

            params_hash = hash_json(params or {})
            memo = _RunMemo()

            for step in topo_steps:
                accepts_params = _step_accepts_params(step)
                sname = _source_name_for(pipeline, sources, step)

                decisions = _plan_step(
                    session,
                    step,
                    items_by_source[sname] if sname else [],
                    ctx.coord_step_mats,
                    params_hash,
                    force,
                    accepts_params,
                )
                _record_planned(session, ctx, step, decisions)
                session.commit()

                to_execute = [d for d in decisions if d.action == "execute"]
                for outcome in _execute_step(
                    step, to_execute, step_sources, params, accepts_params, workers, memo
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
