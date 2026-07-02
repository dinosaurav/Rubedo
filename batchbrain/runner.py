import uuid
import json
import traceback
import concurrent.futures
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, Tuple

from sqlalchemy.orm import Session

from .models import (
    Run,
    RunEvent,
    Manifest,
    ManifestEntry,
    Materialization,
    MaterializationEdge,
    MaterializationLifecycle,
    RunCoordinateStatus,
    RunSummary,
    ProcessResult,
)
from .registry import PipelineSpec, StepSpec
from .db import get_session
from .sources import Source, SourceItem, coerce_source
from .hashing import hash_json, compute_output_address
from .store import stage_and_commit, read_materialization_output
from .util import utcnow_iso


class MatRef:
    def __init__(self, id, output_address, output_content_hash, content_type=None):
        self.id = id
        self.output_address = output_address
        self.output_content_hash = output_content_hash
        self.content_type = content_type


def _emit_event(
    session: Session,
    run_id: str,
    level: str,
    event_type: str,
    pipeline_id: Optional[str] = None,
    step_name: Optional[str] = None,
    coordinate: Optional[str] = None,
    message: Optional[str] = None,
    data: Optional[dict] = None,
):
    event = RunEvent(
        run_id=run_id,
        timestamp=utcnow_iso(),
        level=level,
        event_type=event_type,
        pipeline_id=pipeline_id,
        step_name=step_name,
        coordinate=coordinate,
        message=message,
        data_json=json.dumps(data) if data else None,
    )
    session.add(event)


def topological_sort(pipeline: PipelineSpec) -> List[StepSpec]:
    # Validate and sort
    name_to_step = {s.name: s for s in pipeline.steps}

    if len(name_to_step) != len(pipeline.steps):
        raise ValueError("Duplicate step names in pipeline")

    for s in pipeline.steps:
        for dep in s.depends_on:
            if dep not in name_to_step:
                raise ValueError(f"Step '{s.name}' depends on unknown step '{dep}'")

    # Kahn's algorithm or DFS
    visited = set()
    temp_mark = set()
    order = []

    def visit(n: str):
        if n in temp_mark:
            raise ValueError(f"Cycle detected involving step '{n}'")
        if n not in visited:
            temp_mark.add(n)
            s = name_to_step[n]
            for dep in s.depends_on:
                visit(dep)
            temp_mark.remove(n)
            visited.add(n)
            order.append(s)

    for s in pipeline.steps:
        if s.name not in visited:
            visit(s.name)

    return order


def _compute_step_input_hash(
    step: StepSpec,
    coordinate: str,
    sf_content_hash: str,
    parent_mats: Dict[str, MatRef],
) -> str:
    if not step.depends_on:
        return sf_content_hash
    if len(step.depends_on) == 1:
        parent_name = step.depends_on[0]
        return parent_mats[parent_name].output_content_hash

    # Multi-parent
    parent_hashes = {
        dep: parent_mats[dep].output_content_hash for dep in sorted(step.depends_on)
    }
    return hash_json(parent_hashes)


def _step_accepts_params(step: StepSpec) -> bool:
    import inspect

    return "params" in inspect.signature(step.fn).parameters


def _build_step_params(step: StepSpec, params: Optional[dict]):
    if step.params_model:
        return step.params_model(**(params or {}))
    return params or {}


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


# ---------------------------------------------------------------------------
# Plan phase
#
# Deciding what to do is separated from doing it. A StepDecision is the fate
# of one coordinate at one step: reuse a live materialization, execute the
# step function, or sit blocked behind a failed/blocked parent. "pending"
# only arises in dry-run planning, where an upstream execution's output (and
# therefore every downstream address) is unknowable without running.
# ---------------------------------------------------------------------------


@dataclass
class StepDecision:
    coordinate: str
    action: str  # reuse | execute | blocked | pending
    item: Optional[SourceItem] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    existing: Optional[MatRef] = None
    parent_mats: Dict[str, MatRef] = field(default_factory=dict)
    failed_parents: List[str] = field(default_factory=list)
    blocked_parents: List[str] = field(default_factory=list)


def _plan_step(
    session: Session,
    step: StepSpec,
    scanned_items: List[SourceItem],
    coord_step_mats: Dict[tuple, Any],
    params_hash: str,
    force: bool,
    accepts_params: bool,
) -> List[StepDecision]:
    """Decide the fate of every coordinate for one step. Read-only."""
    decisions = []
    for it in scanned_items:
        coord = it.coordinate

        parent_mats: Dict[str, MatRef] = {}
        failed_parents: List[str] = []
        blocked_parents: List[str] = []
        pending = False

        for dep in step.depends_on:
            parent_mat = coord_step_mats.get((coord, dep))
            if parent_mat == "blocked":
                blocked_parents.append(dep)
            elif parent_mat == "failed":
                failed_parents.append(dep)
            elif parent_mat == "pending":
                pending = True
            else:
                parent_mats[dep] = parent_mat

        if failed_parents or blocked_parents:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="blocked",
                    item=it,
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            )
            continue

        if pending:
            decisions.append(StepDecision(coordinate=coord, action="pending", item=it))
            continue

        input_hash = _compute_step_input_hash(step, coord, it.content_hash, parent_mats)
        output_address = compute_output_address(
            step.name,
            step.version,
            input_hash,
            step.config_hash,
            # Params are part of a step's cache identity only if the step
            # consumes them; downstream steps pick up param changes through
            # the content-hash chain
            params_hash=params_hash if accepts_params else None,
        )

        existing_mat = (
            session.query(Materialization)
            .filter_by(output_address=output_address, is_live=True)
            .first()
        )

        if existing_mat and not force:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="reuse",
                    item=it,
                    input_hash=input_hash,
                    output_address=output_address,
                    existing=MatRef(
                        existing_mat.id,
                        existing_mat.output_address,
                        existing_mat.output_content_hash,
                        existing_mat.content_type,
                    ),
                )
            )
        else:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="execute",
                    item=it,
                    input_hash=input_hash,
                    output_address=output_address,
                    parent_mats=parent_mats,
                )
            )
    return decisions


# ---------------------------------------------------------------------------
# Execute phase
# ---------------------------------------------------------------------------


@dataclass
class _RunContext:
    run_id: str
    pipeline_id: str
    source_id: str
    totals: Dict[str, int]
    by_step: Dict[str, Dict[str, int]]
    coord_step_mats: Dict[tuple, Any] = field(default_factory=dict)

    def count(self, step_name: str, outcome: str):
        self.totals[outcome] += 1
        self.by_step[step_name][outcome] += 1


def _record_planned(
    session: Session, ctx: _RunContext, step: StepSpec, decisions: List[StepDecision]
):
    """Persist the planned (non-executing) outcomes: reuses and blocks."""
    for d in decisions:
        if d.action == "blocked":
            ctx.coord_step_mats[(d.coordinate, step.name)] = "blocked"
            session.add(
                RunCoordinateStatus(
                    run_id=ctx.run_id,
                    pipeline_id=ctx.pipeline_id,
                    step_name=step.name,
                    source_id=ctx.source_id,
                    coordinate=d.coordinate,
                    status="blocked",
                    metadata_json=json.dumps(
                        {
                            "failed_parents": d.failed_parents,
                            "blocked_parents": d.blocked_parents,
                        }
                    ),
                    created_at=utcnow_iso(),
                )
            )
            _emit_event(
                session,
                ctx.run_id,
                "info",
                "step_blocked",
                pipeline_id=ctx.pipeline_id,
                step_name=step.name,
                coordinate=d.coordinate,
            )
            ctx.count(step.name, "blocked")

        elif d.action == "reuse":
            ctx.coord_step_mats[(d.coordinate, step.name)] = d.existing
            session.add(
                RunCoordinateStatus(
                    run_id=ctx.run_id,
                    pipeline_id=ctx.pipeline_id,
                    step_name=step.name,
                    source_id=ctx.source_id,
                    coordinate=d.coordinate,
                    input_hash=d.input_hash,
                    output_address=d.output_address,
                    materialization_id=d.existing.id,
                    status="reused",
                    created_at=utcnow_iso(),
                )
            )
            _emit_event(
                session,
                ctx.run_id,
                "info",
                "step_cache_hit",
                pipeline_id=ctx.pipeline_id,
                step_name=step.name,
                coordinate=d.coordinate,
            )
            ctx.count(step.name, "reused")

        elif d.action == "execute":
            _emit_event(
                session,
                ctx.run_id,
                "info",
                "step_processing_started",
                pipeline_id=ctx.pipeline_id,
                step_name=step.name,
                coordinate=d.coordinate,
            )


def _execute_step(
    step: StepSpec,
    decisions: List[StepDecision],
    source: Source,
    params: Optional[dict],
    accepts_params: bool,
    workers: Optional[int],
) -> Iterator[Tuple[StepDecision, bool, Any, Optional[str]]]:
    """Run the step function for each execute decision; yield as completed."""

    def process(decision: StepDecision):
        try:
            # Root steps get the source payload positionally; dependent steps
            # get parent outputs by parameter name. Either kind may declare
            # `params`.
            if not step.depends_on:
                args = [source.load(decision.item)]
                kwargs = {}
            else:
                args = []
                kwargs = {
                    dep: read_materialization_output(decision.parent_mats[dep])
                    for dep in step.depends_on
                }
            if accepts_params:
                kwargs["params"] = _build_step_params(step, params)
            return decision, True, step.fn(*args, **kwargs), None
        except Exception:
            return decision, False, None, traceback.format_exc()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=workers or step.workers
    ) as executor:
        futures = [executor.submit(process, d) for d in decisions]
        for future in concurrent.futures.as_completed(futures):
            yield future.result()


def _commit_materialization(
    session: Session,
    *,
    pipeline_id: str,
    step: StepSpec,
    input_hash: str,
    output_address: str,
    output_content_hash: str,
    content_type: str,
    output_path: str,
    metadata_json: Optional[str],
    run_id: str,
) -> tuple[Materialization, str]:
    """Record an executed result at its address, honoring generations.

    An address may accumulate generations over time; at most one is live.
    Identical bytes are the same fact (reuse the live row, or restore a
    non-live one); different bytes supersede the live generation so that
    downstream input hashes change and dependents recompute. Every liveness
    transition appends a materialization_lifecycle row — the append-only
    truth that the is_live projection caches.

    Returns (materialization, action) with action one of
    created | reused | restored | superseded.
    """
    live = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    superseded = None
    if live:
        if live.output_content_hash == output_content_hash:
            return live, "reused"
        live.is_live = False
        # Demote before inserting the replacement so the one-live-per-address
        # index never sees two live generations
        session.flush()
        superseded = live
    else:
        prior = (
            session.query(Materialization)
            .filter_by(
                output_address=output_address,
                output_content_hash=output_content_hash,
                is_live=False,
            )
            .order_by(Materialization.id.desc())
            .first()
        )
        if prior:
            prior.is_live = True
            session.add(
                MaterializationLifecycle(
                    materialization_id=prior.id,
                    action="restored",
                    run_id=run_id,
                    reason="recompute produced identical output",
                    created_at=utcnow_iso(),
                )
            )
            return prior, "restored"

    mat = Materialization(
        pipeline_id=pipeline_id,
        step_name=step.name,
        code_version=step.version,
        config_hash=step.config_hash,
        input_hash=input_hash,
        output_address=output_address,
        output_content_hash=output_content_hash,
        content_type=content_type,
        output_path=output_path,
        metadata_json=metadata_json,
        created_at=utcnow_iso(),
        created_by_run_id=run_id,
        is_live=True,
    )
    session.add(mat)
    session.flush()

    if superseded is not None:
        session.add(
            MaterializationLifecycle(
                materialization_id=superseded.id,
                action="superseded",
                run_id=run_id,
                reason="recompute produced different output",
                superseded_by_id=mat.id,
                created_at=utcnow_iso(),
            )
        )
        return mat, "superseded"
    return mat, "created"


def _record_failure(
    session: Session,
    ctx: _RunContext,
    step: StepSpec,
    decision: StepDecision,
    error_message: str,
    error_type: str,
    event_message: str,
):
    session.add(
        RunCoordinateStatus(
            run_id=ctx.run_id,
            pipeline_id=ctx.pipeline_id,
            step_name=step.name,
            source_id=ctx.source_id,
            coordinate=decision.coordinate,
            input_hash=decision.input_hash,
            output_address=decision.output_address,
            status="failed",
            error_message=error_message,
            error_type=error_type,
            created_at=utcnow_iso(),
        )
    )
    _emit_event(
        session,
        ctx.run_id,
        "error",
        "step_failed",
        pipeline_id=ctx.pipeline_id,
        step_name=step.name,
        coordinate=decision.coordinate,
        message=event_message,
    )
    ctx.count(step.name, "failed")
    ctx.coord_step_mats[(decision.coordinate, step.name)] = "failed"


def _commit_execution_result(
    ctx: _RunContext,
    step: StepSpec,
    decision: StepDecision,
    success: bool,
    result: Any,
    error_trace: Optional[str],
):
    """Persist one execution outcome in its own transaction."""
    with get_session() as session:
        if not success:
            _record_failure(
                session,
                ctx,
                step,
                decision,
                error_message=error_trace,
                error_type="ExecutionError",
                event_message=error_trace,
            )
            session.commit()
            return

        try:
            final_path, output_content_hash, content_type = stage_and_commit(
                ctx.run_id, decision.coordinate, result
            )
            metadata_json = None
            if isinstance(result, ProcessResult) and result.metadata:
                metadata_json = json.dumps(result.metadata)

            mat, mat_action = _commit_materialization(
                session,
                pipeline_id=ctx.pipeline_id,
                step=step,
                input_hash=decision.input_hash,
                output_address=decision.output_address,
                output_content_hash=output_content_hash,
                content_type=content_type,
                output_path=final_path,
                metadata_json=metadata_json,
                run_id=ctx.run_id,
            )

            for dep_name, p_mat in decision.parent_mats.items():
                # A reused or resurrected generation already has its
                # lineage edges
                edge_exists = (
                    session.query(MaterializationEdge)
                    .filter_by(parent_id=p_mat.id, child_id=mat.id)
                    .first()
                )
                if not edge_exists:
                    session.add(
                        MaterializationEdge(parent_id=p_mat.id, child_id=mat.id)
                    )

            session.add(
                RunCoordinateStatus(
                    run_id=ctx.run_id,
                    pipeline_id=ctx.pipeline_id,
                    step_name=step.name,
                    source_id=ctx.source_id,
                    coordinate=decision.coordinate,
                    input_hash=decision.input_hash,
                    output_address=decision.output_address,
                    materialization_id=mat.id,
                    status="created",
                    created_at=utcnow_iso(),
                )
            )
            _emit_event(
                session,
                ctx.run_id,
                "info",
                f"materialization_{mat_action}",
                pipeline_id=ctx.pipeline_id,
                step_name=step.name,
                coordinate=decision.coordinate,
                data={"materialization_id": mat.id},
            )
            ctx.count(step.name, "created")
            ctx.coord_step_mats[(decision.coordinate, step.name)] = MatRef(
                mat.id,
                mat.output_address,
                mat.output_content_hash,
                mat.content_type,
            )
        except Exception as e:
            session.rollback()
            _record_failure(
                session,
                ctx,
                step,
                decision,
                error_message=traceback.format_exc(),
                error_type="StagingError",
                event_message=str(e),
            )

        session.commit()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _snapshot_source(
    session: Session,
    ctx: _RunContext,
    scanned_items: List[SourceItem],
    topo_steps: List[StepSpec],
) -> int:
    """Record the manifest and mark removed coordinates. Returns removed count."""
    now_iso = utcnow_iso()
    manifest_id = f"manifest_{uuid.uuid4().hex[:12]}"

    sorted_items = sorted(scanned_items, key=lambda x: x.coordinate)
    manifest_data = [
        {"coordinate": it.coordinate, "hash": it.content_hash} for it in sorted_items
    ]

    prev_manifest = (
        session.query(Manifest)
        .filter(Manifest.source_id == ctx.source_id)
        .order_by(Manifest.created_at.desc())
        .first()
    )

    manifest = Manifest(
        id=manifest_id,
        run_id=ctx.run_id,
        source_id=ctx.source_id,
        manifest_hash=hash_json(manifest_data),
        parent_manifest_id=prev_manifest.id if prev_manifest else None,
        created_at=now_iso,
    )
    session.add(manifest)

    for it in scanned_items:
        session.add(
            ManifestEntry(
                manifest_id=manifest_id,
                coordinate=it.coordinate,
                content_hash=it.content_hash,
                size_bytes=it.metadata.get("size_bytes"),
                mtime_ns=it.metadata.get("mtime_ns"),
            )
        )

    _emit_event(
        session,
        ctx.run_id,
        "info",
        "manifest_created",
        pipeline_id=ctx.pipeline_id,
        data={
            "manifest_id": manifest_id,
            "parent_manifest_id": manifest.parent_manifest_id,
        },
    )
    session.commit()

    scanned_coordinates = {it.coordinate for it in scanned_items}

    removed_count = 0
    if prev_manifest:
        prev_entries = (
            session.query(ManifestEntry).filter_by(manifest_id=prev_manifest.id).all()
        )
        for pe in prev_entries:
            if pe.coordinate not in scanned_coordinates:
                for step in topo_steps:
                    last_rc = (
                        session.query(RunCoordinateStatus)
                        .filter(
                            RunCoordinateStatus.pipeline_id == ctx.pipeline_id,
                            RunCoordinateStatus.source_id == ctx.source_id,
                            RunCoordinateStatus.coordinate == pe.coordinate,
                            RunCoordinateStatus.step_name == step.name,
                            RunCoordinateStatus.status.in_(["created", "reused"]),
                        )
                        .order_by(RunCoordinateStatus.id.desc())
                        .first()
                    )

                    session.add(
                        RunCoordinateStatus(
                            run_id=ctx.run_id,
                            pipeline_id=ctx.pipeline_id,
                            step_name=step.name,
                            source_id=ctx.source_id,
                            coordinate=pe.coordinate,
                            input_hash=pe.content_hash,  # Best approximation for removed
                            previous_output_address=last_rc.output_address
                            if last_rc
                            else None,
                            previous_materialization_id=last_rc.materialization_id
                            if last_rc
                            else None,
                            status="removed",
                            created_at=utcnow_iso(),
                        )
                    )

                    _emit_event(
                        session,
                        ctx.run_id,
                        "info",
                        "coordinate_removed",
                        pipeline_id=ctx.pipeline_id,
                        step_name=step.name,
                        coordinate=pe.coordinate,
                        message="Coordinate removed because it is absent from the latest manifest",
                    )
                removed_count += 1
    session.commit()
    return removed_count


def _finish_run(ctx: _RunContext) -> RunSummary:
    full_summary = {
        "created": ctx.totals["created"],
        "reused": ctx.totals["reused"],
        "failed": ctx.totals["failed"],
        "removed": ctx.totals["removed"],
        "blocked": ctx.totals["blocked"],
        "total": ctx.totals,
        "by_step": ctx.by_step,
    }

    with get_session() as session:
        final_run = session.query(Run).filter_by(id=ctx.run_id).first()
        if ctx.totals["failed"] == 0 and ctx.totals["blocked"] == 0:
            final_run.status = "completed"
        elif ctx.totals["created"] == 0 and ctx.totals["reused"] == 0:
            final_run.status = "failed"
        else:
            final_run.status = "completed_with_failures"

        final_status = final_run.status
        final_run.finished_at = utcnow_iso()
        final_run.summary_json = json.dumps(full_summary)
        _emit_event(
            session,
            ctx.run_id,
            "info",
            "run_completed" if final_status != "failed" else "run_failed",
            pipeline_id=ctx.pipeline_id,
            message=f"Run finished with status {final_run.status}",
        )
        session.commit()

    return RunSummary(
        run_id=ctx.run_id,
        status=final_status,
        created_count=ctx.totals["created"],
        reused_count=ctx.totals["reused"],
        failed_count=ctx.totals["failed"],
        removed_count=ctx.totals["removed"],
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
                for decision, success, result, error_trace in _execute_step(
                    step, to_execute, source, params, accepts_params, workers
                ):
                    _commit_execution_result(
                        ctx, step, decision, success, result, error_trace
                    )

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
