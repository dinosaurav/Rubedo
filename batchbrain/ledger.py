"""Ledger phase: every database write a run makes.

Records manifests, per-coordinate statuses, run events, and
materializations (honoring the generations model). The append-only
discipline is enforced by the guards in models.py; this module is the
only code that should be writing run history.
"""

import json
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .db import get_session
from .execution import ExecutionOutcome, _materialized_ancestors
from .models import (
    Filtered,
    Manifest,
    ManifestEntry,
    Materialization,
    MaterializationEdge,
    MaterializationIndexEntry,
    MaterializationLifecycle,
    ProcessResult,
    Run,
    RunCoordinateStatus,
    RunEvent,
    RunSummary,
)
from .planning import MatRef, StepDecision, _code_drift_message
from .spec import StepSpec
from .sources import SourceItem
from .store import stage_and_commit
from .util import utcnow_iso


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
    session.add(
        RunEvent(
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
    )


def _emit(session: Session, ctx: _RunContext, level: str, event_type: str, **kwargs):
    _emit_event(
        session, ctx.run_id, level, event_type, pipeline_id=ctx.pipeline_id, **kwargs
    )


def _new_status(
    ctx: _RunContext, step_name: str, coordinate: str, status: str, **kwargs
) -> RunCoordinateStatus:
    return RunCoordinateStatus(
        run_id=ctx.run_id,
        pipeline_id=ctx.pipeline_id,
        step_name=step_name,
        source_id=ctx.source_id,
        coordinate=coordinate,
        status=status,
        created_at=utcnow_iso(),
        **kwargs,
    )


def _record_planned(
    session: Session, ctx: _RunContext, step: StepSpec, decisions: List[StepDecision]
):
    """Persist the planned (non-executing) outcomes: reuses and blocks."""
    drifted = sum(1 for d in decisions if d.code_drift)
    if drifted:
        import warnings

        message = _code_drift_message(step, drifted)
        warnings.warn(message, stacklevel=2)
        _emit(
            session,
            ctx,
            "warning",
            "code_drift_detected",
            step_name=step.name,
            message=message,
        )

    for d in decisions:
        if d.action == "blocked":
            ctx.coord_step_mats[(d.coordinate, step.name)] = "blocked"
            session.add(
                _new_status(
                    ctx,
                    step.name,
                    d.coordinate,
                    "blocked",
                    metadata_json=json.dumps(
                        {
                            "failed_parents": d.failed_parents,
                            "blocked_parents": d.blocked_parents,
                        }
                    ),
                )
            )
            _emit(
                session,
                ctx,
                "info",
                "step_blocked",
                step_name=step.name,
                coordinate=d.coordinate,
            )
            ctx.count(step.name, "blocked")

        elif d.action == "reuse":
            ctx.coord_step_mats[(d.coordinate, step.name)] = d.existing
            # A cached filter verdict reads as "filtered" in the ledger:
            # "reused" is reserved for coordinates with a usable output
            status = "filtered" if d.existing.filtered else "reused"
            session.add(
                _new_status(
                    ctx,
                    step.name,
                    d.coordinate,
                    status,
                    input_hash=d.input_hash,
                    output_address=d.output_address,
                    materialization_id=d.existing.id,
                )
            )
            _emit(
                session,
                ctx,
                "info",
                "step_cache_hit",
                step_name=step.name,
                coordinate=d.coordinate,
            )
            ctx.count(step.name, status)

        elif d.action == "filtered":
            ctx.coord_step_mats[(d.coordinate, step.name)] = "filtered"
            session.add(
                _new_status(
                    ctx,
                    step.name,
                    d.coordinate,
                    "filtered",
                    metadata_json=json.dumps(
                        {"filtered_parents": d.filtered_parents}
                    ),
                )
            )
            _emit(
                session,
                ctx,
                "info",
                "step_filtered",
                step_name=step.name,
                coordinate=d.coordinate,
            )
            ctx.count(step.name, "filtered")

        elif d.action == "execute":
            _emit(
                session,
                ctx,
                "info",
                "step_processing_started",
                step_name=step.name,
                coordinate=d.coordinate,
                data={"reason": "stale"} if d.stale else None,
            )


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
    refresh: bool = False,
    filtered: bool = False,
) -> tuple[Materialization, str]:
    """Record an executed result at its address, honoring generations.

    An address may accumulate generations over time; at most one is live.
    Identical bytes are the same fact (reuse the live row, or restore a
    non-live one); different bytes supersede the live generation so that
    downstream input hashes change and dependents recompute. Every liveness
    transition appends a materialization_lifecycle row — the append-only
    truth that the is_live/refreshed_at projections cache.

    refresh marks a staleness-driven recompute: identical bytes then reset
    the generation's freshness clock instead of being a silent no-op.

    Returns (materialization, action) with action one of
    created | reused | restored | superseded | refreshed.
    """
    live = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    superseded = None
    if live:
        if live.output_content_hash == output_content_hash:
            if refresh:
                live.refreshed_at = utcnow_iso()
                session.add(
                    MaterializationLifecycle(
                        materialization_id=live.id,
                        action="refreshed",
                        run_id=run_id,
                        reason="stale output re-verified byte-identical",
                        created_at=utcnow_iso(),
                    )
                )
                return live, "refreshed"
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
        code_hash=step.code_hash,
        input_hash=input_hash,
        output_address=output_address,
        output_content_hash=output_content_hash,
        content_type=content_type,
        output_path=output_path,
        metadata_json=metadata_json,
        created_at=utcnow_iso(),
        created_by_run_id=run_id,
        filtered=filtered,
        is_live=True,
    )

    from sqlalchemy.exc import IntegrityError
    try:
        with session.begin_nested():
            session.add(mat)
            session.flush()
    except IntegrityError:
        live = (
            session.query(Materialization)
            .filter_by(output_address=output_address, is_live=True)
            .first()
        )
        if live:
            if live.output_content_hash == output_content_hash:
                return live, "reused"
            
            live.is_live = False
            session.flush()
            superseded = live
            
            mat2 = Materialization(
                pipeline_id=pipeline_id,
                step_name=step.name,
                code_version=step.version,
                code_hash=step.code_hash,
                input_hash=input_hash,
                output_address=output_address,
                output_content_hash=output_content_hash,
                content_type=content_type,
                output_path=output_path,
                metadata_json=metadata_json,
                created_at=utcnow_iso(),
                created_by_run_id=run_id,
                filtered=filtered,
                is_live=True,
            )
            session.add(mat2)
            session.flush()
            mat = mat2
        else:
            raise

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


def _extract_index_entries(session: Session, mat_id: int, step: StepSpec, result):
    """Project declared value fields into the search index.

    Labels are data someone chose to index: fields come straight from the
    output value (dotted paths for nesting), list fields yield one entry
    per element, missing fields are simply not indexed.
    """
    if not step.index:
        return
    value = result.value if isinstance(result, ProcessResult) else result
    if not isinstance(value, dict):
        return

    for path in step.index:
        node = value
        for part in path.split("."):
            node = node.get(part) if isinstance(node, dict) else None
            if node is None:
                break
        if node is None:
            continue
        elements = node if isinstance(node, list) else [node]
        for el in elements:
            if isinstance(el, (str, int, float, bool)):
                session.add(
                    MaterializationIndexEntry(
                        materialization_id=mat_id, field=path, value=str(el)
                    )
                )


def _record_failure(
    session: Session,
    ctx: _RunContext,
    step: StepSpec,
    decision: StepDecision,
    error_message: str,
    error_type: str,
    event_message: str,
    metadata_json: Optional[str] = None,
):
    session.add(
        _new_status(
            ctx,
            step.name,
            decision.coordinate,
            "failed",
            input_hash=decision.input_hash,
            output_address=decision.output_address,
            error_message=error_message,
            error_type=error_type,
            metadata_json=metadata_json,
        )
    )
    _emit(
        session,
        ctx,
        "error",
        "step_failed",
        step_name=step.name,
        coordinate=decision.coordinate,
        message=event_message,
    )
    ctx.count(step.name, "failed")
    ctx.coord_step_mats[(decision.coordinate, step.name)] = "failed"


def _commit_execution_result(
    ctx: _RunContext, step: StepSpec, outcome: ExecutionOutcome
):
    """Persist one execution outcome in its own transaction."""
    decision = outcome.decision
    result = outcome.result
    attempts_meta = (
        json.dumps({"attempts": outcome.attempts}) if outcome.attempts > 1 else None
    )

    from .hashing import compute_output_address

    with get_session() as session:
        for i, attempt_trace in enumerate(outcome.attempt_errors, start=1):
            _emit(
                session,
                ctx,
                "warning",
                "step_attempt_failed",
                step_name=step.name,
                coordinate=decision.coordinate,
                message=attempt_trace,
                data={"attempt": i, "max_attempts": step.retries + 1},
            )

        if not outcome.success:
            _record_failure(
                session,
                ctx,
                step,
                decision,
                error_message=outcome.error_trace,
                error_type="ExecutionError",
                event_message=outcome.error_trace,
                metadata_json=attempts_meta,
            )
            session.commit()
            return

        try:
            # A step declining the coordinate is a cacheable decision: it is
            # committed like any output (a marker object with filtered=True)
            # so re-runs reuse the verdict instead of re-executing the step.
            is_filtered = isinstance(result, Filtered)
            
            if step.shape == "expand":
                # result is a list of (lane_key, payload)
                produced_lanes = []
                for lane_key, payload in result:
                    produced_lanes.append(lane_key)
                    lane_address = compute_output_address(
                        step.name, step.version, decision.input_hash,
                        params_hash=decision.output_address.split(":params:")[1].split(":")[0] if ":params:" in decision.output_address else None,
                        code_hash=step.code_hash if step.code_mode == "auto" else None,
                        coordinate_for_hash=lane_key
                    )
                    
                    if isinstance(payload, ProcessResult) and payload.metadata:
                        lane_meta = json.dumps(payload.metadata)
                        val = payload.value
                    else:
                        lane_meta = None
                        val = payload
                        
                    final_path, output_content_hash, content_type = stage_and_commit(
                        ctx.run_id, lane_key, val
                    )
                    mat, mat_action = _commit_materialization(
                        session, pipeline_id=ctx.pipeline_id, step=step,
                        input_hash=decision.input_hash, output_address=lane_address,
                        output_content_hash=output_content_hash, content_type=content_type,
                        output_path=final_path, metadata_json=lane_meta,
                        run_id=ctx.run_id, refresh=decision.stale, filtered=False
                    )
                    if mat_action in ("created", "superseded"):
                        _extract_index_entries(session, mat.id, step, payload)
                        
                    flat_parents = {
                        f"{dep}:{lane}": ref 
                        for dep, lanes in decision.parent_mats.items() 
                        for lane, ref in lanes.items()
                    }
                    for p_mat in _materialized_ancestors(flat_parents).values():
                        edge_exists = (
                            session.query(MaterializationEdge)
                            .filter_by(parent_id=p_mat.id, child_id=mat.id)
                            .first()
                        )
                        if not edge_exists:
                            session.add(
                                MaterializationEdge(parent_id=p_mat.id, child_id=mat.id)
                            )
                            
                    status = "created" if mat_action == "created" else "reused"
                    session.add(
                        _new_status(
                            ctx, step.name, lane_key, status, # approximation for UI
                            input_hash=decision.input_hash, output_address=lane_address,
                            materialization_id=mat.id, metadata_json=attempts_meta,
                        )
                    )
                    ctx.count(step.name, status)
                    ctx.coord_step_mats[(lane_key, step.name)] = MatRef(
                        mat.id, mat.output_address, mat.output_content_hash,
                        mat.content_type, filtered=False
                    )
                
                # Now commit the manifest itself
                metadata_json = json.dumps({"produced_lanes": produced_lanes})
                result = {"__expand_manifest__": True}
            else:
                if is_filtered:
                    metadata_json = json.dumps({"reason": result.reason})
                    result = {"__filtered__": True, "reason": result.reason}
                else:
                    metadata_json = None
                    if isinstance(result, ProcessResult) and result.metadata:
                        metadata_json = json.dumps(result.metadata)

            final_path, output_content_hash, content_type = stage_and_commit(
                ctx.run_id, decision.coordinate, result
            )

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
                refresh=decision.stale,
                filtered=is_filtered,
            )

            # Fresh generations get their declared value fields indexed;
            # reused/restored/refreshed rows already carry their entries
            if mat_action in ("created", "superseded") and not is_filtered:
                _extract_index_entries(session, mat.id, step, result)

            # Lineage skips through ephemeral hops to the nearest
            # materialized ancestors; a reused or resurrected generation
            # already has its edges
            if step.shape in ("reduce", "expand"):
                flat_parents = {
                    f"{dep}:{lane}": ref 
                    for dep, lanes in decision.parent_mats.items() 
                    for lane, ref in lanes.items()
                }
            else:
                flat_parents = decision.parent_mats
                
            for p_mat in _materialized_ancestors(flat_parents).values():
                edge_exists = (
                    session.query(MaterializationEdge)
                    .filter_by(parent_id=p_mat.id, child_id=mat.id)
                    .first()
                )
                if not edge_exists:
                    session.add(
                        MaterializationEdge(parent_id=p_mat.id, child_id=mat.id)
                    )

            if is_filtered:
                status = "filtered"
            elif mat_action == "reused":
                status = "reused"
            else:
                status = "created"
            session.add(
                _new_status(
                    ctx,
                    step.name,
                    decision.coordinate,
                    status,
                    input_hash=decision.input_hash,
                    output_address=decision.output_address,
                    materialization_id=mat.id,
                    metadata_json=metadata_json if is_filtered else attempts_meta,
                )
            )
            _emit(
                session,
                ctx,
                "info",
                "step_filtered" if is_filtered else f"materialization_{mat_action}",
                step_name=step.name,
                coordinate=decision.coordinate,
                data={"materialization_id": mat.id},
            )
            ctx.count(step.name, status)
            if step.shape != "expand":
                ctx.coord_step_mats[(decision.coordinate, step.name)] = MatRef(
                    mat.id,
                    mat.output_address,
                    mat.output_content_hash,
                    mat.content_type,
                    filtered=is_filtered,
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
                metadata_json=attempts_meta,
            )
        finally:
            from .store import cleanup_staged
            cleanup_staged(ctx.run_id)

        session.commit()


def _snapshot_source(
    session: Session,
    ctx: _RunContext,
    scanned_items: List[SourceItem],
    topo_steps: List[StepSpec],
    source_id: str,
) -> int:
    """Record what lanes exist before we start planning.
    
    Returns the number of coordinates that were removed since the last run.
    """
    now_iso = utcnow_iso()
    manifest_id = f"manifest_{uuid.uuid4().hex[:12]}"

    prev_manifest = (
        session.query(Manifest)
        .filter(Manifest.source_id == source_id)
        .order_by(Manifest.created_at.desc())
        .first()
    )

    manifest = Manifest(
        id=manifest_id,
        run_id=ctx.run_id,
        source_id=source_id,
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
            )
        )

    _emit(
        session,
        ctx,
        "info",
        "manifest_created",
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
                    if step.skip_cache or step.shape == "reduce":
                        continue
                    session.add(
                        _new_status(
                            ctx,
                            step.name,
                            pe.coordinate,
                            "removed",
                            input_hash=pe.content_hash,  # Best approximation for removed
                        )
                    )

                    _emit(
                        session,
                        ctx,
                        "info",
                        "coordinate_removed",
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
        "filtered": ctx.totals["filtered"],
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
        _emit(
            session,
            ctx,
            "info",
            "run_completed" if final_status != "failed" else "run_failed",
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
        blocked_count=ctx.totals["blocked"],
        filtered_count=ctx.totals["filtered"],
    )
