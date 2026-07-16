"""Ledger phase: every database write a run makes.

Records per-coordinate statuses, run events, and
materializations (honoring the generations model). The append-only
discipline is enforced by the guards in models.py; this module is the
only code that should be writing run history.
"""

import json
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .db import get_session
from .execution import ExecutionOutcome, _materialized_ancestors
from .models import (
    Filtered,
    Materialization,
    MaterializationEdge,
    MaterializationIndexEntry,
    ProcessResult,
    Run,
    RunCoordinateStatus,
    RunEvent,
    RunSummary,
)
from .planning import MatRef, StepDecision, _code_drift_message
from .spec import StepSpec
from .store import stage_and_commit
from . import lane_store
from .util import utcnow_iso


@dataclass
class _RunContext:
    """Context holding state and counts for the current pipeline run."""
    run_id: str
    pipeline_id: str
    source_id: str
    totals: Dict[str, int]
    by_step: Dict[str, Dict[str, int]]
    coord_step_mats: Dict[tuple, Any] = field(default_factory=dict)

    def count(self, step_name: str, outcome: str):
        """Record an outcome count for the run and a specific step."""
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
    """Write a raw run event to the database."""
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
    """Write a run event associated with the current context."""
    _emit_event(
        session, ctx.run_id, level, event_type, pipeline_id=ctx.pipeline_id, **kwargs
    )


def _new_status(
    ctx: _RunContext, step_name: str, coordinate: str, status: str, **kwargs
) -> RunCoordinateStatus:
    """Create a new RunCoordinateStatus object for the given coordinate."""
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
            status = "filtered" if d.existing.filtered else "reused"  # type: ignore
            
            meta = {}
            if d.failed_parents:
                meta["failed_parents"] = d.failed_parents
            if d.blocked_parents:
                meta["blocked_parents"] = d.blocked_parents
                
            if meta:
                _emit(
                    session,
                    ctx,
                    "warning",
                    "partial_fan_in",
                    step_name=step.name,
                    coordinate=d.coordinate,
                    message=f"Proceeding with dropped lanes ({len(d.failed_parents or [])} failed, {len(d.blocked_parents or [])} blocked)",
                )
                
            session.add(
                _new_status(
                    ctx,
                    step.name,
                    d.coordinate,
                    status,
                    input_hash=d.input_hash,
                    output_address=d.output_address,
                    materialization_id=d.existing.id,  # type: ignore
                    metadata_json=json.dumps(meta) if meta else None,
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
            # Claim: insert/update input_hash_usages with fulfilled=False
            # before execution starts.  This is the soft lock — a second
            # worker checking sees fulfilled=False and defers.  The commit
            # path flips fulfilled=True after the step succeeds.
            # Skip for expand parent lanes whose output_address isn't
            # known yet (children are minted during execution).
            if d.output_address is not None:
                from .models import InputHashUsage
                existing_claim = (
                    session.query(InputHashUsage)
                    .filter_by(
                        address=str(d.output_address),
                        step_name=step.name,
                        pipeline_id=ctx.pipeline_id,
                    )
                    .first()
                )
                if existing_claim:
                    existing_claim.fulfilled = False  # type: ignore[assignment]
                    existing_claim.last_run_id = ctx.run_id  # type: ignore[assignment]
                    existing_claim.claimed_at = utcnow_iso()  # type: ignore[assignment]
                else:
                    session.add(
                        InputHashUsage(
                            address=str(d.output_address),
                            lane_key=d.coordinate,
                            step_name=step.name,
                            pipeline_id=ctx.pipeline_id,
                            last_run_id=ctx.run_id,
                            claimed_at=utcnow_iso(),
                            fulfilled=False,
                        )
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
    """Record an executed result at its address.

    In the new model (notes/arrow-storage.md), liveness is
    input_hash_usages.fulfilled, not Materialization.is_live.  This
    function simplified from the generations protocol (supersede/restore/
    savepoint/IntegrityError dance) to a plain insert: if the same bytes
    already exist at this address, return the existing row (reused); if
    different bytes, insert a new row (superseded — but is_live is NOT
    flipped on the old row, because liveness is in input_hash_usages).
    No lifecycle rows, no pairing guard, no savepoints.

    Returns (materialization, action) with action one of
    created | reused | superseded | refreshed.
    """
    existing = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    if existing:
        if existing.output_content_hash == output_content_hash:
            if refresh:
                existing.refreshed_at = utcnow_iso()  # type: ignore[assignment]
                return existing, "refreshed"
            return existing, "reused"
        # Different bytes: flip old row's is_live=False for the partial
        # unique index (uq_live_output_address still enforces one-live-per-
        # address).  No lifecycle row — liveness is input_hash_usages.fulfilled.
        existing.is_live = False  # type: ignore[assignment]
        session.flush()
    else:
        # No live row — check for a non-live row with the same content_hash
        # (restore: recompute produced identical bytes to an invalidated gen).
        # Flip it back to is_live=True.  No lifecycle row.
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
            prior.is_live = True  # type: ignore[assignment]
            if refresh:
                prior.refreshed_at = utcnow_iso()  # type: ignore[assignment]
                return prior, "refreshed"
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
    session.add(mat)
    session.flush()
    if existing:
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
            node = node.get(part) if isinstance(node, dict) else None  # type: ignore
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
    """Record a step execution failure and emit an error event."""
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
                error_message=outcome.error_trace,  # type: ignore
                error_type="ExecutionError",
                event_message=outcome.error_trace,  # type: ignore
                metadata_json=attempts_meta,
            )
            session.commit()
            return

        try:
            # A step declining the coordinate is a cacheable decision: it is
            # committed like any output (a marker object with filtered=True)
            # so re-runs reuse the verdict instead of re-executing the step.
            is_filtered = isinstance(result, Filtered)
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
                input_hash=decision.input_hash,  # type: ignore
                output_address=decision.output_address,  # type: ignore
                output_content_hash=output_content_hash,
                content_type=content_type,
                output_path=final_path,
                metadata_json=metadata_json,
                run_id=ctx.run_id,
                refresh=decision.stale,
                filtered=is_filtered,
            )

            # Parallel write to the lane_store (Arrow) — the new primary
            # data plane.  Coexists with the SQLite materializations row
            # above while readers are migrated one-by-one; the SQLite path
            # is deleted once every reader consults lane_store.  See
            # notes/arrow-storage.md §Phase 2.
            lane_store.append_filled(
                pipeline_id=ctx.pipeline_id,
                step_name=step.name,
                lane_key=decision.coordinate,
                address=str(decision.output_address),
                input_hash=str(decision.input_hash),
                content_hash=output_content_hash,
                content_type=content_type,
                output_path=final_path,
                run_id=ctx.run_id,
                filtered=is_filtered,
                code_hash=step.code_hash,
            )

            # Parallel write: mark this address as fulfilled in
            # input_hash_usages — the liveness gate.  The planning phase
            # checks fulfilled=True to decide reuse.
            from .models import InputHashUsage
            existing_usage = (
                session.query(InputHashUsage)
                .filter_by(
                    address=str(decision.output_address),
                    step_name=step.name,
                    pipeline_id=ctx.pipeline_id,
                )
                .first()
            )
            if existing_usage:
                existing_usage.fulfilled = True  # type: ignore[assignment]
                existing_usage.last_run_id = ctx.run_id  # type: ignore[assignment]
                existing_usage.claimed_at = utcnow_iso()  # type: ignore[assignment]
            else:
                session.add(
                    InputHashUsage(
                        address=str(decision.output_address),
                        lane_key=decision.coordinate,
                        step_name=step.name,
                        pipeline_id=ctx.pipeline_id,
                        last_run_id=ctx.run_id,
                        claimed_at=utcnow_iso(),
                        fulfilled=True,
                    )
                )

            if outcome.is_anchor:
                # Cache anchor only: stored so a re-run's plan can skip the
                # expand fn. Not a lane — no index, edge, status, count, or
                # coord_step_mats entry.
                session.commit()
                return

            # Fresh generations get their declared value fields indexed;
            # reused/restored/refreshed rows already carry their entries
            if mat_action in ("created", "superseded") and not is_filtered:
                _extract_index_entries(session, mat.id, step, result)  # type: ignore

            # Lineage skips through ephemeral hops to the nearest
            # materialized ancestors; a reused or resurrected generation
            # already has its edges
            if step.shape == "reduce":
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
                
            status_meta = {}
            if is_filtered:
                status_meta["reason"] = result["reason"]
            if attempts_meta:
                status_meta.update(json.loads(attempts_meta))
            if getattr(decision, "failed_parents", None):
                status_meta["failed_parents"] = decision.failed_parents
            if getattr(decision, "blocked_parents", None):
                status_meta["blocked_parents"] = decision.blocked_parents
                
            if getattr(decision, "failed_parents", None) or getattr(decision, "blocked_parents", None):
                _emit(
                    session,
                    ctx,
                    "warning",
                    "partial_fan_in",
                    step_name=step.name,
                    coordinate=decision.coordinate,
                    message=f"Proceeding with dropped lanes ({len(getattr(decision, 'failed_parents', []) or [])} failed, {len(getattr(decision, 'blocked_parents', []) or [])} blocked)",
                )

            session.add(
                _new_status(
                    ctx,
                    step.name,
                    decision.coordinate,
                    status,
                    input_hash=decision.input_hash,
                    output_address=decision.output_address,
                    materialization_id=mat.id,
                    metadata_json=json.dumps(status_meta) if status_meta else None,
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


def _finish_run(ctx: _RunContext) -> RunSummary:
    """Finalize the run status and return a summary of all outcomes."""
    # Flush the lane_store's in-memory buffers to disk so a crashed
    # process at least leaves the rows it wrote on disk before the run
    # ended.  On exception paths the buffer is cleared by the next run's
    # fresh start; here it's the normal end-of-run flush.
    lane_store.flush_all()

    full_summary = {
        "created": ctx.totals["created"],
        "reused": ctx.totals["reused"],
        "failed": ctx.totals["failed"],
        "blocked": ctx.totals["blocked"],
        "filtered": ctx.totals["filtered"],
        "total": ctx.totals,
        "by_step": ctx.by_step,
    }

    with get_session() as session:
        final_run = session.query(Run).filter_by(id=ctx.run_id).first()
        if ctx.totals["failed"] == 0 and ctx.totals["blocked"] == 0:
            final_run.status = "completed"  # type: ignore
        elif ctx.totals["created"] == 0 and ctx.totals["reused"] == 0 and ctx.totals["filtered"] == 0:
            final_run.status = "failed"  # type: ignore
        else:
            final_run.status = "completed_with_failures"  # type: ignore

        final_status = final_run.status  # type: ignore
        final_run.finished_at = utcnow_iso()  # type: ignore
        final_run.summary_json = json.dumps(full_summary)  # type: ignore
        _emit(
            session,
            ctx,
            "info",
            "run_completed" if final_status != "failed" else "run_failed",
            message=f"Run finished with status {final_run.status}",  # type: ignore
        )
        session.commit()

    return RunSummary(
        run_id=ctx.run_id,
        status=final_status,
        created_count=ctx.totals["created"],
        reused_count=ctx.totals["reused"],
        failed_count=ctx.totals["failed"],
        blocked_count=ctx.totals["blocked"],
        filtered_count=ctx.totals["filtered"],
    )
