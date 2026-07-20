"""Ledger phase: every database write a run makes.

Records per-coordinate statuses, run events, and
materializations (honoring the generations model). The append-only
discipline is enforced by the guards in models.py; this module is the
only code that should be writing run history.
"""

import json
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from sqlalchemy.orm import Session

from .execution import ExecutionOutcome, _materialized_ancestors
from .models import (
    Filtered,
    Run,
    RunCoordinateStatus,
    RunEvent,
    RunSummary,
)
from .planning import MatRef, StepDecision, _code_drift_message
from .spec import StepSpec
from .util import utcnow_iso


def _upsert_input_hash_usage(
    session: Session,
    *,
    address: str,
    run_id: str,
    fulfilled: Optional[bool] = None,
) -> None:
    """Atomically claim or fulfill one address.

    Claims update ``last_run_id`` while preserving an existing fulfilled
    value. Fulfills set it explicitly. SQLite and Postgres both use native
    ON CONFLICT so concurrent first claims cannot surface a PK IntegrityError.
    """
    from .models import InputHashUsage

    values = {
        "address": address,
        "last_run_id": run_id,
        "fulfilled": bool(fulfilled),
    }
    updates: Dict[str, Any] = {"last_run_id": run_id}
    if fulfilled is not None:
        updates["fulfilled"] = fulfilled

    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        statement = pg_insert(InputHashUsage).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=[InputHashUsage.address],
            set_=updates,
        )
        session.execute(statement)
        return
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        statement = sqlite_insert(InputHashUsage).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=[InputHashUsage.address],
            set_=updates,
        )
        session.execute(statement)
        return

    existing = session.get(InputHashUsage, address)
    if existing is None:
        session.add(InputHashUsage(**values))
    else:
        existing.last_run_id = run_id  # type: ignore[assignment]
        if fulfilled is not None:
            existing.fulfilled = fulfilled  # type: ignore[assignment]


def _insert_materialization_edge(
    session: Session, *, parent_address: str, child_address: str
) -> None:
    """Insert one lineage edge, ignoring a concurrent identical insert."""
    from .models import MaterializationEdge

    values = {
        "parent_address": parent_address,
        "child_address": child_address,
    }
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        session.execute(
            pg_insert(MaterializationEdge).values(**values).on_conflict_do_nothing(
                index_elements=[
                    MaterializationEdge.parent_address,
                    MaterializationEdge.child_address,
                ]
            )
        )
        return
    if dialect == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        session.execute(
            sqlite_insert(MaterializationEdge).values(**values).on_conflict_do_nothing(
                index_elements=[
                    MaterializationEdge.parent_address,
                    MaterializationEdge.child_address,
                ]
            )
        )
        return
    exists = session.query(MaterializationEdge).filter_by(**values).first()
    if exists is None:
        session.add(MaterializationEdge(**values))

if TYPE_CHECKING:
    from .home import Home


@dataclass
class _RunContext:
    """Context holding state and counts for the current pipeline run."""
    run_id: str
    pipeline_id: str
    source_id: str
    home: "Home"
    totals: Dict[str, int]
    by_step: Dict[str, Dict[str, int]]
    coord_step_mats: Dict[tuple, Any] = field(default_factory=dict)
    # Partial-run scope tracking (anchor coordinates that produced a decision).
    scope_reached: set = field(default_factory=set)
    scope_counts: Optional[Dict[str, Any]] = None

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
            # Claim: record that this run is executing this address.
            # We do NOT flip fulfilled=False here — the old output may
            # produce identical bytes (expand root re-run, force=True),
            # and the commit path needs to know if the address was
            # previously fulfilled to distinguish "reused" (same bytes,
            # was already live) from "created" (was invalidated/missing).
            # The claim just records last_run_id for traceability.
            if d.output_address is not None:
                _upsert_input_hash_usage(
                    session,
                    address=str(d.output_address),
                    run_id=ctx.run_id,
                )


def _identity_of(output_value: Any) -> str:
    """Compute a content identity hash from an output value (for
    downstream ``input_hash`` computation).  Same value → same identity →
    downstream reuses, regardless of inline (native Arrow) vs spilled
    (ref string).  Computed once at commit time from the original output
    value and stored in the ``output_identity`` Arrow column."""
    from .hashing import hash_bytes
    import json

    if isinstance(output_value, str):
        return hash_bytes(output_value.encode("utf-8"))
    return hash_bytes(
        json.dumps(output_value, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
    # Fence a cloud writer whose renewable lease was lost before it can
    # fulfill SQLite liveness or buffer another Arrow row.
    ctx.home.lanes.check_writer_lease(ctx.pipeline_id)
    decision = outcome.decision
    result = outcome.result
    attempts_meta = (
        json.dumps({"attempts": outcome.attempts}) if outcome.attempts > 1 else None
    )

    with ctx.home.session() as session:
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
            # committed like any output (a marker dict with filtered=True)
            # so re-runs reuse the verdict instead of re-executing the step.
            is_filtered = isinstance(result, Filtered)
            if is_filtered:
                result = {"__filtered__": True, "reason": result.reason}

            if outcome.arrow_batched:
                # Arrow data already written to the lane store's arrow batch
                # buffer by _expand_table_outcomes — skip serialize_output +
                # append_filled.  Read the output value and identity from the
                # arrow batch buffer for the MatRef.  Always "created" —
                # re-running an expand root and producing the same content
                # is a new generation, not a reuse.
                mat_action = "created"
                content_type = "json"
                batch_row = ctx.home.lanes.arrow_batch_row_by_address(
                    ctx.pipeline_id, step.name, str(decision.output_address)
                )
                output_string = batch_row.get("output") if batch_row else None
                identity = batch_row.get("output_identity") if batch_row else None
            elif outcome.is_anchor:
                # Expand anchor: store in a separate file (<step>.anchor.arrow)
                # via append_anchor, so the anchor's output (a list of child
                # hashes) doesn't pollute the step's output column type.
                # Always serialize as a JSON string — the anchor file's output
                # column is always string type.
                import json as _json
                output_string = _json.dumps(result, sort_keys=True, separators=(",", ":"))
                content_type = "json"
                identity = _identity_of(output_string)
                mat_action = "created"
                from .models import InputHashUsage
                existing_usage = (
                    session.query(InputHashUsage)
                    .filter_by(address=str(decision.output_address))
                    .first()
                )
                if existing_usage and existing_usage.fulfilled:
                    existing_row = ctx.home.lanes.address_row_index().get(str(decision.output_address))
                    if existing_row and existing_row.get("output_identity") == identity:
                        mat_action = "reused"
                if mat_action != "reused":
                    ctx.home.lanes.append_anchor(
                        pipeline_id=ctx.pipeline_id,
                        step_name=step.name,
                        lane_key=decision.coordinate,
                        address=str(decision.output_address),
                        input_hash=str(decision.input_hash),
                        output=output_string,
                        content_type=content_type,
                        run_id=ctx.run_id,
                        code_hash=step.code_hash,
                        code_version=step.version,
                        output_identity=identity,
                    )
            else:
                output_string, content_type = ctx.home.store.serialize_output(
                    ctx.run_id, decision.coordinate, result
                )
                identity = _identity_of(output_string)

                # Determine mat_action: if the address was already fulfilled
                # (live) and the existing Arrow row has the same identity,
                # the step re-ran but produced identical output → "reused".
                mat_action = "created"
                from .models import InputHashUsage
                existing_usage = (
                    session.query(InputHashUsage)
                    .filter_by(address=str(decision.output_address))
                    .first()
                )
                if existing_usage and existing_usage.fulfilled:
                    existing_row = ctx.home.lanes.address_row_index().get(str(decision.output_address))
                    if existing_row and existing_row.get("output_identity") == identity:
                        mat_action = "reused" if not decision.stale else "refreshed"

                if mat_action != "reused":
                    ctx.home.lanes.append_filled(
                        pipeline_id=ctx.pipeline_id,
                        step_name=step.name,
                        lane_key=decision.coordinate,
                        address=str(decision.output_address),
                        input_hash=str(decision.input_hash),
                        output=output_string,
                        content_type=content_type,
                        run_id=ctx.run_id,
                        filtered=is_filtered,
                        code_hash=step.code_hash,
                        code_version=step.version,
                        output_identity=identity,
                    )

            # Mark this address as fulfilled in input_hash_usages — the
            # liveness gate.  The planning phase checks fulfilled=True to
            # decide reuse.
            _upsert_input_hash_usage(
                session,
                address=str(decision.output_address),
                run_id=ctx.run_id,
                fulfilled=True,
            )
            # Update the fulfilled cache so the next planning phase
            # sees this address as live without a SQLite re-query.
            ctx.home.lanes.mark_fulfilled(str(decision.output_address))

            if outcome.is_anchor:
                # Cache anchor only: stored so a re-run's plan can skip the
                # expand fn. Not a lane — no index, edge, status, count, or
                # coord_step_mats entry.
                session.commit()
                return

            # Lineage skips through ephemeral hops to the nearest
            # materialized ancestors; a reused or resurrected generation
            # already has its edges
            if step.in_shape in ("aggregate", "fold"):
                flat_parents = {
                    f"{dep}:{lane}": ref 
                    for dep, lanes in decision.parent_mats.items() 
                    for lane, ref in lanes.items()
                }
            else:
                flat_parents = decision.parent_mats
                
            for p_mat in _materialized_ancestors(flat_parents).values():
                p_addr = getattr(p_mat, "output_address", None) or ""
                c_addr = str(decision.output_address)
                _insert_materialization_edge(
                    session,
                    parent_address=p_addr,
                    child_address=c_addr,
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
                data={"output_address": str(decision.output_address)},
            )
            ctx.count(step.name, status)
            ctx.coord_step_mats[(decision.coordinate, step.name)] = MatRef(
                0,
                str(decision.output_address),
                identity,
                content_type,
                filtered=is_filtered,
                output=output_string,
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
            ctx.home.store.cleanup_staged(ctx.run_id)

        session.commit()


def _finish_run(ctx: _RunContext) -> RunSummary:
    """Finalize the run status and return a summary of all outcomes."""
    # Safety-net flush: per-segment flush in _run_segment should have
    # already written everything, but this catches any edge case (e.g.
    # anchor rows from expand steps that weren't in the segment's step
    # list).  On exception paths, clear_run_buffers drops only the
    # current segment's in-flight work; completed segments are on disk.
    ctx.home.lanes.flush_all()

    full_summary = {
        "created": ctx.totals["created"],
        "reused": ctx.totals["reused"],
        "failed": ctx.totals["failed"],
        "blocked": ctx.totals["blocked"],
        "filtered": ctx.totals["filtered"],
        "total": ctx.totals,
        "by_step": ctx.by_step,
    }
    if ctx.scope_counts is not None:
        full_summary.update(ctx.scope_counts)

    with ctx.home.session() as session:
        final_run = session.query(Run).filter_by(id=ctx.run_id).first()
        assert final_run is not None
        if ctx.totals["failed"] == 0 and ctx.totals["blocked"] == 0:
            final_run.status = "completed"  # type: ignore
        elif ctx.totals["created"] == 0 and ctx.totals["reused"] == 0 and ctx.totals["filtered"] == 0:
            final_run.status = "failed"  # type: ignore
        else:
            final_run.status = "completed_with_failures"  # type: ignore

        final_kind = str(final_run.kind)
        final_status = str(final_run.status)
        final_run.finished_at = utcnow_iso()  # type: ignore
        final_run.summary_json = json.dumps(full_summary)  # type: ignore
        _emit(
            session,
            ctx,
            "info",
            "run_completed" if final_status != "failed" else "run_failed",
            message=f"Run finished with status {final_status}",
        )
        session.commit()

    return RunSummary(
        run_id=ctx.run_id,
        kind=final_kind,
        status=final_status,
        created_count=ctx.totals["created"],
        reused_count=ctx.totals["reused"],
        failed_count=ctx.totals["failed"],
        blocked_count=ctx.totals["blocked"],
        filtered_count=ctx.totals["filtered"],
        scope_requested=ctx.scope_counts.get("scope_requested") if ctx.scope_counts else None,
        scope_reached=ctx.scope_counts.get("scope_reached") if ctx.scope_counts else None,
        scope_missing=ctx.scope_counts.get("scope_missing") if ctx.scope_counts else None,
    ).bind_home(ctx.home)
