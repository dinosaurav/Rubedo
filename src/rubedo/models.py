"""
SQLAlchemy models and immutability guards for the Rubedo ledger.
"""
from typing import Any, Optional, Dict
from pydantic import BaseModel
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    event,
    text,
)
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()


class Run(Base):
    """One execution attempt. Identity columns are written once at creation;
    the lifecycle columns (status, finished_at, error_message, summary_json)
    are a projection of the run_events log and are the only legal updates."""

    __tablename__ = "runs"
    id = Column(String, primary_key=True)
    kind = Column(String, nullable=False)
    status = Column(String, nullable=False)
    source_id = Column(String)
    params_json = Column(String)
    selection_json = Column(String)
    # Snapshot of the pipeline definition (steps, edges, policies) at run
    # time — the ledger's record of what DAG produced this run's outputs
    definition_json = Column(String)
    started_at = Column(String, nullable=False)
    finished_at = Column(String)
    error_message = Column(String)
    summary_json = Column(String)
    pipeline_id = Column(String, index=True)


class RunEvent(Base):
    """A single log event associated with a run."""
    __tablename__ = "run_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("runs.id"), index=True)
    timestamp = Column(String, nullable=False)
    level = Column(String, nullable=False)
    event_type = Column(String, nullable=False, index=True)
    pipeline_id = Column(String, index=True)
    step_name = Column(String, index=True)
    coordinate = Column(String, index=True)
    message = Column(String)
    data_json = Column(String)


class MaterializationEdge(Base):
    """A directed lineage edge between parent and child materializations."""
    __tablename__ = "materialization_edges"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey("materializations.id"), nullable=False)
    child_id = Column(
        Integer, ForeignKey("materializations.id"), nullable=False, index=True
    )
    __table_args__ = (UniqueConstraint("parent_id", "child_id", name="_mat_edge_uc"),)


class Materialization(Base):
    """A committed output. Every column except is_live is immutable.

    is_live is a projection: the append-only materialization_lifecycle table
    is the truth about invalidations, restorations, and supersessions, and
    every is_live flip must be accompanied by a lifecycle row. An address can
    accumulate generations over time (recomputes of non-deterministic steps);
    at most one may be live at once."""

    __tablename__ = "materializations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id = Column(String, nullable=False, index=True)
    step_name = Column(String, nullable=False, index=True)
    code_version = Column(String, nullable=False)
    code_hash = Column(String)  # source hash at creation time, for drift detection
    input_hash = Column(String, nullable=False)
    output_address = Column(String, nullable=False, index=True)
    output_content_hash = Column(String, nullable=False)
    content_type = Column(String)  # bytes | text | json
    output_path = Column(String, nullable=False)
    metadata_json = Column(String)
    created_at = Column(String, nullable=False)
    created_by_run_id = Column(String, ForeignKey("runs.id"), nullable=False)
    # The step declined this coordinate: the stored object is a marker, and
    # downstream steps are filtered rather than executed. Immutable, like
    # the rest of the content columns — a changed decision is a new
    # generation.
    filtered = Column(Boolean, nullable=False, default=False)
    is_live = Column(Boolean, nullable=False, default=True)
    # Projection of the latest "refreshed" lifecycle row: when a stale
    # output was last re-verified byte-identical. Freshness clock is
    # refreshed_at or created_at.
    refreshed_at = Column(String)
    __table_args__ = (
        Index(
            "uq_live_output_address",
            "output_address",
            unique=True,
            sqlite_where=text("is_live"),
        ),
    )


class MaterializationIndexEntry(Base):
    """A searchable projection of an output-value field.

    A "label" is just data someone chose to index: @step(index=[...]) names
    value fields to extract at commit time. Non-unique and multi-valued by
    nature (a list field yields one row per element). Purely operational —
    never part of cache identity or dataflow.
    """

    __tablename__ = "materialization_index"
    id = Column(Integer, primary_key=True, autoincrement=True)
    materialization_id = Column(
        Integer, ForeignKey("materializations.id"), nullable=False, index=True
    )
    field = Column(String, nullable=False)
    value = Column(String, nullable=False)
    __table_args__ = (Index("ix_mat_index_field_value", "field", "value"),)


class MaterializationLifecycle(Base):
    """Append-only record of every liveness transition of a materialization."""

    __tablename__ = "materialization_lifecycle"
    id = Column(Integer, primary_key=True, autoincrement=True)
    materialization_id = Column(
        Integer, ForeignKey("materializations.id"), nullable=False, index=True
    )
    action = Column(String, nullable=False)  # invalidated | restored | superseded | refreshed
    run_id = Column(String, ForeignKey("runs.id"))
    reason = Column(String)
    superseded_by_id = Column(Integer, ForeignKey("materializations.id"))
    created_at = Column(String, nullable=False)


class RunCoordinateStatus(Base):
    """The outcome of a specific step for a specific coordinate during a run."""
    __tablename__ = "run_coordinate_statuses"
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, ForeignKey("runs.id"), nullable=False, index=True)
    pipeline_id = Column(String, index=True)
    step_name = Column(String, nullable=False, index=True)
    source_id = Column(String, nullable=False)
    coordinate = Column(String, nullable=False, index=True)
    input_hash = Column(String)
    output_address = Column(String)
    materialization_id = Column(Integer, ForeignKey("materializations.id"))
    status = Column(String, nullable=False, index=True)
    error_message = Column(String)
    error_type = Column(String, index=True)
    metadata_json = Column(String)
    created_at = Column(String, nullable=False)
    __table_args__ = (
        UniqueConstraint("run_id", "coordinate", "step_name", name="_run_coord_uc"),
    )


# ---------------------------------------------------------------------------
# Immutability guards
#
# The ledger is append-only: history is recorded by inserting new rows, never
# by rewriting old ones. These listeners make that physical instead of
# conventional. Run and Materialization carry a small set of projection
# columns (caches of the run_events / materialization_lifecycle logs) that
# are the only legal updates anywhere in the schema.
# ---------------------------------------------------------------------------


class ImmutabilityError(RuntimeError):
    """Raised when an attempt is made to update or delete immutable ledger rows."""
    pass


_APPEND_ONLY = (
    RunEvent,
    MaterializationEdge,
    MaterializationIndexEntry,
    MaterializationLifecycle,
    RunCoordinateStatus,
)

_PROJECTION_COLUMNS = {
    Run: frozenset({"status", "finished_at", "error_message", "summary_json"}),
    Materialization: frozenset({"is_live", "refreshed_at"}),
}


def _reject_update(mapper, connection, target):
    """Guard to prevent updates on append-only models."""
    raise ImmutabilityError(
        f"{type(target).__name__} rows are append-only and cannot be updated"
    )


def _reject_delete(mapper, connection, target):
    """Guard to prevent deletions on all ledger models."""
    raise ImmutabilityError(
        f"{type(target).__name__} rows are ledger history and cannot be deleted"
    )


def _projection_guard(allowed):
    """Guard to allow updates only on specific projection columns."""
    def guard(mapper, connection, target):
        changed = {
            attr.key
            for attr in sa_inspect(target).attrs
            if attr.history.has_changes()
        }
        illegal = changed - allowed
        if illegal:
            raise ImmutabilityError(
                f"{type(target).__name__} columns {sorted(illegal)} are immutable; "
                f"only {sorted(allowed)} may be updated"
            )

    return guard


for _model in _APPEND_ONLY:
    event.listen(_model, "before_update", _reject_update)
    event.listen(_model, "before_delete", _reject_delete)

for _model, _allowed in _PROJECTION_COLUMNS.items():
    event.listen(_model, "before_update", _projection_guard(_allowed))
    event.listen(_model, "before_delete", _reject_delete)


# ---------------------------------------------------------------------------
# Pairing-rule guard (invariant 8)
#
# is_live/refreshed_at are legal to update (above), but every such flip must
# ship a materialization_lifecycle row in the same transaction — otherwise the
# append-only lifecycle log stops being the truth about liveness. This is
# enforced at commit rather than flush: the supersede path in
# _commit_materialization legitimately flushes a demotion (is_live=False)
# before the superseded-by materialization exists to reference in its lifecycle
# row. So we accumulate across every flush and check the sets once, at commit.
# ---------------------------------------------------------------------------


def _track_liveness_pairing(session, flush_context, instances):
    """before_flush: record which materializations changed liveness and which
    lifecycle rows were added, into session.info. Runs on every flush and
    accumulates — the demotion of a superseded generation is flushed away
    before its lifecycle row is added, so a single snapshot would miss it."""
    changed = session.info.setdefault("_liveness_changed", set())
    paired = session.info.setdefault("_liveness_paired", set())
    for obj in session.dirty:
        if isinstance(obj, Materialization):
            attrs = sa_inspect(obj).attrs
            if attrs.is_live.history.has_changes() or attrs.refreshed_at.history.has_changes():
                changed.add(obj.id)
    for obj in session.new:
        if isinstance(obj, MaterializationLifecycle):
            paired.add(obj.materialization_id)


def _clear_liveness_tracking(session):
    """Clear the liveness pairing tracking accumulators."""
    session.info.pop("_liveness_changed", None)
    session.info.pop("_liveness_paired", None)


def _assert_liveness_pairing(session):
    """before_commit: every liveness flip in this transaction must be paired
    with a lifecycle row for the same materialization. before_commit fires
    *before* commit's own pre-flush, so flush here first to make the
    accumulators complete (this captures the lifecycle rows that the supersede
    and refresh paths add just before committing). Consume the accumulators so
    a clean commit starts the next transaction fresh; aborted transactions are
    cleared by after_rollback.

    before_commit also fires when a savepoint (begin_nested) is released, with
    the outer transaction still open — skip those: _commit_materialization
    demotes and flushes inside a savepoint but adds the superseded lifecycle
    row only after it, so validating at the savepoint would false-positive."""
    if session.in_nested_transaction():
        return
    session.flush()
    changed = session.info.get("_liveness_changed", set())
    paired = session.info.get("_liveness_paired", set())
    missing = changed - paired
    _clear_liveness_tracking(session)
    if missing:
        raise ImmutabilityError(
            f"materialization(s) {sorted(missing)} changed is_live/refreshed_at "
            f"without a materialization_lifecycle row in the same transaction "
            f"(invariant 8: every liveness transition is logged)"
        )


event.listen(Session, "before_flush", _track_liveness_pairing)
event.listen(Session, "before_commit", _assert_liveness_pairing)
event.listen(Session, "after_rollback", _clear_liveness_tracking)


class ProcessResult(BaseModel):
    """The successful output of a step, carrying the value and optional metadata."""
    value: Any
    metadata: Optional[Dict[str, Any]] = None


class Filtered:
    """Return this from a step to decline a coordinate.

    The decision is cached like any other output (recorded as a filtered
    materialization), and downstream steps skip the coordinate with status
    "filtered" instead of executing.
    """

    def __init__(self, reason: Optional[str] = None):
        self.reason = reason

    def __repr__(self):
        return f"Filtered(reason={self.reason!r})"


class RunSummary(BaseModel):
    """A summary of the outcomes of a completed run."""
    run_id: str
    status: str
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    blocked_count: int = 0
    filtered_count: int = 0
