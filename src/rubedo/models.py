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
from sqlalchemy.orm import DeclarativeBase

from .util import iso_age_seconds

class Base(DeclarativeBase):
    pass


class Run(Base):
    """One execution attempt. Identity columns are written once at creation;
    the lifecycle columns (status, finished_at, error_message, summary_json,
    last_heartbeat_at) are the only legal updates.

    status is terminal-only: NULL while the run is in flight, set exactly
    once at the end (completed | completed_with_failures | failed) as a
    projection of the run_events log. "running" is never stored — it is a
    present-tense claim no durable row can keep truthfully (a killed process
    would leave it lying forever). Readers derive running/interrupted from
    last_heartbeat_at via effective_run_status()."""

    __tablename__ = "runs"
    id = Column(String, primary_key=True)
    kind = Column(String, nullable=False)
    status = Column(String)  # NULL until terminal
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
    # Ephemeral presence signal: set at creation and bumped periodically by
    # the run process. Exempt from event pairing — presence is about *now*,
    # not history, and nothing durable is ever derived from it.
    last_heartbeat_at = Column(String)


RUN_HEARTBEAT_INTERVAL_SECONDS = 60.0
"""How often a live run process bumps Run.last_heartbeat_at."""

RUN_HEARTBEAT_STALE_SECONDS = 180.0
"""An unfinished run whose heartbeat is older than this reads as interrupted."""


def effective_run_status(run: "Run") -> str:
    """The status a reader should display for a run.

    Stored status is terminal-only; an unfinished run is "running" while its
    heartbeat is fresh and "interrupted" once it goes stale (process died, or
    the machine slept — a resumed process starts beating again and the run
    flips back to "running" on its own).
    """
    if run.status is not None:
        return str(run.status)
    beat = run.last_heartbeat_at or run.started_at
    if iso_age_seconds(str(beat)) < RUN_HEARTBEAT_STALE_SECONDS:
        return "running"
    return "interrupted"


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


class ObjectReclamation(Base):
    """Append-only record of an object file deleted by retention GC (10b).

    A swept object is a *deliberate* deletion (retention removed bytes, never
    facts — the ledger rows that named it stay). This table is how `rubedo du`
    tells a reclaimed object apart from one that is genuinely missing
    (corruption): a content hash present here was pruned on purpose. Bytes are
    recorded at deletion time (the store is content-addressed, so the same hash
    is only ever deleted once while unreferenced)."""

    __tablename__ = "object_reclamations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    content_hash = Column(String, nullable=False, index=True)
    bytes = Column(Integer, nullable=False)
    trigger = Column(String)  # gc | auto_prune | budget
    run_id = Column(String, ForeignKey("runs.id"))
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


class InputHashUsage(Base):
    """The ``address -> (last_run_id, fulfilled)`` map.

    Two columns, one job: tell the planning phase whether to reuse or
    recompute.  ``fulfilled=True`` → a filled Arrow row exists (reuse);
    ``fulfilled=False`` → recompute (crash, in-flight claim, or
    invalidation — all three mean "no filled Arrow row to reuse").

    ``address`` is the comprehensive cache identity
    (``hash(step, version, input_hash[, params][, code])``) and the
    primary key.  The caller already knows ``step_name`` and
    ``pipeline_id`` (it's planning a specific step in a specific
    pipeline), so those don't need to be stored here — the Arrow file
    path is ``tables/<pipeline>/<step>.arrow``, constructed by the caller.

    Mutability: this table is the one *non-append-only* ledger table —
    ``last_run_id`` and ``fulfilled`` legitimately update (claim at plan
    time, fulfill at commit time, tombstone on invalidate, demote on
    prune).  The rest of the ledger stays append-only.

    Future todo: a bloom filter in front of this table for fast "does
    this address exist at all?" checks, so cold-cache lanes skip the
    SQLite lookup entirely.
    """
    __tablename__ = "input_hash_usages"
    address = Column(String, primary_key=True)
    last_run_id = Column(String, nullable=False)
    fulfilled = Column(Boolean, nullable=False, default=False)


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
    RunCoordinateStatus,
    ObjectReclamation,
)

_PROJECTION_COLUMNS = {
    Run: frozenset(
        {"status", "finished_at", "error_message", "summary_json", "last_heartbeat_at"}
    ),
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

for _model, _allowed in _PROJECTION_COLUMNS.items():  # type: ignore
    event.listen(_model, "before_update", _projection_guard(_allowed))
    event.listen(_model, "before_delete", _reject_delete)


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

    def failures(self) -> list[Dict[str, Any]]:
        """Retrieve the failed coordinates and errors for this run."""
        from .db import get_session
        from .queries import get_run_failures
        with get_session() as session:
            return get_run_failures(session, self.run_id)

    def output_for(self, step_name: str) -> dict[str, Any]:
        """Fetch the output values for a specific step from this run.
        
        Returns a dict mapping coordinates to their materialization payload.
        """
        from .db import get_session
        from .store import read_materialization_output
        with get_session() as session:
            statuses = (
                session.query(RunCoordinateStatus)
                .filter_by(run_id=self.run_id, step_name=step_name)
                .all()
            )
            result: dict[str, Any] = {}
            for s in statuses:
                if s.status in ("created", "filtered", "reused") and s.materialization_id:
                    mat = session.get(Materialization, s.materialization_id)
                    if mat:
                        result[str(s.coordinate)] = read_materialization_output(mat)  # type: ignore[arg-type]
            return result
