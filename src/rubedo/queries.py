"""Read-only query layer for the Rubedo engine.

These functions provide a shared interface for querying the ledger state,
used by both the HTTP server and the CLI.
"""
from __future__ import annotations

import fnmatch
import json
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from sqlalchemy import func
from sqlalchemy.orm import Session

from .models import InputHashUsage, Run, RunCoordinateStatus, effective_run_status
from .schemas import RunListItem, RunDetailOut
from .selection import Selection, _resolve_dotted

if TYPE_CHECKING:
    from .home import Home


_OUTPUT_STATUSES = frozenset({"created", "reused", "filtered"})


@dataclass(frozen=True)
class Cell:
    """One (run, step, lane) outcome — the unit of read."""

    run_id: str
    pipeline_id: str
    step_name: str
    coordinate: str
    status: str
    output_address: Optional[str] = None
    output: Any = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    source_id: Optional[str] = None
    code_version: Optional[str] = None
    input_hash: Optional[str] = None
    updated_at: Optional[str] = None
    content_type: Optional[str] = None
    output_identity: Optional[str] = None


def _to_dict(obj):
    """Convert an SQLAlchemy model instance to a dictionary."""
    d = dict(obj.__dict__)
    d.pop("_sa_instance_state", None)
    return d


def get_recent_runs(
    session: Session,
    limit: int = 50,
    *,
    pipeline: Optional[str] = None,
    kind: Optional[str] = None,
    status: Optional[str] = None,
) -> List[RunListItem]:
    """List recent pipeline runs, newest first.

    ``status`` filters on *effective* status (terminal stored status, or
    ``running`` / ``interrupted`` derived from heartbeat). Partial runs are
    included unless ``kind`` excludes them. Existing callers that pass only
    ``session`` / ``limit`` are unchanged.
    """
    if limit < 1:
        raise ValueError(f"limit must be >= 1, got {limit}")
    query = session.query(Run)
    if pipeline is not None:
        query = query.filter(Run.pipeline_id == pipeline)
    if kind is not None:
        query = query.filter(Run.kind == kind)
    query = query.order_by(Run.started_at.desc(), Run.id.desc())
    # Effective running/interrupted status is derived from heartbeat, so a
    # status-filtered query cannot be capped in SQL without potentially hiding
    # older matches behind arbitrarily many newer non-matches.
    runs = query.limit(limit).all() if status is None else query.all()
    results = []
    for run in runs:
        d = _to_dict(run)
        d["status"] = effective_run_status(run)
        if status is not None and d["status"] != status:
            continue
        summary = {}
        if run.summary_json:
            try:
                summary = json.loads(str(run.summary_json))
            except Exception:
                pass
        d["created_count"] = summary.get("created", 0)
        d["reused_count"] = summary.get("reused", 0)
        d["failed_count"] = summary.get("failed", 0)
        d["blocked_count"] = summary.get("blocked", 0)
        d["filtered_count"] = summary.get("filtered", 0)
        results.append(RunListItem(**d))
        if len(results) >= limit:
            break
    return results


def get_run_summary(session: Session, run_id: str) -> Optional[RunDetailOut]:
    """Get detailed information for a specific run."""
    run = session.query(Run).filter_by(id=run_id).first()
    if not run:
        return None

    d = _to_dict(run)
    d["status"] = effective_run_status(run)
    summary = {}
    if run.summary_json:
        try:
            summary = json.loads(str(run.summary_json))
        except Exception:
            pass
    d["created_count"] = summary.get("created", 0)
    d["reused_count"] = summary.get("reused", 0)
    d["failed_count"] = summary.get("failed", 0)
    d["blocked_count"] = summary.get("blocked", 0)
    d["filtered_count"] = summary.get("filtered", 0)
    d["by_step"] = summary.get("by_step")
    d["scope_requested"] = summary.get("scope_requested")
    d["scope_reached"] = summary.get("scope_reached")
    d["scope_missing"] = summary.get("scope_missing")
    if run.selection_json:
        try:
            d["selection"] = json.loads(str(run.selection_json))
        except Exception:
            pass
    if run.definition_json:
        try:
            d["definition"] = json.loads(str(run.definition_json))
        except Exception:
            pass
    return RunDetailOut(**d)


def get_run_failures(session: Session, run_id: str) -> List[Dict[str, Any]]:
    """Retrieve failed coordinates and error messages for a run."""
    failures = (
        session.query(RunCoordinateStatus)
        .filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.status == "failed"
        )
        .all()
    )
    return [
        {
            "coordinate": f.coordinate,
            "step_name": f.step_name,
            "error_type": f.error_type,
            "error_message": f.error_message,
        }
        for f in failures
    ]


def _status_values(status: Optional[str | Collection[str]]) -> Optional[list[str]]:
    if status is None:
        return None
    if isinstance(status, str):
        return [status]
    return [str(s) for s in status]


def _addr(output_address: Any) -> Optional[str]:
    return str(output_address) if output_address else None


def _row_timestamp(row: Optional[dict[str, Any]], fallback: Any) -> Optional[str]:
    if row is not None and row.get("ts"):
        return str(row["ts"])
    if fallback is None:
        return None
    return str(fallback)


def _cell_from_rc(
    rc: RunCoordinateStatus,
    *,
    home: "Home",
    arrow_idx: dict[str, dict[str, Any]],
    resolve_output: bool,
) -> Cell:
    addr = _addr(rc.output_address)
    row = arrow_idx.get(addr) if addr else None
    output = None
    if (
        resolve_output
        and rc.status in _OUTPUT_STATUSES
        and addr is not None
        and row is not None
    ):
        output = home.store.read_output(row.get("output"), row.get("content_type"))

    return Cell(
        run_id=str(rc.run_id),
        pipeline_id=str(rc.pipeline_id or ""),
        step_name=str(rc.step_name),
        coordinate=str(rc.coordinate),
        status=str(rc.status),
        output_address=addr,
        output=output,
        error_type=str(rc.error_type) if rc.error_type else None,
        error_message=str(rc.error_message) if rc.error_message else None,
        source_id=str(rc.source_id) if rc.source_id else None,
        code_version=str(row.get("code_version")) if row and row.get("code_version") else None,
        input_hash=str(rc.input_hash) if rc.input_hash else None,
        updated_at=_row_timestamp(row, rc.created_at),
        content_type=str(row.get("content_type")) if row and row.get("content_type") else None,
        output_identity=str(row.get("output_identity")) if row and row.get("output_identity") else None,
    )


def get_run_cells(
    session: Session,
    home: "Home",
    run_id: str,
    *,
    step: Optional[str] = None,
    status: Optional[str | Collection[str]] = None,
    resolve_output: bool = False,
    address_rows: Optional[dict[str, dict[str, Any]]] = None,
) -> list[Cell]:
    """Return the ledger cells recorded for a single run."""
    query = session.query(RunCoordinateStatus).filter(RunCoordinateStatus.run_id == run_id)
    if step is not None:
        query = query.filter(RunCoordinateStatus.step_name == step)
    statuses = _status_values(status)
    if statuses is not None:
        query = query.filter(RunCoordinateStatus.status.in_(statuses))

    rows = query.order_by(RunCoordinateStatus.id.asc()).all()
    arrow_idx = (
        address_rows if address_rows is not None else home.lanes.address_row_index()
    )
    return [
        _cell_from_rc(rc, home=home, arrow_idx=arrow_idx, resolve_output=resolve_output)
        for rc in rows
    ]


def get_current_cells(
    session: Session,
    home: "Home",
    *,
    pipeline: Optional[str] = None,
    step: Optional[str] = None,
    resolve_output: bool = False,
) -> list[Cell]:
    """Return each pipeline's latest *full* run's live created/reused/filtered cells.

    "Current" is the latest terminal ``kind='process'`` run — never a
    declaration, invalidate, gc, or partial trial. Partial runs remain
    queryable by run id and do not displace the authoritative membership.
    """
    # Preserve the existing "latest terminal run" semantics for failures;
    # only partial/non-process invocations are non-authoritative.
    process_filter = (Run.kind == "process") & Run.status.isnot(None)
    latest_started = (
        session.query(
            Run.pipeline_id.label("pid"),
            func.max(Run.started_at).label("mx"),
        )
        .filter(process_filter)
        .group_by(Run.pipeline_id)
        .subquery()
    )
    latest_run_ids = (
        session.query(Run.id)
        .join(
            latest_started,
            (Run.pipeline_id == latest_started.c.pid)
            & (Run.started_at == latest_started.c.mx),
        )
        .filter(process_filter)
        .subquery()
    )
    latest_ids_subq = (
        session.query(func.max(RunCoordinateStatus.id).label("max_id"))
        .filter(RunCoordinateStatus.run_id.in_(session.query(latest_run_ids.c.id)))
        .group_by(
            RunCoordinateStatus.pipeline_id,
            RunCoordinateStatus.step_name,
            RunCoordinateStatus.source_id,
            RunCoordinateStatus.coordinate,
        )
        .subquery()
    )
    query = (
        session.query(RunCoordinateStatus)
        .filter(RunCoordinateStatus.id.in_(session.query(latest_ids_subq.c.max_id)))
        .filter(RunCoordinateStatus.status.in_(list(_OUTPUT_STATUSES)))
        .filter(RunCoordinateStatus.output_address.isnot(None))
    )
    if pipeline is not None:
        query = query.filter(RunCoordinateStatus.pipeline_id == pipeline)
    if step is not None:
        query = query.filter(RunCoordinateStatus.step_name == step)

    rows = query.order_by(RunCoordinateStatus.id.asc()).all()
    arrow_idx = home.lanes.address_row_index()
    fulfilled_addrs = {
        str(u.address)
        for u in session.query(InputHashUsage.address)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }

    cells: list[Cell] = []
    for rc in rows:
        addr = _addr(rc.output_address)
        if addr is None or addr not in fulfilled_addrs or addr not in arrow_idx:
            continue
        cells.append(
            _cell_from_rc(
                rc,
                home=home,
                arrow_idx=arrow_idx,
                resolve_output=resolve_output,
            )
        )
    return cells


def _matches_version(version_str: Optional[str], version_range: Optional[str]) -> bool:
    if version_range is None:
        return True
    if not version_str:
        return False
    from packaging.specifiers import SpecifierSet
    from packaging.version import InvalidVersion, Version

    try:
        specifier_set = SpecifierSet(version_range)
        return Version(str(version_str)) in specifier_set
    except (InvalidVersion, ValueError):
        return False


def _output_for_match(
    cell: Cell,
    row: Optional[dict[str, Any]],
    *,
    home: "Home",
) -> Any:
    if cell.output is not None:
        return cell.output
    if row is None:
        return None
    output = row.get("output")
    if isinstance(output, dict):
        return output
    return home.store.read_output(output, row.get("content_type"))


def _matches_index(
    cell: Cell,
    row: Optional[dict[str, Any]],
    index: dict[str, str],
    *,
    home: "Home",
) -> bool:
    output = _output_for_match(cell, row, home=home)
    if not isinstance(output, dict):
        return False
    for field, expected in index.items():
        val = _resolve_dotted(output, field)
        if val is None:
            return False
        check_vals = [str(v) for v in val] if isinstance(val, (list, tuple)) else [str(val)]
        if expected not in check_vals:
            return False
    return True


def _matches_selection(
    cell: Cell,
    selection: Selection,
    *,
    row: Optional[dict[str, Any]],
    home: "Home",
    fulfilled_addrs: Optional[set[str]] = None,
) -> bool:
    if selection.pipeline_id and cell.pipeline_id != selection.pipeline_id:
        return False
    if selection.step and cell.step_name != selection.step:
        return False
    if selection.source_id and cell.source_id != selection.source_id:
        return False
    if selection.output_address and cell.output_address != selection.output_address:
        return False
    if selection.coordinate_glob and not fnmatch.fnmatch(
        cell.coordinate,
        selection.coordinate_glob,
    ):
        return False
    if selection.code_version and cell.code_version != selection.code_version:
        return False
    if not _matches_version(cell.code_version, selection.version_range):
        return False
    if selection.invalidated is not None:
        live = bool(cell.output_address and fulfilled_addrs and cell.output_address in fulfilled_addrs)
        if selection.invalidated == live:
            return False
    if selection.index and not _matches_index(cell, row, selection.index, home=home):
        return False
    return True


def _latest_cells_for_addresses(
    session: Session,
    home: "Home",
    addresses: Collection[str],
    *,
    resolve_output: bool,
) -> list[Cell]:
    if not addresses:
        return []
    latest_ids = (
        session.query(func.max(RunCoordinateStatus.id).label("max_id"))
        .filter(RunCoordinateStatus.output_address.in_(list(addresses)))
        .group_by(
            RunCoordinateStatus.pipeline_id,
            RunCoordinateStatus.step_name,
            RunCoordinateStatus.source_id,
            RunCoordinateStatus.coordinate,
        )
        .subquery()
    )
    rows = (
        session.query(RunCoordinateStatus)
        .filter(RunCoordinateStatus.id.in_(session.query(latest_ids.c.max_id)))
        .order_by(RunCoordinateStatus.id.asc())
        .all()
    )
    arrow_idx = home.lanes.address_row_index()
    return [
        _cell_from_rc(rc, home=home, arrow_idx=arrow_idx, resolve_output=resolve_output)
        for rc in rows
    ]


def select_cells(
    session: Session,
    home: "Home",
    selection: Selection | str,
    *,
    run_id: Optional[str] = None,
    resolve_output: bool = False,
) -> list[Cell]:
    """Return cells matching a Selection query.

    Without ``run_id``, selection is scoped to ``home.current()`` unless the
    query explicitly asks for ``live:false``. With ``run_id``, the run's RCS
    rows are filtered first, then output/index criteria are applied.
    """
    sel = Selection.parse(selection) if isinstance(selection, str) else selection
    arrow_idx = home.lanes.address_row_index()
    fulfilled_addrs = {
        str(u.address)
        for u in session.query(InputHashUsage.address)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }

    if run_id is None:
        if sel.invalidated is True:
            from .selection import get_selection_addresses

            addresses = get_selection_addresses(session, sel, home=home)
            candidates = _latest_cells_for_addresses(
                session,
                home,
                addresses,
                resolve_output=resolve_output,
            )
        else:
            candidates = get_current_cells(
                session,
                home,
                pipeline=sel.pipeline_id,
                step=sel.step,
                resolve_output=resolve_output,
            )
        return [
            cell
            for cell in candidates
            if _matches_selection(
                cell,
                sel,
                row=arrow_idx.get(cell.output_address) if cell.output_address else None,
                home=home,
                fulfilled_addrs=fulfilled_addrs,
            )
        ]

    query = session.query(RunCoordinateStatus).filter(RunCoordinateStatus.run_id == run_id)
    if sel.pipeline_id:
        query = query.filter(RunCoordinateStatus.pipeline_id == sel.pipeline_id)
    if sel.step:
        query = query.filter(RunCoordinateStatus.step_name == sel.step)
    if sel.source_id:
        query = query.filter(RunCoordinateStatus.source_id == sel.source_id)
    if sel.output_address:
        query = query.filter(RunCoordinateStatus.output_address == sel.output_address)

    candidates = [
        _cell_from_rc(rc, home=home, arrow_idx=arrow_idx, resolve_output=resolve_output)
        for rc in query.order_by(RunCoordinateStatus.id.asc()).all()
    ]
    return [
        cell
        for cell in candidates
        if _matches_selection(
            cell,
            sel,
            row=arrow_idx.get(cell.output_address) if cell.output_address else None,
            home=home,
            fulfilled_addrs=fulfilled_addrs,
        )
    ]


def cell_to_current_output(cell: Cell) -> dict[str, Any]:
    """Convert a Cell to the /api/current-outputs response shape."""
    return {
        "source_id": cell.source_id or "",
        "coordinate": cell.coordinate,
        "status": cell.status,
        "pipeline_id": cell.pipeline_id,
        "step_name": cell.step_name,
        "code_version": cell.code_version,
        "input_hash": cell.input_hash,
        "output_address": cell.output_address,
        "run_id": cell.run_id,
        "updated_at": cell.updated_at,
    }
