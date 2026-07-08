"""
Read-only query layer for the Rubedo engine.

These functions provide a shared interface for querying the ledger state,
used by both the HTTP server and the CLI.
"""
import json
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from .models import Run, RunCoordinateStatus, effective_run_status
from .schemas import RunListItem, RunDetailOut


def _to_dict(obj):
    """Convert an SQLAlchemy model instance to a dictionary."""
    d = dict(obj.__dict__)
    d.pop("_sa_instance_state", None)
    return d


def get_recent_runs(session: Session, limit: int = 50) -> List[RunListItem]:
    """List recent pipeline runs, ordered by most recent."""
    runs = session.query(Run).order_by(Run.started_at.desc()).limit(limit).all()
    results = []
    for run in runs:
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
        results.append(RunListItem(**d))
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
