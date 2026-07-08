"""
Invalidation logic for marking outputs as no longer live.
"""
import uuid
from .models import (
    Run,
    Materialization,
    MaterializationLifecycle,
    RunEvent,
)
from .db import get_session
from .selection import Selection, get_selection_materialization_ids
from .util import utcnow_iso


def invalidate(selection: Selection, reason: str) -> dict:
    """
    Invalidate materializations matching the given selection.

    Args:
        selection (Selection): The criteria for what to invalidate.
        reason (str): The reason for invalidation.

    Returns:
        dict: A summary of the invalidation run.
    """
    run_id = f"run_{uuid.uuid4().hex[:12]}"

    with get_session() as session:
        # Create invalidate run
        now = utcnow_iso()
        run = Run(
            id=run_id,
            kind="invalidate",
            selection_json=selection.model_dump_json(),
            started_at=now,
            last_heartbeat_at=now,
        )
        session.add(run)

        # Log event
        event = RunEvent(
            run_id=run_id,
            timestamp=utcnow_iso(),
            level="info",
            event_type="run_started",
            message=f"Starting invalidation {run_id}",
        )
        session.add(event)
        session.commit()

        try:
            mat_ids = get_selection_materialization_ids(session, selection)

            invalidated_count = 0
            for mat_id in mat_ids:
                mat = session.get(Materialization, mat_id)
                if mat and mat.is_live:
                    mat.is_live = False  # type: ignore
                    session.add(
                        MaterializationLifecycle(
                            materialization_id=mat.id,
                            action="invalidated",
                            run_id=run_id,
                            reason=reason,
                            created_at=utcnow_iso(),
                        )
                    )
                    invalidated_count += 1

            run.status = "completed"  # type: ignore
            run.finished_at = utcnow_iso()  # type: ignore

            event = RunEvent(
                run_id=run_id,
                timestamp=utcnow_iso(),
                level="info",
                event_type="run_completed",
                message=f"Invalidation {run_id} finished, invalidated {invalidated_count} materializations",
            )
            session.add(event)
            session.commit()

            return {
                "run_id": run_id,
                "invalidated_count": invalidated_count,
                "materialization_ids": mat_ids,
            }
        except Exception as e:
            session.rollback()
            run.status = "failed"  # type: ignore
            run.error_message = str(e)  # type: ignore
            run.finished_at = utcnow_iso()  # type: ignore
            session.commit()
            raise e

