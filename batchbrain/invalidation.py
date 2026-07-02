import uuid
import datetime
from typing import Any, Callable, Optional
from sqlalchemy.orm import Session
from .models import Run, Materialization, RunEvent, RunSummary
from .db import get_session
from .selection import Selection, get_selection_materialization_ids
from .runner import run_pipeline

def invalidate(selection: Selection, reason: str) -> dict:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    
    with get_session() as session:
        # Create invalidate run
        run = Run(
            id=run_id,
            kind="invalidate",
            status="running",
            selection_json=selection.model_dump_json(),
            started_at=datetime.datetime.utcnow().isoformat() + "Z",
        )
        session.add(run)
        
        # Log event
        event = RunEvent(
            run_id=run_id,
            timestamp=datetime.datetime.utcnow().isoformat() + "Z",
            level="info",
            event_type="run_started",
            message=f"Starting invalidation {run_id}"
        )
        session.add(event)
        session.commit()
        
        try:
            mat_ids = get_selection_materialization_ids(session, selection)
            
            invalidated_count = 0
            for mat_id in mat_ids:
                mat = session.query(Materialization).get(mat_id)
                if mat and mat.invalidated_at is None:
                    mat.invalidated_at = datetime.datetime.utcnow().isoformat() + "Z"
                    mat.invalidated_by_run_id = run_id
                    mat.invalidation_reason = reason
                    
                    invalidated_count += 1
            
            run.status = "completed"
            run.finished_at = datetime.datetime.utcnow().isoformat() + "Z"
            
            event = RunEvent(
                run_id=run_id,
                timestamp=datetime.datetime.utcnow().isoformat() + "Z",
                level="info",
                event_type="run_completed",
                message=f"Invalidation {run_id} finished, invalidated {invalidated_count} materializations"
            )
            session.add(event)
            session.commit()
            
            return {
                "run_id": run_id,
                "invalidated_count": invalidated_count,
                "materialization_ids": mat_ids
            }
        except Exception as e:
            run.status = "failed"
            run.error_message = str(e)
            run.finished_at = datetime.datetime.utcnow().isoformat() + "Z"
            session.commit()
            raise e

def recompute(
    selection: Selection,
    pipeline, # PipelineSpec
    config: Optional[dict[str, Any]] = None,
    workers: Optional[int] = None,
    force: bool = True,
    inputs: Optional[dict] = None
) -> RunSummary:
    """
    For MVP, recompute invalidates the selection and then runs the pipeline on the source folder.
    """
    if not selection.source_folder:
        raise ValueError("Recompute requires source_folder in MVP")
    
    invalidate(selection, reason="Recompute triggered")
    
    return run_pipeline(
        pipeline=pipeline,
        folder=selection.source_folder,
        config=config,
        workers=workers,
        force=False, # Since we invalidated, they will be recreated
        inputs=inputs
    )
