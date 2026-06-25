import uuid
import datetime
import json
import traceback
import concurrent.futures
from typing import Callable, Any, Optional

from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert

from .models import (
    Run, Event, SourceFile, Materialization, 
    RunCoordinate, CurrentOutput, RunSummary, ProcessResult
)
from .db import get_session
from .scanner import scan_folder
from .hashing import hash_json, compute_output_address
from .store import stage_and_commit

def _emit_event(session: Session, run_id: str, level: str, event_type: str, coordinate: Optional[str] = None, message: Optional[str] = None, data: Optional[dict] = None):
    event = Event(
        run_id=run_id,
        timestamp=datetime.datetime.utcnow().isoformat() + "Z",
        level=level,
        event_type=event_type,
        coordinate=coordinate,
        message=message,
        data_json=json.dumps(data) if data else None
    )
    session.add(event)

def run_process(
    folder: str,
    fn: Callable[[str], Any],
    code_version: str,
    config: Optional[dict[str, Any]] = None,
    step: str = "process_file",
    workers: int = 4,
    force: bool = False,
) -> RunSummary:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    config = config or {}
    config_hash = hash_json(config)
    
    with get_session() as session:
        run = Run(
            id=run_id,
            kind="process",
            status="running",
            source_folder=folder,
            step=step,
            code_version=code_version,
            config_hash=config_hash,
            started_at=datetime.datetime.utcnow().isoformat() + "Z",
        )
        session.add(run)
        _emit_event(session, run_id, "info", "run_started", message=f"Starting run {run_id}")
        session.commit()

        try:
            # Scan folder
            scanned_files = scan_folder(folder)
            
            # Upsert source files
            now_iso = datetime.datetime.utcnow().isoformat() + "Z"
            for sf in scanned_files:
                stmt = insert(SourceFile).values(
                    source_folder=folder,
                    coordinate=sf.coordinate,
                    content_hash=sf.content_hash,
                    size_bytes=sf.size_bytes,
                    mtime_ns=sf.mtime_ns,
                    observed_at=now_iso
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['source_folder', 'coordinate'],
                    set_=dict(
                        content_hash=stmt.excluded.content_hash,
                        size_bytes=stmt.excluded.size_bytes,
                        mtime_ns=stmt.excluded.mtime_ns,
                        observed_at=stmt.excluded.observed_at
                    )
                )
                session.execute(stmt)
            session.commit()
            
            scanned_coordinates = {sf.coordinate for sf in scanned_files}
            prior_current = session.query(CurrentOutput).filter_by(
                source_folder=folder,
                step=step,
                code_version=code_version,
                config_hash=config_hash
            ).all()
            
            prior_coords = {pc.coordinate: pc for pc in prior_current}
            removed_coordinates = set(prior_coords.keys()) - scanned_coordinates
            
            removed_count = 0
            for coord in removed_coordinates:
                old = prior_coords[coord]
                session.delete(old)
                
                rc = RunCoordinate(
                    run_id=run_id,
                    source_folder=folder,
                    coordinate=coord,
                    input_hash=old.input_hash,
                    output_address=old.output_address,
                    materialization_id=old.materialization_id,
                    status="removed"
                )
                session.add(rc)
                
                _emit_event(
                    session, run_id, "info", "coordinate_removed",
                    coordinate=coord,
                    message="Coordinate removed from current outputs because source file is absent",
                    data={
                        "previous_output_address": old.output_address,
                        "previous_materialization_id": old.materialization_id,
                    }
                )
                removed_count += 1
                
            session.commit()

            tasks = []
            
            # Prepare work
            for sf in scanned_files:
                output_address = compute_output_address(step, code_version, sf.content_hash, config_hash)
                
                # Check for existing valid materialization
                existing_mat = session.query(Materialization).filter_by(
                    output_address=output_address,
                    invalidated_at=None
                ).first()
                
                if existing_mat and not force:
                    # Reuse
                    rc = RunCoordinate(
                        run_id=run_id,
                        source_folder=folder,
                        coordinate=sf.coordinate,
                        input_hash=sf.content_hash,
                        output_address=output_address,
                        materialization_id=existing_mat.id,
                        status="reused"
                    )
                    session.add(rc)
                    _emit_event(session, run_id, "info", "file_reused", coordinate=sf.coordinate)
                else:
                    # Need to compute
                    tasks.append({
                        "coordinate": sf.coordinate,
                        "absolute_path": sf.absolute_path,
                        "input_hash": sf.content_hash,
                        "output_address": output_address,
                    })
            
            session.commit()

            # Execute tasks
            created_count = 0
            failed_count = 0
            
            if tasks:
                def process_task(task_spec):
                    try:
                        result = fn(task_spec["absolute_path"])
                        return True, task_spec, result, None
                    except Exception as e:
                        return False, task_spec, None, traceback.format_exc()

                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = [executor.submit(process_task, t) for t in tasks]
                    for future in concurrent.futures.as_completed(futures):
                        success, task_spec, result, error_trace = future.result()
                        
                        coordinate = task_spec["coordinate"]
                        output_address = task_spec["output_address"]
                        input_hash = task_spec["input_hash"]
                        
                        with get_session() as task_session:
                            if success:
                                # Stage and commit
                                try:
                                    final_path, output_content_hash = stage_and_commit(
                                        run_id, coordinate, output_address, result
                                    )
                                    
                                    metadata_json = None
                                    if isinstance(result, ProcessResult) and result.metadata:
                                        metadata_json = json.dumps(result.metadata)

                                    # Insert materialization
                                    # In SQLite, if output_address is unique and there's a conflict, it means
                                    # another parallel process or run inserted it. But in our case output_address 
                                    # is theoretically perfectly deterministic, so if it's inserted, we could skip.
                                    # For MVP, we might get an IntegrityError if force=True and multiple workers try.
                                    # We will just catch it or ignore. We use a simple save:
                                    mat = Materialization(
                                        step=step,
                                        code_version=code_version,
                                        config_hash=config_hash,
                                        input_hash=input_hash,
                                        output_address=output_address,
                                        output_content_hash=output_content_hash,
                                        output_path=final_path,
                                        metadata_json=metadata_json,
                                        created_at=datetime.datetime.utcnow().isoformat() + "Z",
                                        created_by_run_id=run_id
                                    )
                                    task_session.add(mat)
                                    try:
                                        task_session.flush() # to get mat.id
                                    except Exception as db_e:
                                        task_session.rollback()
                                        # Likely a unique constraint violation on output_address. Fetch it.
                                        mat = task_session.query(Materialization).filter_by(output_address=output_address).first()
                                        if not mat:
                                            raise db_e
                                            
                                        # If it was previously invalidated, we just recomputed it and got the exact same output!
                                        # We should clear the invalidation so it can be reused again.
                                        if mat.invalidated_at is not None:
                                            mat.invalidated_at = None
                                            mat.invalidated_by_run_id = None
                                            mat.invalidation_reason = None
                                    # Insert/update current outputs
                                    stmt = insert(CurrentOutput).values(
                                        source_folder=folder,
                                        coordinate=coordinate,
                                        step=step,
                                        code_version=code_version,
                                        config_hash=config_hash,
                                        input_hash=input_hash,
                                        output_address=output_address,
                                        materialization_id=mat.id,
                                        updated_at=datetime.datetime.utcnow().isoformat() + "Z"
                                    )
                                    stmt = stmt.on_conflict_do_update(
                                        index_elements=['source_folder', 'coordinate', 'step', 'code_version', 'config_hash'],
                                        set_=dict(
                                            input_hash=stmt.excluded.input_hash,
                                            output_address=stmt.excluded.output_address,
                                            materialization_id=stmt.excluded.materialization_id,
                                            updated_at=stmt.excluded.updated_at
                                        )
                                    )
                                    task_session.execute(stmt)
                                    
                                    # Insert run coordinate
                                    rc = RunCoordinate(
                                        run_id=run_id,
                                        source_folder=folder,
                                        coordinate=coordinate,
                                        input_hash=input_hash,
                                        output_address=output_address,
                                        materialization_id=mat.id,
                                        status="created"
                                    )
                                    task_session.add(rc)
                                    
                                    _emit_event(task_session, run_id, "info", "file_succeeded", coordinate=coordinate)
                                    created_count += 1
                                    
                                except Exception as e:
                                    # Staging/committing failed
                                    task_session.rollback()
                                    error_msg = traceback.format_exc()
                                    rc = RunCoordinate(
                                        run_id=run_id,
                                        source_folder=folder,
                                        coordinate=coordinate,
                                        input_hash=input_hash,
                                        output_address=output_address,
                                        status="failed",
                                        error_message=error_msg
                                    )
                                    task_session.add(rc)
                                    _emit_event(task_session, run_id, "error", "file_failed", coordinate=coordinate, message=str(e))
                                    failed_count += 1
                            else:
                                # Function execution failed
                                rc = RunCoordinate(
                                    run_id=run_id,
                                    source_folder=folder,
                                    coordinate=coordinate,
                                    input_hash=input_hash,
                                    output_address=output_address,
                                    status="failed",
                                    error_message=error_trace
                                )
                                task_session.add(rc)
                                _emit_event(task_session, run_id, "error", "file_failed", coordinate=coordinate, message=error_trace)
                                failed_count += 1
                                
                            task_session.commit()
            
            # Finish run
            with get_session() as final_session:
                final_run = final_session.query(Run).filter_by(id=run_id).first()
                if failed_count == 0:
                    final_run.status = "succeeded"
                elif created_count == 0 and len(scanned_files) > 0 and len(tasks) > 0:
                    final_run.status = "failed"
                else:
                    final_run.status = "partial"
                
                final_status = final_run.status
                final_run.finished_at = datetime.datetime.utcnow().isoformat() + "Z"
                _emit_event(final_session, run_id, "info", "run_finished", message=f"Run finished with status {final_run.status}")
                final_session.commit()
                
            reused_count = len(scanned_files) - len(tasks)
            return RunSummary(
                run_id=run_id,
                status=final_status,
                created_count=created_count,
                reused_count=reused_count,
                failed_count=failed_count,
                removed_count=removed_count
            )
            
        except Exception as e:
            with get_session() as err_session:
                err_run = err_session.query(Run).filter_by(id=run_id).first()
                if err_run:
                    err_run.status = "failed"
                    err_run.error_message = traceback.format_exc()
                    err_run.finished_at = datetime.datetime.utcnow().isoformat() + "Z"
                    _emit_event(err_session, run_id, "error", "run_failed", message=str(e))
                    err_session.commit()
            raise
