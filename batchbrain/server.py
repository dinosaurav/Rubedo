import os
import json
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, Query, Response
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func
from sqlalchemy.orm import Session

from .db import get_session, init_db
from .models import Run, RunEvent, Materialization, RunCoordinateStatus
from .selection import Selection
from .invalidation import invalidate, recompute
from .schemas import (
    RunListItem, RunDetailOut, RunCoordinateStatusOut, RunEventOut,
    MaterializationOut, CurrentOutputOut, SelectionPreviewResponse,
    SelectionPreviewItem, SelectionInvalidateResponse
)

app = FastAPI(title="BatchBrain")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"],
)

def _to_dict(obj):
    d = dict(obj.__dict__)
    d.pop('_sa_instance_state', None)
    return d

@app.on_event("startup")
def startup():
    init_db()

@app.get("/api/runs", response_model=List[RunListItem])
def get_runs():
    with get_session() as session:
        runs = session.query(Run).order_by(Run.started_at.desc()).all()
        results = []
        for run in runs:
            d = _to_dict(run)
            summary = {}
            if run.summary_json:
                try:
                    summary = json.loads(run.summary_json)
                except:
                    pass
            d['created_count'] = summary.get('created', 0)
            d['reused_count'] = summary.get('reused', 0)
            d['failed_count'] = summary.get('failed', 0)
            d['removed_count'] = summary.get('removed', 0)
            results.append(d)
        return results

@app.get("/api/runs/{run_id}", response_model=RunDetailOut)
def get_run(run_id: str):
    with get_session() as session:
        run = session.query(Run).filter_by(id=run_id).first()
        if not run:
            raise HTTPException(404, "Run not found")
        
        d = _to_dict(run)
        summary = {}
        if run.summary_json:
            try:
                summary = json.loads(run.summary_json)
            except:
                pass
        d['created_count'] = summary.get('created', 0)
        d['reused_count'] = summary.get('reused', 0)
        d['failed_count'] = summary.get('failed', 0)
        d['removed_count'] = summary.get('removed', 0)
        return d

@app.get("/api/runs/{run_id}/coordinates", response_model=List[RunCoordinateStatusOut])
def get_run_coordinates(run_id: str):
    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=run_id).all()
        return [_to_dict(c) for c in coords]

@app.get("/api/runs/{run_id}/events", response_model=List[RunEventOut])
def get_run_events(run_id: str):
    with get_session() as session:
        events = session.query(RunEvent).filter_by(run_id=run_id).order_by(RunEvent.id).all()
        return [_to_dict(e) for e in events]

@app.get("/api/materializations", response_model=List[MaterializationOut])
def get_materializations(response: Response, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with get_session() as session:
        total = session.query(Materialization).count()
        mats = session.query(Materialization).order_by(Materialization.id.desc()).limit(limit).offset(offset).all()
        response.headers["X-Total-Count"] = str(total)
        return [_to_dict(m) for m in mats]

@app.get("/api/current-outputs", response_model=List[CurrentOutputOut])
def get_current_outputs():
    with get_session() as session:
        latest_ids_subq = (
            session.query(func.max(RunCoordinateStatus.id).label("max_id"))
            .group_by(RunCoordinateStatus.source_folder, RunCoordinateStatus.coordinate)
            .subquery()
        )
        rows = (
            session.query(RunCoordinateStatus)
            .filter(RunCoordinateStatus.id.in_(session.query(latest_ids_subq.c.max_id)))
            .filter(RunCoordinateStatus.status.in_(["created", "reused"]))
            .all()
        )

        results = []
        for rc in rows:
            mat = None
            if rc.materialization_id is not None:
                mat = session.query(Materialization).filter_by(id=rc.materialization_id).first()
            if mat and mat.invalidated_at is not None:
                continue
            results.append({
                "source_folder": rc.source_folder,
                "coordinate": rc.coordinate,
                "status": rc.status,
                "step": mat.step if mat else None,
                "code_version": mat.code_version if mat else None,
                "input_hash": rc.input_hash,
                "output_address": rc.output_address,
                "materialization_id": rc.materialization_id,
                "run_id": rc.run_id,
            })
        return results


@app.get("/api/objects/{output_address}")
def get_object_metadata(output_address: str):
    obj_path = os.path.abspath(os.path.join(".batchbrain/objects", output_address[:2], output_address[2:4], output_address))
    
    if not os.path.exists(obj_path):
        raise HTTPException(404, "Object not found")
        
    size = os.path.getsize(obj_path)
    
    # Try to preview first 4KB
    preview_kind = "binary"
    preview_text = None
    preview_json = None
    
    if size < 1024 * 1024 * 10: # Only try to preview if < 10MB
        try:
            with open(obj_path, 'r', encoding='utf-8') as f:
                content = f.read(4096)
                preview_text = content
                preview_kind = "text"
                try:
                    preview_json = json.loads(content)
                    preview_kind = "json"
                except:
                    pass
        except UnicodeDecodeError:
            pass # It's binary
            
    # Fetch the materialization data
    mat_data = {}
    with get_session() as session:
        mat = session.query(Materialization).filter_by(output_address=output_address).first()
        if mat:
            mat_data = {
                "step": mat.step,
                "code_version": mat.code_version,
                "created_by_run_id": mat.created_by_run_id,
                "created_at": mat.created_at,
                "invalidated_at": mat.invalidated_at,
                "output_content_hash": mat.output_content_hash,
            }

    return {
        "output_address": output_address,
        "exists": True,
        "size_bytes": size,
        "preview_kind": preview_kind,
        "preview_text": preview_text,
        "preview_json": preview_json,
        **mat_data
    }

@app.get("/api/objects/{output_address}/download")
def download_object(output_address: str):
    obj_path = os.path.abspath(os.path.join(".batchbrain/objects", output_address[:2], output_address[2:4], output_address))
    
    if not os.path.exists(obj_path):
        raise HTTPException(404, "Object not found")
        
    return FileResponse(obj_path, filename=output_address)

@app.post("/api/selection/preview", response_model=SelectionPreviewResponse)
async def preview_selection(request: Request):
    data = await request.json()
    sel = Selection(**data)
    from .selection import get_selection_materialization_ids
    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, sel)
        mats = session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        items = []
        for m in mats:
            d = _to_dict(m)
            items.append({
                "materialization_id": m.id,
                "coordinate": None,
                "step": m.step,
                "code_version": m.code_version,
                "output_address": m.output_address,
                "output_content_hash": m.output_content_hash,
                "metadata": json.loads(m.metadata_json) if m.metadata_json else {},
                "invalidated": m.invalidated_at is not None
            })
        
        return {
            "materialization_count": len(mats),
            "coordinate_count": len(mats),
            "items": items
        }

@app.post("/api/selection/invalidate", response_model=SelectionInvalidateResponse)
async def invalidate_selection(request: Request):
    data = await request.json()
    reason = request.query_params.get("reason", "UI Invalidation")
    
    sel = Selection(**data)
    result = invalidate(sel, reason)
    
    return {
        "run_id": result["run_id"],
        "invalidated_count": result["invalidated_count"],
        "materialization_ids": result["materialization_ids"]
    }

@app.get("/api/runs/{left_run_id}/diff/{right_run_id}")
def run_diff(left_run_id: str, right_run_id: str):
    with get_session() as session:
        left_coords = {c.coordinate: c for c in session.query(RunCoordinateStatus).filter_by(run_id=left_run_id).all()}
        right_coords = {c.coordinate: c for c in session.query(RunCoordinateStatus).filter_by(run_id=right_run_id).all()}
        
        all_keys = set(left_coords.keys()) | set(right_coords.keys())
        diff = []
        for k in all_keys:
            lc = left_coords.get(k)
            rc = right_coords.get(k)
            
            status = "changed"
            if not lc or lc.status == "removed":
                if rc and rc.status != "removed":
                    status = "added"
                else:
                    status = "unchanged" # removed to removed
            elif not rc or rc.status == "removed":
                status = "removed"
            elif lc.output_address == rc.output_address and lc.status == rc.status:
                status = "unchanged"
                
            diff.append({
                "coordinate": k,
                "status": status,
                "left_output_address": lc.output_address if lc else None,
                "right_output_address": rc.output_address if rc else None,
                "left_status": lc.status if lc else None,
                "right_status": rc.status if rc else None,
            })
        return diff

from datetime import datetime, timezone
import uuid
import subprocess
from .registry import list_processors, get_processor
from .models import ExecutionRequest
from .schemas import ProcessorSpecOut, RunProcessorRequest, RunProcessorResponse, ExecutionRequestOut

@app.get('/api/processors', response_model=List[ProcessorSpecOut])
def get_processors_api():
    processors = list_processors()
    out = []
    for p in processors:
        schema = None
        defaults = None
        if p.input_model:
            schema = p.input_model.model_json_schema()
            # Extract defaults if any
            defaults = {k: v.default for k, v in p.input_model.model_fields.items() if not v.is_required()}
            
        out.append(ProcessorSpecOut(
            id=p.id,
            name=p.name,
            folder=p.folder,
            step=p.step,
            code_version=p.code_version,
            workers=p.workers,
            allow_folder_override=p.allow_folder_override,
            input_schema=schema,
            default_inputs=defaults or {}
        ))
    return out

@app.post('/api/processors/{processor_id}/run', response_model=RunProcessorResponse)
def run_processor_api(processor_id: str, req: RunProcessorRequest):
    spec = get_processor(processor_id)
    
    if req.folder and not spec.allow_folder_override:
        raise HTTPException(400, 'Folder override not allowed for this processor')
        
    if spec.input_model:
        try:
            # Validate against model
            spec.input_model.model_validate(req.inputs)
        except Exception as e:
            raise HTTPException(400, f'Invalid inputs: {str(e)}')
            
    exec_id = f'exec_{uuid.uuid4().hex[:12]}'
    
    with get_session() as session:
        ex = ExecutionRequest(
            id=exec_id,
            processor_id=processor_id,
            status='queued',
            requested_at=datetime.now(timezone.utc).isoformat(),
            force=int(req.force),
            input_json=json.dumps(req.inputs),
            folder_override=req.folder,
            workers_override=req.workers
        )
        
        # Prepare log paths
        base_dir = os.path.join('.batchbrain', 'executions', exec_id)
        os.makedirs(base_dir, exist_ok=True)
        out_path = os.path.join(base_dir, 'stdout.log')
        err_path = os.path.join(base_dir, 'stderr.log')
        
        ex.stdout_path = out_path
        ex.stderr_path = err_path
        
        session.add(ex)
        session.commit()
        
    import sys
    out_f = open(out_path, 'w')
    err_f = open(err_path, 'w')
    
    subprocess.Popen(
        [sys.executable, '-m', 'batchbrain.processor_worker', exec_id],
        stdout=out_f,
        stderr=err_f,
        cwd=os.getcwd(),
        env=os.environ.copy()
    )
    
    out_f.close()
    err_f.close()
    
    return RunProcessorResponse(execution_id=exec_id, status='queued')

@app.get('/api/executions', response_model=List[ExecutionRequestOut])
def get_executions():
    with get_session() as session:
        reqs = session.query(ExecutionRequest).order_by(ExecutionRequest.requested_at.desc()).all()
        return [_to_dict(r) for r in reqs]

@app.get('/api/executions/{execution_id}', response_model=ExecutionRequestOut)
def get_execution(execution_id: str):
    with get_session() as session:
        req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
        if not req:
            raise HTTPException(404, 'Execution not found')
        # Map force back to boolean
        d = _to_dict(req)
        d['force'] = bool(d['force'])
        return d

@app.get('/api/executions/{execution_id}/stdout')
def get_execution_stdout(execution_id: str):
    with get_session() as session:
        req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
        if not req or not req.stdout_path or not os.path.exists(req.stdout_path):
            return PlainTextResponse('')
        with open(req.stdout_path, 'r') as f:
            return PlainTextResponse(f.read())

@app.get('/api/executions/{execution_id}/stderr')
def get_execution_stderr(execution_id: str):
    with get_session() as session:
        req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
        if not req or not req.stderr_path or not os.path.exists(req.stderr_path):
            return PlainTextResponse('')
        with open(req.stderr_path, 'r') as f:
            return PlainTextResponse(f.read())

