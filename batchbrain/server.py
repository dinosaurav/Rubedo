import os
import json
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .db import get_session, init_db
from .models import Run, Event, Materialization, RunCoordinate, CurrentOutput
from .selection import Selection
from .invalidation import invalidate, recompute
from .schemas import (
    RunListItem, RunDetailOut, RunCoordinateOut, EventOut,
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
            # calculate counts
            coords = session.query(RunCoordinate).filter_by(run_id=run.id).all()
            created = sum(1 for c in coords if c.status == "created")
            reused = sum(1 for c in coords if c.status == "reused")
            failed = sum(1 for c in coords if c.status == "failed")
            
            d = _to_dict(run)
            d['created_count'] = created
            d['reused_count'] = reused
            d['failed_count'] = failed
            results.append(d)
        return results

@app.get("/api/runs/{run_id}", response_model=RunDetailOut)
def get_run(run_id: str):
    with get_session() as session:
        run = session.query(Run).filter_by(id=run_id).first()
        if not run:
            raise HTTPException(404, "Run not found")
        
        # calculate counts
        coords = session.query(RunCoordinate).filter_by(run_id=run_id).all()
        created = sum(1 for c in coords if c.status == "created")
        reused = sum(1 for c in coords if c.status == "reused")
        failed = sum(1 for c in coords if c.status == "failed")
        
        d = _to_dict(run)
        d['created_count'] = created
        d['reused_count'] = reused
        d['failed_count'] = failed
        return d

@app.get("/api/runs/{run_id}/coordinates", response_model=List[RunCoordinateOut])
def get_run_coordinates(run_id: str):
    with get_session() as session:
        coords = session.query(RunCoordinate).filter_by(run_id=run_id).all()
        return [_to_dict(c) for c in coords]

@app.get("/api/runs/{run_id}/events", response_model=List[EventOut])
def get_run_events(run_id: str):
    with get_session() as session:
        events = session.query(Event).filter_by(run_id=run_id).order_by(Event.id).all()
        return [_to_dict(e) for e in events]

@app.get("/api/materializations", response_model=List[MaterializationOut])
def get_materializations(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    with get_session() as session:
        mats = session.query(Materialization).order_by(Materialization.id.desc()).limit(limit).offset(offset).all()
        return [_to_dict(m) for m in mats]

@app.get("/api/current-outputs", response_model=List[CurrentOutputOut])
def get_current_outputs():
    with get_session() as session:
        outputs = session.query(CurrentOutput).all()
        return [_to_dict(o) for o in outputs]

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
    sel_data = data.get("selection", {})
    reason = data.get("reason", "UI Invalidation")
    
    sel = Selection(**sel_data)
    result = invalidate(sel, reason)
    
    return {
        "run_id": result["run_id"],
        "invalidated_count": result["invalidated_count"],
        "materialization_ids": result["materialization_ids"]
    }

@app.get("/api/runs/{left_run_id}/diff/{right_run_id}")
def run_diff(left_run_id: str, right_run_id: str):
    with get_session() as session:
        left_coords = {c.coordinate: c for c in session.query(RunCoordinate).filter_by(run_id=left_run_id).all()}
        right_coords = {c.coordinate: c for c in session.query(RunCoordinate).filter_by(run_id=right_run_id).all()}
        
        all_keys = set(left_coords.keys()) | set(right_coords.keys())
        diff = []
        for k in all_keys:
            lc = left_coords.get(k)
            rc = right_coords.get(k)
            
            status = "changed"
            if not lc: status = "added"
            elif not rc: status = "removed"
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
