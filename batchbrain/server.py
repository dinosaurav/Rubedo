import os
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from .db import get_session, init_db
from .models import Run, Event, Materialization, RunCoordinate, CurrentOutput
from .selection import Selection
from .invalidation import invalidate, recompute

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

@app.get("/api/runs")
def get_runs():
    with get_session() as session:
        runs = session.query(Run).order_by(Run.started_at.desc()).all()
        return [_to_dict(r) for r in runs]

@app.get("/api/runs/{run_id}")
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
        
        rd = _to_dict(run)
        rd["created_count"] = created
        rd["reused_count"] = reused
        rd["failed_count"] = failed
        
        return rd

@app.get("/api/runs/{run_id}/coordinates")
def get_run_coordinates(run_id: str):
    with get_session() as session:
        coords = session.query(RunCoordinate).filter_by(run_id=run_id).all()
        return [_to_dict(c) for c in coords]

@app.get("/api/runs/{run_id}/events")
def get_run_events(run_id: str):
    with get_session() as session:
        events = session.query(Event).filter_by(run_id=run_id).order_by(Event.id).all()
        return [_to_dict(e) for e in events]

@app.get("/api/materializations")
def get_materializations():
    with get_session() as session:
        mats = session.query(Materialization).order_by(Materialization.created_at.desc()).limit(100).all()
        return [_to_dict(m) for m in mats]

@app.get("/api/current-outputs")
def get_current_outputs():
    with get_session() as session:
        outputs = session.query(CurrentOutput).order_by(CurrentOutput.coordinate).all()
        return [_to_dict(o) for o in outputs]

@app.get("/api/objects/{output_address}")
def get_object(output_address: str):
    with get_session() as session:
        mat = session.query(Materialization).filter_by(output_address=output_address).first()
        if not mat:
            raise HTTPException(404, "Object not found")
        rd = _to_dict(mat)
        
        # Read contents if text/json
        try:
            with open(mat.output_path, 'r', encoding='utf-8') as f:
                content = f.read()
                try:
                    rd["preview_json"] = json.loads(content)
                except:
                    rd["preview_text"] = content
        except UnicodeDecodeError:
            rd["preview_binary"] = True
            
        return rd

@app.get("/api/objects/{output_address}/download")
def download_object(output_address: str):
    with get_session() as session:
        mat = session.query(Materialization).filter_by(output_address=output_address).first()
        if not mat:
            raise HTTPException(404, "Object not found")
        return FileResponse(mat.output_path)

@app.post("/api/selection/preview")
def selection_preview(selection: Selection):
    from .selection import get_selection_materialization_ids
    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, selection)
        mats = session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        return [_to_dict(m) for m in mats]

@app.post("/api/selection/invalidate")
def selection_invalidate(selection: Selection, reason: str = "Invalidated via API"):
    res = invalidate(selection, reason=reason)
    return res

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
