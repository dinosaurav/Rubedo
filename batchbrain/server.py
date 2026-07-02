import os
import json
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, HTTPException, Request, Query, Response
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func

from .db import get_session, init_db
from .models import (
    Run,
    RunEvent,
    Materialization,
    MaterializationLifecycle,
    RunCoordinateStatus,
)
from .selection import Selection
from .invalidation import invalidate
from .schemas import (
    RunListItem,
    RunDetailOut,
    RunCoordinateStatusOut,
    RunEventOut,
    MaterializationOut,
    CurrentOutputOut,
    SelectionPreviewResponse,
    SelectionInvalidateResponse,
    ProcessorSpecOut,
)

from .registry import list_processors

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="BatchBrain", lifespan=lifespan)

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
    d.pop("_sa_instance_state", None)
    return d


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
                except Exception:
                    pass
            d["created_count"] = summary.get("created", 0)
            d["reused_count"] = summary.get("reused", 0)
            d["failed_count"] = summary.get("failed", 0)
            d["removed_count"] = summary.get("removed", 0)
            d["blocked_count"] = summary.get("blocked", 0)
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
            except Exception:
                pass
        d["created_count"] = summary.get("created", 0)
        d["reused_count"] = summary.get("reused", 0)
        d["failed_count"] = summary.get("failed", 0)
        d["removed_count"] = summary.get("removed", 0)
        d["blocked_count"] = summary.get("blocked", 0)
        return d


@app.get("/api/runs/{run_id}/coordinates", response_model=List[RunCoordinateStatusOut])
def get_run_coordinates(run_id: str):
    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=run_id).all()
        return [_to_dict(c) for c in coords]


@app.get("/api/runs/{run_id}/events", response_model=List[RunEventOut])
def get_run_events(run_id: str):
    with get_session() as session:
        events = (
            session.query(RunEvent).filter_by(run_id=run_id).order_by(RunEvent.id).all()
        )
        return [_to_dict(e) for e in events]


@app.get("/api/materializations", response_model=List[MaterializationOut])
def get_materializations(
    response: Response,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    with get_session() as session:
        total = session.query(Materialization).count()
        mats = (
            session.query(Materialization)
            .order_by(Materialization.id.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        response.headers["X-Total-Count"] = str(total)
        return [_to_dict(m) for m in mats]


@app.get("/api/current-outputs", response_model=List[CurrentOutputOut])
def get_current_outputs():
    with get_session() as session:
        latest_ids_subq = (
            session.query(func.max(RunCoordinateStatus.id).label("max_id"))
            .group_by(RunCoordinateStatus.source_id, RunCoordinateStatus.coordinate)
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
                mat = (
                    session.query(Materialization)
                    .filter_by(id=rc.materialization_id)
                    .first()
                )
            if mat and not mat.is_live:
                continue
            results.append(
                {
                    "source_id": rc.source_id,
                    "coordinate": rc.coordinate,
                    "status": rc.status,
                    "processor_name": mat.processor_name if mat else None,
                    "step_name": mat.step_name if mat else None,
                    "code_version": mat.code_version if mat else None,
                    "input_hash": rc.input_hash,
                    "output_address": rc.output_address,
                    "materialization_id": rc.materialization_id,
                    "run_id": rc.run_id,
                    # when the output bytes were produced, not when a run
                    # last confirmed them (reuse bumps rc rows every run)
                    "updated_at": mat.created_at if mat else rc.created_at,
                }
            )
        return results


def _resolve_materialization(session, output_address: str):
    """Latest generation at an address, preferring the live one."""
    live = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    if live:
        return live
    return (
        session.query(Materialization)
        .filter_by(output_address=output_address)
        .order_by(Materialization.id.desc())
        .first()
    )


@app.get("/api/objects/{output_address}")
def get_object_metadata(output_address: str):
    with get_session() as session:
        mat = _resolve_materialization(session, output_address)
        if not mat:
            raise HTTPException(404, "Object not found")
        obj_path = os.path.abspath(mat.output_path)

    if not os.path.exists(obj_path):
        raise HTTPException(404, "Object bytes not found in store")

    size = os.path.getsize(obj_path)

    # Try to preview first 4KB
    preview_kind = "binary"
    preview_text = None
    preview_json = None

    if size < 1024 * 1024 * 10:  # Only try to preview if < 10MB
        try:
            with open(obj_path, "r", encoding="utf-8") as f:
                content = f.read(4096)
                preview_text = content
                preview_kind = "text"
                try:
                    preview_json = json.loads(content)
                    preview_kind = "json"
                except Exception:
                    pass
        except UnicodeDecodeError:
            pass  # It's binary

    # Fetch the materialization data; when/why it stopped being live is
    # derived from the append-only lifecycle log
    with get_session() as session:
        mat = _resolve_materialization(session, output_address)
        invalidated_at = None
        invalidation_reason = None
        if not mat.is_live:
            lc = (
                session.query(MaterializationLifecycle)
                .filter_by(materialization_id=mat.id)
                .order_by(MaterializationLifecycle.id.desc())
                .first()
            )
            if lc:
                invalidated_at = lc.created_at
                invalidation_reason = lc.reason
        mat_data = {
            "processor_name": mat.processor_name,
            "step_name": mat.step_name,
            "code_version": mat.code_version,
            "created_by_run_id": mat.created_by_run_id,
            "created_at": mat.created_at,
            "is_live": mat.is_live,
            "invalidated_at": invalidated_at,
            "invalidation_reason": invalidation_reason,
            "output_content_hash": mat.output_content_hash,
            "content_type": mat.content_type,
        }

    return {
        "output_address": output_address,
        "exists": True,
        "size_bytes": size,
        "preview_kind": preview_kind,
        "preview_text": preview_text,
        "preview_json": preview_json,
        **mat_data,
    }


@app.get("/api/objects/{output_address}/download")
def download_object(output_address: str):
    with get_session() as session:
        mat = _resolve_materialization(session, output_address)
        if not mat:
            raise HTTPException(404, "Object not found")
        obj_path = os.path.abspath(mat.output_path)

    if not os.path.exists(obj_path):
        raise HTTPException(404, "Object bytes not found in store")

    return FileResponse(obj_path, filename=output_address)


@app.post("/api/selection/preview", response_model=SelectionPreviewResponse)
async def preview_selection(request: Request):
    data = await request.json()
    sel = Selection(**data)
    from .selection import get_selection_materialization_ids

    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, sel)
        mats = (
            session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        )
        items = []
        for m in mats:
            _to_dict(m)
            items.append(
                {
                    "materialization_id": m.id,
                    "coordinate": None,
                    "processor_name": m.processor_name,
                    "step_name": m.step_name,
                    "code_version": m.code_version,
                    "output_address": m.output_address,
                    "output_content_hash": m.output_content_hash,
                    "metadata": json.loads(m.metadata_json) if m.metadata_json else {},
                    "invalidated": not m.is_live,
                }
            )

        return {
            "materialization_count": len(mats),
            "coordinate_count": len(mats),
            "items": items,
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
        "materialization_ids": result["materialization_ids"],
    }


@app.get("/api/runs/{left_run_id}/diff/{right_run_id}")
def run_diff(left_run_id: str, right_run_id: str):
    with get_session() as session:
        left_coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=left_run_id)
            .all()
        }
        right_coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=right_run_id)
            .all()
        }

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
                    status = "unchanged"  # removed to removed
            elif not rc or rc.status == "removed":
                status = "removed"
            elif lc.output_address == rc.output_address and lc.status == rc.status:
                status = "unchanged"

            diff.append(
                {
                    "coordinate": k,
                    "status": status,
                    "left_output_address": lc.output_address if lc else None,
                    "right_output_address": rc.output_address if rc else None,
                    "left_status": lc.status if lc else None,
                    "right_status": rc.status if rc else None,
                }
            )
        return diff





@app.get("/api/processors", response_model=List[ProcessorSpecOut])
def get_processors_api():
    processors = list_processors()
    out = []
    for p in processors:
        schema = None
        defaults = None
        first_step = p.steps[0] if p.steps else None
        params_model = first_step.params_model if first_step else None

        if params_model:
            schema = params_model.model_json_schema()
            # Extract defaults if any
            defaults = {
                k: v.default
                for k, v in params_model.model_fields.items()
                if not v.is_required()
            }

        out.append(
            ProcessorSpecOut(
                id=p.id,
                name=p.name,
                source_id=p.source.id,
                step_name=first_step.name if first_step else "",
                code_version=first_step.version if first_step else "",
                workers=first_step.workers if first_step else 4,
                params_schema=schema,
                default_params=defaults or {},
            )
        )
    return out

