"""
FastAPI server for the read-only web UI and invalidation API.
"""
import os
import json
import time
from contextlib import asynccontextmanager
from importlib.resources import files as _resource_files
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Request, Query, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func

from .db import get_session, init_db
from .models import (
    Run,
    RunEvent,
    effective_run_status,
    Materialization,
    MaterializationIndexEntry,
    MaterializationEdge,
    RunCoordinateStatus,
    InputHashUsage,
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
    PipelineOut,
    ObjectMetadataOut,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Rubedo", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count"],
)


def _to_dict(obj):
    """Convert an SQLAlchemy model instance to a dictionary."""
    d = dict(obj.__dict__)
    d.pop("_sa_instance_state", None)
    return d


@app.get("/api/runs", response_model=List[RunListItem])
def get_runs():
    """List all pipeline runs, ordered by most recent."""
    from .queries import get_recent_runs
    with get_session() as session:
        return get_recent_runs(session)


@app.get("/api/runs/{run_id}", response_model=RunDetailOut)
def get_run(run_id: str):
    """Get detailed information for a specific run."""
    from .queries import get_run_summary
    with get_session() as session:
        run_detail = get_run_summary(session, run_id)
        if not run_detail:
            raise HTTPException(404, "Run not found")
        return run_detail


@app.get("/api/runs/{run_id}/stream")
async def stream_run(run_id: str):
    """Stream live progress of a run via Server-Sent Events (SSE)."""
    def event_generator():
        while True:
            with get_session() as session:
                run = session.query(Run).filter_by(id=run_id).first()
                if not run:
                    yield "event: error\ndata: Run not found\n\n"
                    break
                
                # Compute live counts from run_coordinate_statuses
                rows = (
                    session.query(
                        RunCoordinateStatus.step_name,
                        RunCoordinateStatus.status,
                        func.count(RunCoordinateStatus.id)
                    )
                    .filter_by(run_id=run_id)
                    .group_by(RunCoordinateStatus.step_name, RunCoordinateStatus.status)
                    .all()
                )
                
                by_step = {}
                totals = {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
                for step_name, status, count in rows:
                    if step_name not in by_step:
                        by_step[step_name] = {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
                    if status in by_step[step_name]:
                        by_step[step_name][status] = count
                        totals[status] += count
                
                status = effective_run_status(run)
                data = {
                    "status": status,
                    "totals": totals,
                    "by_step": by_step,
                }

                yield f"data: {json.dumps(data)}\n\n"

                if status != "running":
                    break
                    
            time.sleep(0.3)
            
    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/runs/{run_id}/coordinates", response_model=List[RunCoordinateStatusOut])
def get_run_coordinates(run_id: str):
    """Get the status of every coordinate in a specific run."""
    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=run_id).all()
        return [_to_dict(c) for c in coords]


@app.get("/api/runs/{run_id}/events", response_model=List[RunEventOut])
def get_run_events(run_id: str):
    """Get the event log for a specific run."""
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
    """List materializations with pagination."""
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
    """The current outputs: each pipeline's *latest run's* live lanes.

    "Current" is the latest run's lanes — nothing more. A coordinate that
    vanished from a source simply isn't in the latest run, so it isn't current;
    there is no cross-run "removed" bookkeeping.
    """
    with get_session() as session:
        # The latest run per pipeline (by start time).
        latest_started = (
            session.query(
                Run.pipeline_id.label("pid"),
                func.max(Run.started_at).label("mx"),
            )
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
            .subquery()
        )
        latest_ids_subq = (
            session.query(func.max(RunCoordinateStatus.id).label("max_id"))
            .filter(
                RunCoordinateStatus.run_id.in_(session.query(latest_run_ids.c.id))
            )
            .group_by(RunCoordinateStatus.source_id, RunCoordinateStatus.coordinate)
            .subquery()
        )
        rows = (
            session.query(RunCoordinateStatus)
            .filter(RunCoordinateStatus.id.in_(session.query(latest_ids_subq.c.max_id)))
            .filter(RunCoordinateStatus.status.in_(["created", "reused", "filtered"]))
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
                    "pipeline_id": mat.pipeline_id if mat else None,
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


@app.get("/api/runs/{run_id}/search")
def search_run(run_id: str, query: str = Query(..., min_length=1)):
    """Search for a value in a run and return the full lineage trace."""
    with get_session() as session:
        # 1. Find matching materialization IDs for this run
        matching_mat_ids = set()

        coords_match = session.query(RunCoordinateStatus.materialization_id).filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.materialization_id.isnot(None),
            RunCoordinateStatus.coordinate.contains(query)
        ).all()
        for (m_id,) in coords_match:
            matching_mat_ids.add(m_id)

        index_match = session.query(RunCoordinateStatus.materialization_id).join(
            MaterializationIndexEntry,
            RunCoordinateStatus.materialization_id == MaterializationIndexEntry.materialization_id
        ).filter(
            RunCoordinateStatus.run_id == run_id,
            MaterializationIndexEntry.value.contains(query)
        ).all()
        for (m_id,) in index_match:
            matching_mat_ids.add(m_id)

        if not matching_mat_ids:
            return {"trace": []}

        # 2. Get all materializations used in this run
        run_mat_ids = {m_id for (m_id,) in session.query(RunCoordinateStatus.materialization_id).filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.materialization_id.isnot(None)
        ).all()}

        # 3. Get all edges within this run's materializations
        all_edges = session.query(MaterializationEdge).filter(
            (MaterializationEdge.parent_id.in_(run_mat_ids)) | (MaterializationEdge.child_id.in_(run_mat_ids))
        ).all()

        parents = {m: [] for m in run_mat_ids}  # type: ignore
        children = {m: [] for m in run_mat_ids}  # type: ignore
        for e in all_edges:
            if e.child_id in run_mat_ids and e.parent_id in run_mat_ids:
                parents[e.child_id].append(e.parent_id)
                children[e.parent_id].append(e.child_id)

        # 4. BFS to find all related materializations
        queue = list(matching_mat_ids)
        visited = set(matching_mat_ids)

        while queue:
            curr = queue.pop(0)
            for p in parents.get(curr, []):
                if p not in visited:
                    visited.add(p)
                    queue.append(p)
            for c in children.get(curr, []):
                if c not in visited:
                    visited.add(c)
                    queue.append(c)

        # 5. Fetch details for the trace
        results = session.query(RunCoordinateStatus, Materialization).join(
            Materialization, RunCoordinateStatus.materialization_id == Materialization.id
        ).filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.materialization_id.in_(visited)
        ).all()

        trace = []
        for rc, mat in results:
            trace.append({
                "step_name": rc.step_name,
                "coordinate": rc.coordinate,
                "status": rc.status,
                "output_address": rc.output_address,
                "materialization_id": mat.id,
                "is_match": mat.id in matching_mat_ids,
                "created_at": mat.created_at
            })
            
        return {"trace": trace}


@app.get("/api/runs/{run_id}/steps/{step_name}/outputs")
def get_step_outputs(run_id: str, step_name: str, limit: int = Query(50), offset: int = Query(0)):
    """Get all outputs for a specific step in a run."""
    with get_session() as session:
        base_query = session.query(RunCoordinateStatus).filter_by(run_id=run_id, step_name=step_name)
        total = base_query.count()
        rows = base_query.order_by(RunCoordinateStatus.id).limit(limit).offset(offset).all()
        
        items = []
        for rc in rows:
            items.append({
                "coordinate": rc.coordinate,
                "status": rc.status,
                "output_address": rc.output_address,
                "materialization_id": rc.materialization_id,
                "error_message": rc.error_message
            })
        return {"total": total, "items": items}


def _is_fulfilled(session, output_address: str, step_name: Optional[str] = None, pipeline_id: Optional[str] = None) -> bool:
    """Check input_hash_usages.fulfilled for this address."""
    row = session.query(InputHashUsage.fulfilled).filter(
        InputHashUsage.address == output_address,
    ).first()
    return bool(row and row[0])


def _resolve_materialization(session, output_address: str):
    """Latest generation at an address, preferring the fulfilled one."""
    # Check input_hash_usages for fulfilled=True (new liveness gate)
    fulfilled_addrs = {
        u.address for u in session.query(InputHashUsage)
        .filter(InputHashUsage.address == output_address, InputHashUsage.fulfilled.is_(True))
        .all()
    }
    live = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    if live and (not fulfilled_addrs or output_address in fulfilled_addrs):
        return live
    return (
        session.query(Materialization)
        .filter_by(output_address=output_address)
        .order_by(Materialization.id.desc())
        .first()
    )


@app.get("/api/objects/{output_address}", response_model=ObjectMetadataOut)
def get_object_metadata(output_address: str):
    """Get metadata and a preview for a materialized object."""
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
        if not _is_fulfilled(session, output_address):
            usage = (
                session.query(InputHashUsage)
                .filter_by(address=output_address)
                .first()
            )
            if usage:
                invalidated_at = usage.last_run_id
                invalidation_reason = "invalidated"
        mat_data = {
            "pipeline_id": mat.pipeline_id,
            "step_name": mat.step_name,
            "code_version": mat.code_version,
            "created_by_run_id": mat.created_by_run_id,
            "created_at": mat.created_at,
            "is_live": mat.is_live,
            "invalidated_at": invalidated_at,
            "invalidation_reason": invalidation_reason,
            "output_content_hash": mat.output_content_hash,
            "content_type": mat.content_type,
            "index": [
                {"field": e.field, "value": e.value}
                for e in session.query(MaterializationIndexEntry)
                .filter_by(materialization_id=mat.id)
                .order_by(MaterializationIndexEntry.id)
                .all()
            ],
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
def download_object(output_address: str) -> FileResponse:
    """Download the raw bytes of a materialized object."""
    with get_session() as session:
        mat = _resolve_materialization(session, output_address)
        if not mat:
            raise HTTPException(404, "Object not found")
        obj_path = os.path.abspath(mat.output_path)

    if not os.path.exists(obj_path):
        raise HTTPException(404, "Object bytes not found in store")

    return FileResponse(obj_path, filename=output_address)


def _selection_from_payload(data: dict) -> Selection:
    """Accept either structured Selection fields or a selection-language
    string: {"query": "step:extract company:acme live:true"}."""
    if "query" in data:
        try:
            return Selection.parse(data["query"])
        except ValueError as e:
            raise HTTPException(400, str(e))
    return Selection(**data)


@app.post("/api/selection/preview", response_model=SelectionPreviewResponse)
async def preview_selection(request: Request):
    """Preview which materializations match a selection query."""
    data = await request.json()
    sel = _selection_from_payload(data)
    from .selection import get_selection_materialization_ids

    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, sel)
        mats = (
            session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        )
        items = []
        for m in mats:
            items.append(
                {
                    "materialization_id": m.id,
                    "pipeline_id": m.pipeline_id,
                    "step_name": m.step_name,
                    "code_version": m.code_version,
                    "output_address": m.output_address,
                    "output_content_hash": m.output_content_hash,
                    "metadata": json.loads(m.metadata_json) if m.metadata_json else {},  # type: ignore
                    "invalidated": not _is_fulfilled(session, str(m.output_address), str(m.step_name), str(m.pipeline_id)),
                }
            )

        return {
            "materialization_count": len(mats),
            "items": items,
        }


@app.post("/api/selection/invalidate", response_model=SelectionInvalidateResponse)
async def invalidate_selection(request: Request):
    """Invalidate materializations matching a selection query."""
    data = await request.json()
    reason = request.query_params.get("reason", "UI Invalidation")

    sel = _selection_from_payload(data)
    result = invalidate(sel, reason)

    return {
        "run_id": result["run_id"],
        "invalidated_count": result["invalidated_count"],
        "materialization_ids": result["materialization_ids"],
    }






@app.get("/api/pipelines", response_model=List[PipelineOut])
def get_pipelines_api():
    """Ledger-derived: a pipeline appears here once declared or run."""
    with get_session() as session:
        latest_subq = (
            session.query(
                Run.pipeline_id,
                func.max(Run.started_at).label("last_run_at"),
                func.count(Run.id).label("run_count"),
            )
            .filter(Run.pipeline_id.isnot(None), Run.kind.in_(["process", "declaration"]))
            .group_by(Run.pipeline_id)
            .subquery()
        )

        rows = (
            session.query(Run, latest_subq.c.run_count)
            .join(
                latest_subq,
                (Run.pipeline_id == latest_subq.c.pipeline_id)
                & (Run.started_at == latest_subq.c.last_run_at)
            )
            .all()
        )

        out = []
        seen = set()
        for run, run_count in rows:
            if run.pipeline_id in seen:
                continue
            seen.add(run.pipeline_id)
            out.append(
                PipelineOut(
                    id=run.pipeline_id,
                    source_id=getattr(run, 'source_id', ''),
                    run_count=run_count,
                    last_run_id=getattr(run, 'id', ''),
                    last_run_status=effective_run_status(run),
                    last_run_at=getattr(run, 'started_at', ''),
                    last_run_finished_at=getattr(run, 'finished_at', ''),
                    definition=json.loads(getattr(run, 'definition_json', '{}')) if getattr(run, 'definition_json', None) else None,
                )
            )
        return out


def _web_static_dir() -> str | None:
    """Locate the built web assets directory, or None if absent.

    Checks three places, in order:
    1. ``RUBEDO_WEB_DIR`` env var (escape hatch / dev override).
    2. ``rubedo.web_static`` as importlib package data (the normal
       installed-package path — ``web/dist`` is shipped as package data).
    3. ``src/rubedo/web_static`` relative to this file (the editable /
       in-tree path, useful when ``npm run build`` was just run).
    """
    env = os.environ.get("RUBEDO_WEB_DIR")
    if env and os.path.isdir(env):
        return env
    try:
        d = _resource_files("rubedo") / "web_static"
        if d.is_dir():
            return str(d)
    except (FileNotFoundError, ModuleNotFoundError, TypeError):
        pass
    here = os.path.join(os.path.dirname(__file__), "web_static")
    if os.path.isdir(here):
        return here
    return None


_STATIC_DIR = _web_static_dir()

if _STATIC_DIR is not None:
    _static_dir: str = _STATIC_DIR

    @app.get("/{full_path:path}")
    def spa_fallback(full_path: str):
        """Serve the SPA: static files from web_static/, or index.html for
        client-side routes. Unmatched /api paths get a JSON 404."""
        if full_path == "api" or full_path.startswith("api/"):
            raise HTTPException(404, "Not found")
        if full_path:
            candidate = os.path.join(_static_dir, full_path)
            if os.path.isfile(candidate):
                return FileResponse(candidate)
        return FileResponse(os.path.join(_static_dir, "index.html"))

