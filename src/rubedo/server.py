"""
FastAPI server for the read-only web UI and invalidation API.
"""
import os
import json
import time
from contextlib import asynccontextmanager
from importlib.resources import files as _resource_files
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Request, Query, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func

from .db import get_session, init_db
from .models import (
    Run,
    RunEvent,
    effective_run_status,
    MaterializationEdge,
    RunCoordinateStatus,
    InputHashUsage,
)
from . import lane_store
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
        # Build the list from Arrow rows + IHU liveness, not Materialization.
        all_rows = lane_store.all_filled_rows()
        # Deduplicate by address (latest ts wins), sorted newest first
        by_addr: Dict[str, dict] = {}
        for row in all_rows:
            addr = row.get("address", "")
            existing = by_addr.get(addr)
            if existing is None or (row.get("ts") and existing.get("ts") and row["ts"] > existing["ts"]):
                by_addr[addr] = row
        sorted_rows = sorted(
            by_addr.values(),
            key=lambda r: (r.get("ts") or ""),
            reverse=True,
        )
        total = len(sorted_rows)
        page = sorted_rows[offset : offset + limit]
        # Liveness for the is_live field
        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .all()
        }
        response.headers["X-Total-Count"] = str(total)
        return [
            {
                "id": 0,  # transitional: integer id gone once materializations table deleted
                "pipeline_id": r.get("pipeline_id", ""),
                "step_name": r.get("step_name", ""),
                "code_version": r.get("code_version") or "",
                "input_hash": r.get("input_hash", ""),
                "output_address": r.get("address", ""),
                "output_content_hash": r.get("content_hash") or "",
                "content_type": r.get("content_type"),
                "metadata_json": None,
                "created_at": str(r.get("ts", "")),
                "is_live": r.get("address", "") in fulfilled_addrs,
            }
            for r in page
        ]


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
        # Build an address→row index from Arrow for metadata lookups
        arrow_idx = lane_store.address_row_index()
        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .all()
        }
        for rc in rows:
            addr = str(rc.output_address) if rc.output_address else None
            if not addr:
                continue
            arrow_row = arrow_idx.get(addr)
            if not arrow_row:
                continue
            # Skip non-fulfilled (dead) outputs
            if addr not in fulfilled_addrs:
                continue
            results.append(
                {
                    "source_id": rc.source_id,
                    "coordinate": rc.coordinate,
                    "status": rc.status,
                    "pipeline_id": arrow_row.get("pipeline_id"),
                    "step_name": arrow_row.get("step_name"),
                    "code_version": arrow_row.get("code_version"),
                    "input_hash": rc.input_hash,
                    "output_address": rc.output_address,
                    "materialization_id": None,
                    "run_id": rc.run_id,
                    "updated_at": str(arrow_row.get("ts", "")) if arrow_row.get("ts") else rc.created_at,
                }
            )
        return results


@app.get("/api/runs/{run_id}/search")
def search_run(run_id: str, query: str = Query(..., min_length=1)):
    """Search for a value in a run and return the full lineage trace."""
    with get_session() as session:
        # 1. Find matching addresses for this run
        matching_addrs = set()

        coords_match = session.query(RunCoordinateStatus.output_address).filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.output_address.isnot(None),
            RunCoordinateStatus.coordinate.contains(query)
        ).all()
        for (addr,) in coords_match:
            matching_addrs.add(str(addr))

        # Indexed-field substring search: scan Arrow files for this run's
        # steps, then map matching addresses back.
        run_rcs = session.query(
            RunCoordinateStatus.output_address,
            RunCoordinateStatus.pipeline_id,
            RunCoordinateStatus.step_name,
        ).filter(
            RunCoordinateStatus.run_id == run_id,
            RunCoordinateStatus.output_address.isnot(None),
        ).all()

        run_addrs = {str(r.output_address) for r in run_rcs}
        seen_steps = set()
        for r in run_rcs:
            key = (r.pipeline_id, r.step_name)
            if key in seen_steps:
                continue
            seen_steps.add(key)
            for addr in lane_store.search_indexed_values(r.pipeline_id, r.step_name, query):
                if addr in run_addrs:
                    matching_addrs.add(addr)

        if not matching_addrs:
            return {"trace": []}

        # 2. Build edge graph via MaterializationEdge (address-based)
        all_edges = session.query(
            MaterializationEdge.parent_address,
            MaterializationEdge.child_address,
        ).filter(
            (MaterializationEdge.parent_address.in_(run_addrs))
            | (MaterializationEdge.child_address.in_(run_addrs))
        ).all()

        parents: Dict[str, List[str]] = {a: [] for a in run_addrs}
        children: Dict[str, List[str]] = {a: [] for a in run_addrs}
        for e in all_edges:
            p, c = str(e.parent_address), str(e.child_address)
            if p in run_addrs and c in run_addrs:
                parents[c].append(p)
                children[p].append(c)

        # 3. BFS to find all related addresses
        queue = list(matching_addrs)
        visited = set(matching_addrs)

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

        # 4. Fetch details for the trace (RCS + Arrow)
        arrow_idx = lane_store.address_row_index()
        rcs_rows = (
            session.query(RunCoordinateStatus)
            .filter(
                RunCoordinateStatus.run_id == run_id,
                RunCoordinateStatus.output_address.isnot(None),
            )
            .filter(RunCoordinateStatus.output_address.in_(visited))
            .all()
        )

        trace = []
        for rc in rcs_rows:
            addr = str(rc.output_address)
            arrow_row = arrow_idx.get(addr, {})
            trace.append({
                "step_name": rc.step_name,
                "coordinate": rc.coordinate,
                "status": rc.status,
                "output_address": rc.output_address,
                "materialization_id": None,
                "is_match": addr in matching_addrs,
                "created_at": str(arrow_row.get("ts", "")) if arrow_row.get("ts") else rc.created_at,
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
                "materialization_id": None,
                "error_message": rc.error_message
            })
        return {"total": total, "items": items}


def _is_fulfilled(session, output_address: str, step_name: Optional[str] = None, pipeline_id: Optional[str] = None) -> bool:
    """Check input_hash_usages.fulfilled for this address."""
    row = session.query(InputHashUsage.fulfilled).filter(
        InputHashUsage.address == output_address,
    ).first()
    return bool(row and row[0])


def _resolve_arrow_row(output_address: str) -> Optional[Dict[str, Any]]:
    """Resolve an output_address to its Arrow lane_store row (latest by ts).
    Returns None if no Arrow row exists for this address."""
    return lane_store.address_row_index().get(output_address)


@app.get("/api/objects/{output_address}", response_model=ObjectMetadataOut)
def get_object_metadata(output_address: str):
    """Get metadata and a preview for a materialized object."""
    arrow_row = _resolve_arrow_row(output_address)
    if not arrow_row:
        raise HTTPException(404, "Object not found")
    obj_path = os.path.abspath(arrow_row.get("output_path", ""))

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

    with get_session() as session:
        is_live = _is_fulfilled(session, output_address)
        invalidated_at = None
        invalidation_reason = None
        if not is_live:
            usage = (
                session.query(InputHashUsage)
                .filter_by(address=output_address)
                .first()
            )
            if usage:
                invalidated_at = usage.last_run_id
                invalidation_reason = "invalidated"
        mat_data = {
            "pipeline_id": arrow_row.get("pipeline_id", ""),
            "step_name": arrow_row.get("step_name", ""),
            "code_version": arrow_row.get("code_version") or "",
            "created_by_run_id": str(arrow_row.get("run_id", "")),
            "created_at": str(arrow_row.get("ts", "")),
            "is_live": is_live,
            "invalidated_at": invalidated_at,
            "invalidation_reason": invalidation_reason,
            "output_content_hash": arrow_row.get("content_hash") or "",
            "content_type": arrow_row.get("content_type"),
            "index": [
                {"field": field, "value": val}
                for field, val in lane_store.get_index_values(
                    arrow_row.get("pipeline_id", ""), arrow_row.get("step_name", ""), output_address
                )
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
    arrow_row = _resolve_arrow_row(output_address)
    if not arrow_row:
        raise HTTPException(404, "Object not found")
    obj_path = os.path.abspath(arrow_row.get("output_path", ""))

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
    from .selection import get_selection_addresses

    with get_session() as session:
        addrs = get_selection_addresses(session, sel)
        arrow_idx = lane_store.address_row_index()
        items = []
        for addr in addrs:
            row = arrow_idx.get(addr)
            if not row:
                continue
            items.append(
                {
                    "materialization_id": 0,
                    "pipeline_id": row.get("pipeline_id", ""),
                    "step_name": row.get("step_name", ""),
                    "code_version": row.get("code_version") or "",
                    "output_address": addr,
                    "output_content_hash": row.get("content_hash") or "",
                    "metadata": {},
                    "invalidated": not _is_fulfilled(session, addr),
                }
            )

        return {
            "materialization_count": len(items),
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
        "addresses": result["addresses"],
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

