"""
Pydantic schemas for the FastAPI server.
"""
from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class RunListItem(BaseModel):
    """Summary of a run, typically for list views."""
    id: str
    kind: str
    status: str
    source_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    started_at: str
    finished_at: Optional[str] = None
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    blocked_count: int = 0
    filtered_count: int = 0


class RunDetailOut(RunListItem):
    """Detailed view of a run including DAG snapshot and step counts."""
    error_message: Optional[str] = None
    # DAG snapshot recorded at run start, and per-step outcome counts
    definition: Optional[Dict[str, Any]] = None
    by_step: Optional[Dict[str, Dict[str, int]]] = None


class RunCoordinateStatusOut(BaseModel):
    """Status of a single coordinate for a step during a run."""
    coordinate: str
    step_name: Optional[str] = None
    status: str
    pipeline_id: Optional[str] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    materialization_id: Optional[int] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    metadata_json: Optional[str] = None
    created_at: Optional[str] = None


class RunEventOut(BaseModel):
    """A single log event from a run."""
    timestamp: str
    level: str
    event_type: str
    pipeline_id: Optional[str] = None
    coordinate: Optional[str] = None
    message: Optional[str] = None
    data_json: Optional[str] = None


class MaterializationOut(BaseModel):
    """A materialized output from a step."""
    id: int
    pipeline_id: str
    step_name: str
    code_version: str
    input_hash: str
    output_address: str
    output_content_hash: str
    content_type: Optional[str] = None
    metadata_json: Optional[str] = None
    created_at: str
    is_live: bool


class CurrentOutputOut(BaseModel):
    """The latest live output for a given coordinate."""
    source_id: str
    coordinate: str
    status: str
    pipeline_id: Optional[str] = None
    step_name: Optional[str] = None
    code_version: Optional[str] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    materialization_id: Optional[int] = None
    run_id: str
    updated_at: Optional[str] = None


class SelectionPreviewItem(BaseModel):
    """A materialization matched by a selection query."""
    materialization_id: int
    pipeline_id: str
    step_name: str
    code_version: str
    output_address: str
    output_content_hash: str
    metadata: Dict[str, Any]
    invalidated: bool


class SelectionPreviewResponse(BaseModel):
    """The full set of materializations matched by a selection query."""
    materialization_count: int
    items: List[SelectionPreviewItem]


class SelectionInvalidateResponse(BaseModel):
    """Result of an invalidation request."""
    run_id: str
    invalidated_count: int
    materialization_ids: List[int]


class PipelineOut(BaseModel):
    """A pipeline definition as seen by the engine."""
    id: str
    source_id: Optional[str] = None
    run_count: int
    last_run_id: Optional[str] = None
    last_run_status: Optional[str] = None
    last_run_at: Optional[str] = None
    last_run_finished_at: Optional[str] = None
    # DAG snapshot recorded by the most recent run (steps, edges, policies)
    definition: Optional[Dict[str, Any]] = None


class MaterializationIndexEntryOut(BaseModel):
    """One indexed field/value pair for a materialization."""
    field: str
    value: str


class ObjectMetadataOut(BaseModel):
    """Metadata and a content preview for a materialized object."""
    output_address: str
    exists: bool
    size_bytes: int
    preview_kind: str  # "text" | "json" | "binary"
    preview_text: Optional[str] = None
    preview_json: Optional[Any] = None
    pipeline_id: str
    step_name: str
    code_version: str
    created_by_run_id: str
    created_at: str
    is_live: bool
    invalidated_at: Optional[str] = None
    invalidation_reason: Optional[str] = None
    output_content_hash: str
    content_type: Optional[str] = None
    index: List[MaterializationIndexEntryOut]
