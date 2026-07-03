from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class RunListItem(BaseModel):
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
    removed_count: int = 0
    blocked_count: int = 0
    filtered_count: int = 0


class RunDetailOut(RunListItem):
    error_message: Optional[str] = None


class RunCoordinateStatusOut(BaseModel):
    coordinate: str
    status: str
    pipeline_id: Optional[str] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    materialization_id: Optional[int] = None
    previous_output_address: Optional[str] = None
    previous_materialization_id: Optional[int] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    metadata_json: Optional[str] = None
    created_at: Optional[str] = None


class RunEventOut(BaseModel):
    timestamp: str
    level: str
    event_type: str
    pipeline_id: Optional[str] = None
    coordinate: Optional[str] = None
    message: Optional[str] = None
    data_json: Optional[str] = None


class MaterializationOut(BaseModel):
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
    materialization_id: int
    coordinate: Optional[str] = None
    pipeline_id: str
    step_name: str
    code_version: str
    output_address: str
    output_content_hash: str
    metadata: Dict[str, Any]
    invalidated: bool


class SelectionPreviewResponse(BaseModel):
    materialization_count: int
    coordinate_count: int
    items: List[SelectionPreviewItem]


class SelectionInvalidateResponse(BaseModel):
    run_id: str
    invalidated_count: int
    materialization_ids: List[int]


class PipelineOut(BaseModel):
    id: str
    name: str
    source_id: str
    step_name: str
    code_version: str
    workers: int
    params_schema: Optional[Dict[str, Any]] = None
    default_params: Optional[Dict[str, Any]] = None
