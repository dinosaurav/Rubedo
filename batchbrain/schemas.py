from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class RunListItem(BaseModel):
    id: str
    kind: str
    status: str
    source_folder: Optional[str] = None
    processor_name: Optional[str] = None
    step_name: Optional[str] = None
    code_version: Optional[str] = None
    started_at: str
    finished_at: Optional[str] = None
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    removed_count: int = 0
    blocked_count: int = 0


class RunDetailOut(RunListItem):
    error_message: Optional[str] = None


class RunCoordinateStatusOut(BaseModel):
    coordinate: str
    status: str
    processor_name: Optional[str] = None
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
    processor_name: Optional[str] = None
    coordinate: Optional[str] = None
    message: Optional[str] = None
    data_json: Optional[str] = None


class MaterializationOut(BaseModel):
    id: int
    processor_name: str
    step_name: str
    code_version: str
    input_hash: str
    output_address: str
    output_content_hash: str
    metadata_json: Optional[str] = None
    created_at: str
    invalidated_at: Optional[str] = None
    invalidation_reason: Optional[str] = None


class CurrentOutputOut(BaseModel):
    source_folder: str
    coordinate: str
    status: str
    processor_name: Optional[str] = None
    step_name: Optional[str] = None
    code_version: Optional[str] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    materialization_id: Optional[int] = None
    run_id: str


class SelectionPreviewItem(BaseModel):
    materialization_id: int
    coordinate: Optional[str] = None
    processor_name: str
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


class ProcessorSpecOut(BaseModel):
    id: str
    name: str
    folder: str
    step_name: str
    code_version: str
    workers: int
    allow_folder_override: bool
    input_schema: Optional[Dict[str, Any]] = None
    default_inputs: Optional[Dict[str, Any]] = None
