from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

class RunListItem(BaseModel):
    id: str
    kind: str
    status: str
    source_folder: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    started_at: str
    finished_at: Optional[str] = None
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    removed_count: int = 0

class RunDetailOut(RunListItem):
    error_message: Optional[str] = None

class RunCoordinateOut(BaseModel):
    coordinate: str
    status: str
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    materialization_id: Optional[int] = None
    error_message: Optional[str] = None
    metadata_json: Optional[str] = None

class EventOut(BaseModel):
    timestamp: str
    level: str
    event_type: str
    coordinate: Optional[str] = None
    message: Optional[str] = None
    data_json: Optional[str] = None

class MaterializationOut(BaseModel):
    id: int
    step: str
    code_version: str
    input_hash: str
    output_address: str
    output_content_hash: str
    metadata_json: Optional[str] = None
    created_at: str
    invalidated_at: Optional[str] = None
    invalidation_reason: Optional[str] = None



class SelectionPreviewItem(BaseModel):
    materialization_id: int
    coordinate: Optional[str] = None
    step: str
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
    step: str
    code_version: str
    workers: int
    allow_folder_override: bool
    input_schema: Optional[Dict[str, Any]] = None
    default_inputs: Optional[Dict[str, Any]] = None

class RunProcessorRequest(BaseModel):
    inputs: Dict[str, Any] = Field(default_factory=dict)
    force: bool = False
    folder: Optional[str] = None
    workers: Optional[int] = None

class RunProcessorResponse(BaseModel):
    execution_id: str
    status: str

class ExecutionRequestOut(BaseModel):
    id: str
    processor_id: str
    status: str
    requested_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    run_id: Optional[str] = None
    force: bool
    input_json: str
    folder_override: Optional[str] = None
    workers_override: Optional[int] = None
    error_message: Optional[str] = None
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None

