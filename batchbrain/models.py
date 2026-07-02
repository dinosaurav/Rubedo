import datetime
from typing import Any, Optional, Dict
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Boolean, JSON, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Run(Base):
    __tablename__ = 'runs'
    id = Column(String, primary_key=True)
    kind = Column(String, nullable=False)
    status = Column(String, nullable=False)
    source_folder = Column(String)
    code_version = Column(String)
    config_hash = Column(String)
    selection_json = Column(String)
    started_at = Column(String, nullable=False)
    finished_at = Column(String)
    error_message = Column(String)
    manifest_id = Column(String)
    parent_manifest_id = Column(String)
    summary_json = Column(String)
    processor_name = Column(String, index=True)

class RunEvent(Base):
    __tablename__ = 'run_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, index=True)
    timestamp = Column(String, nullable=False)
    level = Column(String, nullable=False)
    event_type = Column(String, nullable=False, index=True)
    processor_name = Column(String, index=True)
    step_name = Column(String, index=True)
    coordinate = Column(String, index=True)
    message = Column(String)
    data_json = Column(String)

class Manifest(Base):
    __tablename__ = 'manifests'
    id = Column(String, primary_key=True)
    run_id = Column(String, nullable=False)
    root_path = Column(String, nullable=False)
    manifest_hash = Column(String, nullable=False)
    created_at = Column(String, nullable=False)

class ManifestEntry(Base):
    __tablename__ = 'manifest_entries'
    id = Column(Integer, primary_key=True, autoincrement=True)
    manifest_id = Column(String, nullable=False)
    coordinate = Column(String, nullable=False)
    content_hash = Column(String, nullable=False)
    size_bytes = Column(Integer)
    mtime_ns = Column(Integer)
    __table_args__ = (UniqueConstraint('manifest_id', 'coordinate', name='_manifest_coord_uc'),)

class MaterializationEdge(Base):
    __tablename__ = 'materialization_edges'
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, nullable=False)
    child_id = Column(Integer, nullable=False)
    __table_args__ = (UniqueConstraint('parent_id', 'child_id', name='_mat_edge_uc'),)

class Materialization(Base):
    __tablename__ = 'materializations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    processor_name = Column(String, nullable=False, index=True)
    step_name = Column(String, nullable=False, index=True)
    code_version = Column(String, nullable=False)
    config_hash = Column(String, nullable=False)
    input_hash = Column(String, nullable=False)
    output_address = Column(String, nullable=False, unique=True)
    output_content_hash = Column(String, nullable=False)
    output_path = Column(String, nullable=False)
    metadata_json = Column(String)
    created_at = Column(String, nullable=False)
    created_by_run_id = Column(String, nullable=False)
    invalidated_at = Column(String)
    invalidated_by_run_id = Column(String)
    invalidation_reason = Column(String)

class RunCoordinateStatus(Base):
    __tablename__ = 'run_coordinate_statuses'
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False, index=True)
    processor_name = Column(String, index=True)
    step_name = Column(String, nullable=False, index=True)
    source_folder = Column(String, nullable=False)
    coordinate = Column(String, nullable=False, index=True)
    input_hash = Column(String)
    output_address = Column(String)
    materialization_id = Column(Integer)
    previous_output_address = Column(String)
    previous_materialization_id = Column(Integer)
    status = Column(String, nullable=False, index=True)
    error_message = Column(String)
    error_type = Column(String, index=True)
    metadata_json = Column(String)
    created_at = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('run_id', 'coordinate', 'step_name', name='_run_coord_uc'),)



class ExecutionRequest(Base):
    __tablename__ = 'execution_requests'
    id = Column(String, primary_key=True)
    processor_id = Column(String, nullable=False)
    status = Column(String, nullable=False) # queued, running, succeeded, failed
    requested_at = Column(String, nullable=False)
    started_at = Column(String)
    finished_at = Column(String)
    run_id = Column(String)
    force = Column(Integer, nullable=False, default=0)
    input_json = Column(String, nullable=False, default='{}')
    folder_override = Column(String)
    workers_override = Column(Integer)
    error_message = Column(String)
    stdout_path = Column(String)
    stderr_path = Column(String)

class ProcessResult(BaseModel):
    value: Any
    metadata: Optional[Dict[str, Any]] = None

class RunSummary(BaseModel):
    run_id: str
    status: str
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    removed_count: int = 0
