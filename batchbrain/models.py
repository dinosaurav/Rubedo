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
    step = Column(String)
    code_version = Column(String)
    config_hash = Column(String)
    selection_json = Column(String)
    started_at = Column(String, nullable=False)
    finished_at = Column(String)
    error_message = Column(String)

class Event(Base):
    __tablename__ = 'events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String)
    timestamp = Column(String, nullable=False)
    level = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    coordinate = Column(String)
    message = Column(String)
    data_json = Column(String)

class SourceFile(Base):
    __tablename__ = 'source_files'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_folder = Column(String, nullable=False)
    coordinate = Column(String, nullable=False)
    content_hash = Column(String, nullable=False)
    size_bytes = Column(Integer)
    mtime_ns = Column(Integer)
    observed_at = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('source_folder', 'coordinate', name='_source_coord_uc'),)

class Materialization(Base):
    __tablename__ = 'materializations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    step = Column(String, nullable=False)
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

class RunCoordinate(Base):
    __tablename__ = 'run_coordinates'
    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String, nullable=False)
    source_folder = Column(String, nullable=False)
    coordinate = Column(String, nullable=False)
    input_hash = Column(String)
    output_address = Column(String)
    materialization_id = Column(Integer)
    status = Column(String, nullable=False)
    error_message = Column(String)
    metadata_json = Column(String)
    __table_args__ = (UniqueConstraint('run_id', 'coordinate', name='_run_coord_uc'),)

class CurrentOutput(Base):
    __tablename__ = 'current_outputs'
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_folder = Column(String, nullable=False)
    coordinate = Column(String, nullable=False)
    step = Column(String, nullable=False)
    code_version = Column(String, nullable=False)
    config_hash = Column(String, nullable=False)
    input_hash = Column(String, nullable=False)
    output_address = Column(String, nullable=False)
    materialization_id = Column(Integer, nullable=False)
    updated_at = Column(String, nullable=False)
    __table_args__ = (UniqueConstraint('source_folder', 'coordinate', 'step', 'code_version', 'config_hash', name='_curr_output_uc'),)

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
