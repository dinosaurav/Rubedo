"""
Batchit: A local-first batch processing engine.

This package provides a framework for defining DAG pipelines over collections of 
coordinates with content-addressed caching, durable run history, and surgical invalidation.
"""
from .spec import (
    step,
    pipeline,
    describe,
    PipelineSpec,
    StepSpec,
)
from .models import Filtered, ProcessResult, RunSummary
from .selection import Selection
from .sources import Source, SourceItem, FolderSource, CsvSource
from .invalidation import invalidate
from .runner import plan, run, RunPlan

__all__ = [
    "invalidate",
    "Filtered",
    "ProcessResult",
    "RunSummary",
    "Selection",
    "Source",
    "SourceItem",
    "FolderSource",
    "CsvSource",
    "step",
    "pipeline",
    "describe",
    "PipelineSpec",
    "StepSpec",
    "run",
    "plan",
    "RunPlan",
]
