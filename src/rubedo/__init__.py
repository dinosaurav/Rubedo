"""
Rubedo: A local-first batch processing engine.

This package provides a framework for defining DAG pipelines over collections of
coordinates with content-addressed caching, durable run history, and surgical invalidation.
"""
from .spec import (
    step,
    PipelineSpec,
    StepSpec,
)
from .pipeline import Pipeline, pipeline
from .models import Filtered, ProcessResult, RunSummary
from .selection import Selection
from .invalidation import invalidate
from .runner import RunPlan
from .progress import TerminalProgress
from .trace import trace, TraceNode, TraceResult

__all__ = [
    "invalidate",
    "trace",
    "TraceNode",
    "TraceResult",
    "Filtered",
    "ProcessResult",
    "RunSummary",
    "Selection",
    "step",
    "pipeline",
    "Pipeline",
    "PipelineSpec",
    "StepSpec",
    "RunPlan",
    "TerminalProgress",
]
