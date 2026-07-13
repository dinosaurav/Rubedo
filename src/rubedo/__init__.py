"""
Rubedo: A local-first batch processing engine.

This package provides a framework for defining DAG pipelines over collections of 
coordinates with content-addressed caching, durable run history, and surgical invalidation.
"""
from .spec import (
    step,
    source,
    pipeline,
    PipelineSpec,
    StepSpec,
    PipelineBuilder,
)
from .render import describe
from .models import Filtered, ProcessResult, RunSummary
from .selection import Selection
from .invalidation import invalidate
from .runner import plan, run, RunPlan
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
    "source",
    "pipeline",
    "describe",
    "PipelineSpec",
    "StepSpec",
    "PipelineBuilder",
    "run",
    "plan",
    "RunPlan",
    "TerminalProgress",
]
