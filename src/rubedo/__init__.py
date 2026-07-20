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
from .home import Home
from .models import Filtered, RunSummary
from .selection import Selection
from .queries import Cell
from .scope import RunScope
from .diff import CellDiff, RunDiff, ValueChange
from .schemas import RunListItem
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
    "Home",
    "RunSummary",
    "Cell",
    "CellDiff",
    "RunDiff",
    "RunListItem",
    "ValueChange",
    "Selection",
    "RunScope",
    "step",
    "pipeline",
    "Pipeline",
    "PipelineSpec",
    "StepSpec",
    "RunPlan",
    "TerminalProgress",
]
