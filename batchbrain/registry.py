import re
from typing import Callable, Optional, Dict, Any, Tuple, Type, List
from pydantic import BaseModel
from dataclasses import dataclass
import importlib.util
import sys
import os

from .sources import Source

_RATE_PERIODS = {"s": 1.0, "sec": 1.0, "second": 1.0,
                 "m": 60.0, "min": 60.0, "minute": 60.0,
                 "h": 3600.0, "hour": 3600.0}


def parse_rate_limit(spec: str) -> Tuple[int, float]:
    """'10/min' -> (10, 60.0). Raises on anything unparseable."""
    m = re.fullmatch(r"\s*(\d+)\s*/\s*([a-z]+)\s*", spec.lower())
    if not m or m.group(2) not in _RATE_PERIODS:
        raise ValueError(
            f"Invalid rate_limit {spec!r}: expected '<count>/<s|min|hour>'"
        )
    count = int(m.group(1))
    if count < 1:
        raise ValueError(f"Invalid rate_limit {spec!r}: count must be >= 1")
    return count, _RATE_PERIODS[m.group(2)]


_DURATION_UNITS = {"s": 1.0, "sec": 1.0, "second": 1.0,
                   "m": 60.0, "min": 60.0, "minute": 60.0,
                   "h": 3600.0, "hour": 3600.0,
                   "d": 86400.0, "day": 86400.0}


def parse_duration(spec: str) -> float:
    """'24h' -> 86400.0 seconds. Raises on anything unparseable."""
    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([a-z]+?)s?\s*", spec.lower())
    if not m or m.group(2) not in _DURATION_UNITS:
        raise ValueError(
            f"Invalid duration {spec!r}: expected '<number><s|min|h|d>'"
        )
    return float(m.group(1)) * _DURATION_UNITS[m.group(2)]


@dataclass
class StepSpec:
    name: str
    fn: Callable
    version: str
    depends_on: List[str]
    config_hash: str
    params_model: Optional[Type[BaseModel]] = None
    config: Optional[Dict[str, Any]] = None
    workers: int = 4
    code_hash: Optional[str] = None
    code_mode: str = "warn"  # warn | auto
    retries: int = 0
    retry_on: Tuple[Type[BaseException], ...] = (Exception,)
    retry_delay: float = 0.0
    retry_backoff: float = 1.0
    rate_limit: Optional[Tuple[int, float]] = None  # (count, period_seconds)
    stale_after: Optional[float] = None  # seconds; None = never stale
    skip_cache: bool = False  # inline util: never materialized, fused into consumers


@dataclass
class PipelineSpec:
    id: str
    name: str
    source: Source
    steps: List[StepSpec]


_REGISTRY: Dict[str, PipelineSpec] = {}


def clear_registry():
    _REGISTRY.clear()


def _hash_source(fn: Callable) -> Optional[str]:
    import inspect

    from .hashing import hash_text

    try:
        return hash_text(inspect.getsource(fn))
    except (OSError, TypeError):
        return None


def step(
    name: str,
    version: str,
    depends_on: Optional[List[str]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    config: Optional[Dict[str, Any]] = None,
    workers: int = 4,
    code: str = "warn",
    retries: int = 0,
    retry_on=Exception,
    retry_delay: float = 0.0,
    retry_backoff: float = 1.0,
    rate_limit: Optional[str] = None,
    stale_after: Optional[str] = None,
    skip_cache: bool = False,
):
    """Declare a step.

    version is the step's semantic identity — bump it for deliberate
    behavior changes (also the escape hatch for edits code hashing can't
    see, like helpers the step calls).

    code decides what a *source edit* means, independently of version:
      - "warn" (default): edits never recompute; reusing an output whose
        code has since changed produces a loud warning. Right for
        expensive/non-deterministic steps.
      - "auto": the function's source hash joins the cache identity, so any
        edit recomputes — no version bump needed. Right for cheap,
        deterministic steps.

    retries re-runs a failed execution up to `retries` extra times, but only
    for exceptions matching retry_on — narrow it to transient error types
    (timeouts, rate-limit responses); retrying a deterministic bug on an
    expensive step just multiplies its cost. retry_delay seconds separate
    attempts, multiplied by retry_backoff each time. Attempts are recorded
    as run events.

    rate_limit ("10/min", "2/s", "500/hour") paces the step's executions
    across all of its workers, retries included.

    stale_after ("24h", "30min", "7d") expires outputs: a cached output
    older than this re-executes on the next run. A recompute that produces
    different bytes supersedes the old generation; identical bytes refresh
    its clock. Natural for scraped or otherwise time-sensitive data.

    skip_cache marks an inline util: the step is never materialized or
    recorded — its identity (version/code/config) fuses into its consumers'
    cache keys, and it executes lazily (memoized per run) only when a
    consumer actually runs. Intended for quick, idempotent helpers that
    exist to keep other steps readable. Values pass in memory without a
    serialization round-trip, and execution policies (retries, rate_limit)
    are not applied — if a step needs those, it deserves materialization.
    """
    if code not in ("warn", "auto"):
        raise ValueError(f"Step '{name}': code must be 'warn' or 'auto', got {code!r}")
    if version == "auto":
        raise ValueError(
            f"Step '{name}': version is a semantic label; use code='auto' "
            "to derive cache identity from the source instead"
        )
    if retries < 0:
        raise ValueError(f"Step '{name}': retries must be >= 0")
    if skip_cache and stale_after is not None:
        raise ValueError(
            f"Step '{name}': stale_after is meaningless with skip_cache — "
            "nothing is stored to expire"
        )
    if isinstance(retry_on, type) and issubclass(retry_on, BaseException):
        retry_on = (retry_on,)
    parsed_rate = parse_rate_limit(rate_limit) if rate_limit else None
    parsed_stale = parse_duration(stale_after) if stale_after else None

    def decorator(fn: Callable):
        from .hashing import hash_json

        code_hash = _hash_source(fn)
        if code == "auto" and code_hash is None:
            raise ValueError(
                f"Step '{name}': code='auto' requires an inspectable "
                "function source"
            )

        config_hash = hash_json(config or {})
        return StepSpec(
            name=name,
            fn=fn,
            version=version,
            depends_on=depends_on or [],
            config_hash=config_hash,
            params_model=params_model,
            config=config,
            workers=workers,
            code_hash=code_hash,
            code_mode=code,
            retries=retries,
            retry_on=tuple(retry_on),
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            rate_limit=parsed_rate,
            stale_after=parsed_stale,
            skip_cache=skip_cache,
        )

    return decorator


def pipeline(
    name: str,
    folder: Optional[str] = None,
    steps: Optional[List[StepSpec]] = None,
    id: Optional[str] = None,
    source: Optional[Source] = None,
):
    if (source is None) == (folder is None):
        raise ValueError("Pass exactly one of source= or folder= (FolderSource sugar)")
    if source is None:
        from .sources import FolderSource

        source = FolderSource(folder)

    consumed = {dep for s in (steps or []) for dep in s.depends_on}
    for s in steps or []:
        if s.skip_cache and s.name not in consumed:
            raise ValueError(
                f"Step '{s.name}' has skip_cache but no consumer: its output "
                "would never be computed or stored"
            )

    pipe_id = id or name
    spec = PipelineSpec(
        id=pipe_id,
        name=name,
        source=source,
        steps=steps or [],
    )
    _REGISTRY[pipe_id] = spec
    return spec


def list_pipelines() -> List[PipelineSpec]:
    load_pipelines_module()
    return list(_REGISTRY.values())


def get_pipeline(pipeline_id: str) -> PipelineSpec:
    load_pipelines_module()
    if pipeline_id not in _REGISTRY:
        raise ValueError(f"Pipeline '{pipeline_id}' not found.")
    return _REGISTRY[pipeline_id]


def load_pipelines_module(path: Optional[str] = None):
    if path is None:
        path = os.environ.get("BATCHBRAIN_PIPELINES", "batchbrain_pipelines.py")
    if os.path.exists(path):
        module_name = "batchbrain_pipelines"
        if module_name not in sys.modules:
            spec = importlib.util.spec_from_file_location(
                module_name, os.path.abspath(path)
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
