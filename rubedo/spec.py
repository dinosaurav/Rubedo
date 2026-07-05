"""
Pipeline and step specification definitions.
"""
import re
from typing import Callable, Optional, Dict, Any, Tuple, Type, List
from pydantic import BaseModel
from dataclasses import dataclass

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
    """The static definition of a pipeline step and its policies."""
    name: str
    fn: Callable
    version: str
    depends_on: List[str]
    params_model: Optional[Type[BaseModel]] = None
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
    index: Tuple[str, ...] = ()  # value fields extracted into the search index
    shape: str = "map"  # map | reduce
    executor: str = "thread"


@dataclass
class PipelineSpec:
    """The static definition of a complete DAG pipeline."""
    id: str
    name: str
    source: Source
    steps: List[StepSpec]


def _hash_source(fn: Callable) -> Optional[str]:
    """Extract and hash the source code of a function for code drift detection."""
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
    workers: int = 4,
    code: str = "warn",
    retries: int = 0,
    retry_on=Exception,
    retry_delay: float = 0.0,
    retry_backoff: float = 1.0,
    rate_limit: Optional[str] = None,
    stale_after: Optional[str] = None,
    skip_cache: bool = False,
    index: Optional[List[str]] = None,
    shape: str = "map",
    executor: str = "thread",
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

    index names fields of the output value (dotted paths for nesting) to
    extract into the search index at commit time, making outputs findable
    by their content: Selection(index={"company": "acme"}). List-valued
    fields index one entry per element. Purely operational — changing
    index= never affects cache identity, and only newly created
    materializations are indexed under the new declaration.
    """
    if code not in ("warn", "auto"):
        raise ValueError(f"Step '{name}': code must be 'warn' or 'auto', got {code!r}")
    if shape not in ("map", "reduce", "expand"):
        raise ValueError(
            f"Step '{name}': shape must be 'map', 'reduce', or 'expand', got {shape!r}"
        )
    if shape == "expand" and skip_cache:
        raise ValueError(
            f"Step '{name}': skip_cache is not supported with shape='expand'"
        )
    if shape == "expand" and len(depends_on or []) != 1:
        raise ValueError(
            f"Step '{name}': shape='expand' requires exactly one parent in "
            "depends_on (multi-parent expansion is a join — not yet supported)"
        )
    if executor not in ("thread", "process"):
        raise ValueError(f"Step '{name}': executor must be 'thread' or 'process', got {executor!r}")
    if shape == "reduce" and skip_cache:
        raise ValueError(f"Step '{name}': skip_cache is meaningless with shape='reduce' (reductions must be materialized)")
    if shape == "reduce" and not depends_on:
        raise ValueError(f"Step '{name}': shape='reduce' requires at least one parent in depends_on")
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
    if skip_cache and index:
        raise ValueError(
            f"Step '{name}': index is meaningless with skip_cache — "
            "nothing is stored to search"
        )
    if isinstance(retry_on, type) and issubclass(retry_on, BaseException):
        retry_on = (retry_on,)
    parsed_rate = parse_rate_limit(rate_limit) if rate_limit else None
    parsed_stale = parse_duration(stale_after) if stale_after else None

    def decorator(fn: Callable):
        code_hash = _hash_source(fn)
        if code == "auto" and code_hash is None:
            raise ValueError(
                f"Step '{name}': code='auto' requires an inspectable "
                "function source"
            )

        return StepSpec(
            name=name,
            fn=fn,
            version=version,
            depends_on=depends_on or [],
            params_model=params_model,
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
            index=tuple(index or ()),
            shape=shape,
            executor=executor,
        )

    return decorator


def pipeline(
    name: str,
    folder: Optional[str] = None,
    steps: Optional[List[StepSpec]] = None,
    id: Optional[str] = None,
    source: Optional[Source] = None,
):
    """Construct a pipeline specification from a source and a list of steps."""
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

    return PipelineSpec(
        id=id or name,
        name=name,
        source=source,
        steps=steps or [],
    )


def definition(spec: PipelineSpec) -> Dict[str, Any]:
    """JSON-safe snapshot of a pipeline's structure and policies.

    Recorded on every Run row so the ledger knows what DAG produced each
    run's outputs, and rendered by describe().
    """
    steps = []
    for s in spec.steps:
        entry: Dict[str, Any] = {
            "name": s.name,
            "version": s.version,
            "depends_on": list(s.depends_on),
            "workers": s.workers,
            "code": s.code_mode,
        }
        if s.skip_cache:
            entry["skip_cache"] = True
        if s.retries:
            entry["retries"] = s.retries
            entry["retry_on"] = [e.__name__ for e in s.retry_on]
        if s.rate_limit:
            count, period = s.rate_limit
            entry["rate_limit"] = f"{count}/{int(period)}s"
        if s.stale_after is not None:
            entry["stale_after_seconds"] = s.stale_after
        if s.params_model is not None:
            entry["params_schema"] = s.params_model.model_json_schema()
        if s.shape != "map":
            entry["shape"] = s.shape
        if s.executor != "thread":
            entry["executor"] = s.executor
        steps.append(entry)

    return {
        "id": spec.id,
        "name": spec.name,
        "source_id": spec.source.id,
        "steps": steps,
    }


def describe(spec: PipelineSpec, format: str = "text") -> str:
    """Render a pipeline's DAG before ever running it.

    format="text" prints steps in dependency order with their policies;
    format="mermaid" emits a Mermaid graph for markdown viewers.
    """
    from .planning import topological_sort

    topo = topological_sort(spec)

    if format == "mermaid":
        lines = ["graph TD"]
        for s in topo:
            label = f"{s.name}<br/>{s.version}" if s.version else s.name
            shape = f'{s.name}(["{label}"])' if s.skip_cache else f'{s.name}["{label}"]'
            lines.append(f"    {shape}")
        for s in topo:
            for dep in s.depends_on:
                lines.append(f"    {dep} --> {s.name}")
        return "\n".join(lines)

    if format != "text":
        raise ValueError(f"Unknown format {format!r}: expected 'text' or 'mermaid'")

    lines = [f"Pipeline '{spec.id}' over {spec.source.id}"]
    for s in topo:
        deps = f" <- {', '.join(s.depends_on)}" if s.depends_on else " (root)"
        policies = []
        if s.skip_cache:
            policies.append("skip_cache")
        if s.retries:
            policies.append(f"retries={s.retries}")
        if s.rate_limit:
            count, period = s.rate_limit
            policies.append(f"rate_limit={count}/{int(period)}s")
        if s.stale_after is not None:
            policies.append(f"stale_after={int(s.stale_after)}s")
        if s.code_mode == "auto":
            policies.append("code=auto")
        if s.params_model is not None:
            policies.append(f"params={s.params_model.__name__}")
        policy_str = f"  [{', '.join(policies)}]" if policies else ""
        lines.append(f"  {s.name} ({s.version}){deps}{policy_str}")
    return "\n".join(lines)
