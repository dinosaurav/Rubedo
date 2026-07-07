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
    shape: str = "map"  # map | reduce | expand
    executor: str = "thread"
    group_key: Optional[str] = None  # reduce: indexed field to group lanes by
    source: Optional[str] = None  # root step: which named source it reads
    join_on: Optional[Dict[str, str]] = None  # join: {parent: indexed field}
    output_model: Optional[Type[BaseModel]] = None
    assertions: Optional[List[Callable[[Any], None]]] = None


DEFAULT_SOURCE = "__source__"


@dataclass
class PipelineSpec:
    """The static definition of a complete DAG pipeline.

    `sources` maps a name to a Source. Single-source pipelines use one entry
    under DEFAULT_SOURCE; multi-source pipelines (for joins) name each one and
    root steps pick with `@step(source="name")`.
    """
    id: str
    name: str
    sources: Dict[str, Source]
    steps: List[StepSpec]

    @property
    def source(self) -> Source:
        """The sole source — convenience for single-source pipelines."""
        if len(self.sources) == 1:
            return next(iter(self.sources.values()))
        raise ValueError(
            "pipeline has multiple sources; use .sources or a step's source="
        )

    def source_for(self, step: "StepSpec") -> Optional[Source]:
        """The Source a step reads, or None if it reads none.

        Dependent steps and root *expand* steps (which are themselves sources)
        read nothing; a root non-expand step reads a named/sole source.
        """
        if step.depends_on or step.shape == "expand":
            return None
        if step.source is not None:
            return self.sources[step.source]
        return next(iter(self.sources.values()))  # single-source default


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
    group_key: Optional[str] = None,
    source: Optional[str] = None,
    join_on: Optional[Dict[str, str]] = None,
    output_model: Optional[Type[BaseModel]] = None,
    assertions: Optional[List[Callable[[Any], None]]] = None,
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
    if shape not in ("map", "reduce", "expand", "join"):
        raise ValueError(
            f"Step '{name}': shape must be 'map', 'reduce', 'expand', or 'join', "
            f"got {shape!r}"
        )
    if shape == "join":
        if not join_on:
            raise ValueError(
                f"Step '{name}': shape='join' requires join_on={{parent: field}}"
            )
        if len(depends_on or []) < 2:
            raise ValueError(
                f"Step '{name}': shape='join' requires at least two parents in "
                "depends_on (N-way star join on a shared value)"
            )
        if set(join_on) != set(depends_on or []):
            raise ValueError(
                f"Step '{name}': join_on keys {sorted(join_on)} must match "
                f"depends_on {sorted(depends_on or [])}"
            )
    if join_on is not None and shape != "join":
        raise ValueError(f"Step '{name}': join_on requires shape='join'")
    if shape == "expand" and skip_cache:
        raise ValueError(
            f"Step '{name}': skip_cache is not supported with shape='expand'"
        )
    if shape == "expand" and len(depends_on or []) > 1:
        raise ValueError(
            f"Step '{name}': shape='expand' takes at most one parent — none = a "
            "root (a source that yields the initial lanes); two+ would be a join"
        )
    if executor not in ("thread", "process"):
        raise ValueError(f"Step '{name}': executor must be 'thread' or 'process', got {executor!r}")
    if shape == "reduce" and skip_cache:
        raise ValueError(f"Step '{name}': skip_cache is meaningless with shape='reduce' (reductions must be materialized)")
    if shape == "reduce" and not depends_on:
        raise ValueError(f"Step '{name}': shape='reduce' requires at least one parent in depends_on")
    if group_key is not None and shape != "reduce":
        raise ValueError(
            f"Step '{name}': group_key requires shape='reduce' (it partitions a "
            "reduction's input lanes by an indexed field)"
        )
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

    if assertions is not None:
        if not isinstance(assertions, (list, tuple)) or not all(callable(a) for a in assertions):
            raise ValueError(
                f"Step '{name}': assertions must be a list of callables"
            )

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
            group_key=group_key,
            source=source,
            join_on=join_on,
            output_model=output_model,
            assertions=list(assertions) if assertions else None,
        )

    return decorator


def source(fn=None, *, name=None, version="1", **step_kwargs):
    """A root source: sugar for a parentless `expand` step.

    Decorate a generator that yields payloads — each becomes a content-addressed
    lane:

        @source
        def hn_top():
            for sid in fetch_top_ids():
                yield fetch_story(sid)

    It is exactly `@step(shape="expand")` with no `depends_on`, so drop it in
    `pipeline(steps=[...])` with no `source=`. `name` defaults to the function
    name; other `@step` policies (`index=`, `retries=`, `rate_limit=`, …)
    forward through.
    """
    def wrap(f):
        return step(
            name=name or f.__name__, version=version, shape="expand", **step_kwargs
        )(f)

    return wrap(fn) if fn is not None else wrap


def pipeline(
    name: str,
    folder: Optional[str] = None,
    steps: Optional[List[StepSpec]] = None,
    id: Optional[str] = None,
    source: Optional[Source] = None,
    sources: Optional[Dict[str, Source]] = None,
):
    """Construct a pipeline from its steps (and optional source sugar).

    Pass at most one of `folder=` (FolderSource sugar), `source=` (one Source),
    or `sources={name: Source}` (multiple). Or pass none: then a root
    `shape="expand"` step *is* the source (it yields the initial lanes).
    """
    given = [x for x in (folder, source, sources) if x is not None]
    if len(given) > 1:
        raise ValueError("Pass at most one of folder=, source=, or sources=")

    if sources is None:
        if folder is not None:
            from .sources import FolderSource

            sources = {DEFAULT_SOURCE: FolderSource(folder)}
        elif source is not None:
            sources = {DEFAULT_SOURCE: source}
        else:
            sources = {}  # no source: a root expand step yields the lanes

    steps = steps or []
    single = len(sources) == 1
    roots = [s for s in steps if not s.depends_on]
    if not sources and not any(s.shape == "expand" for s in roots):
        raise ValueError(
            "pipeline has no source — a root step must be shape='expand' "
            "(a source that yields the initial lanes)"
        )
    for s in steps:
        if s.source is not None and s.source not in sources:
            raise ValueError(
                f"Step '{s.name}' reads source '{s.source}', which is not in "
                f"sources={sorted(sources)}"
            )
        # a root non-expand step reads a source; a root expand reads none
        if not s.depends_on and s.shape != "expand":
            if not sources:
                raise ValueError(
                    f"Root step '{s.name}' needs a source, or must be shape='expand'"
                )
            if s.source is None and not single:
                raise ValueError(
                    f"Root step '{s.name}' must declare source= (pipeline has "
                    f"multiple sources {sorted(sources)})"
                )

    consumed = {dep for s in steps for dep in s.depends_on}
    name_to_step = {s.name: s for s in steps}
    for s in steps:
        if s.skip_cache and s.name not in consumed:
            raise ValueError(
                f"Step '{s.name}' has skip_cache but no consumer: its output "
                "would never be computed or stored"
            )
        if s.shape == "join":
            for dep in s.join_on or {}:
                parent = name_to_step.get(dep)
                if parent and parent.skip_cache:
                    raise ValueError(
                        f"Step '{s.name}': shape='join' cannot have a skip_cache parent ('{dep}')"
                    )
        if s.shape == "reduce" and s.group_key is not None:
            for dep in s.depends_on:
                parent = name_to_step.get(dep)
                if parent and parent.skip_cache:
                    raise ValueError(
                        f"Step '{s.name}': group_key requires materialized parents, but '{dep}' is skip_cache"
                    )

    return PipelineSpec(id=id or name, name=name, sources=sources, steps=steps)


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
        if s.group_key is not None:
            entry["group_key"] = s.group_key
        if s.join_on is not None:
            entry["join_on"] = dict(s.join_on)
        if s.source is not None:
            entry["source"] = s.source
        if s.executor != "thread":
            entry["executor"] = s.executor
        if s.output_model is not None:
            entry["output_schema"] = s.output_model.model_json_schema()
        if s.assertions:
            entry["assertions"] = [
                a.__name__ if hasattr(a, "__name__") and a.__name__ != "<lambda>" else "assertion" 
                for a in s.assertions
            ]
        steps.append(entry)

    return {
        "id": spec.id,
        "name": spec.name,
        "source_id": ",".join(sorted(s.id for s in spec.sources.values())),
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

    src_desc = ", ".join(
        (f"{name}={s.id}" if name != DEFAULT_SOURCE else s.id)
        for name, s in sorted(spec.sources.items())
    )
    lines = [f"Pipeline '{spec.id}' over {src_desc}"]
    for s in topo:
        root_tag = f" (root:{s.source})" if s.source else " (root)"
        deps = f" <- {', '.join(s.depends_on)}" if s.depends_on else root_tag
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


class PipelineBuilder:
    """A helper for constructing pipelines using the builder pattern.
    
    Instead of passing a list of steps to `pipeline(steps=[...])`, you can 
    use `@p.step()` to accumulate them on the builder instance.
    """
    def __init__(self, **pipeline_kwargs):
        self.pipeline_kwargs = pipeline_kwargs
        self.steps: List[StepSpec] = []
        
    def step(self, *args, **kwargs):
        """Decorate a function to define it as a step in this pipeline."""
        def decorator(fn):
            s = step(*args, **kwargs)(fn)
            self.steps.append(s)
            return s
        return decorator

    def source(self, fn=None, **kwargs):
        """Decorate a function to define it as a source step in this pipeline."""
        def wrap(f):
            s = source(**kwargs)(f)
            self.steps.append(s)
            return s
        return wrap(fn) if fn is not None else wrap
        
    def build(self, **kwargs) -> PipelineSpec:
        """Build the final PipelineSpec using the accumulated steps."""
        merged = {**self.pipeline_kwargs, **kwargs}
        merged["steps"] = self.steps + merged.get("steps", [])
        return pipeline(**merged)
