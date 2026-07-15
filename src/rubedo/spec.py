"""
Pipeline and step specification definitions.
"""
import inspect
import re
from typing import Callable, Optional, Dict, Any, Tuple, Type, List, Literal, Union
from pydantic import BaseModel
from dataclasses import dataclass

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
    # False when `step()` was called with no explicit `depends_on=` — the
    # signal `_build_spec` (pipeline.py) uses to infer `depends_on` from
    # `fn`'s parameter names once every sibling step's name is known (it
    # can't happen here at decoration time). Any explicit `depends_on=`
    # (list or dict alias form) sets this True and disables inference.
    depends_on_explicit: bool = True
    # Set only by the dict alias form (`depends_on={"param": "step"}`):
    # step name -> the parameter name its output binds to, for steps whose
    # signature spells a parent under a different name than the step itself.
    depends_on_aliases: Optional[Dict[str, str]] = None
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
    join_on: Optional[Dict[str, str]] = None  # join: {parent: indexed field}
    output_model: Optional[Type[BaseModel]] = None
    assertions: Optional[List[Callable[[Any], None]]] = None
    on_failed: Literal["use_passed", "block"] = "use_passed"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Pure passthrough to `fn` — lets a decorated step be called
        directly in a unit test (`extract(scan={"text": "hi"})`) without
        touching the engine. The engine itself always calls `step.fn`."""
        return self.fn(*args, **kwargs)


@dataclass
class PipelineSpec:
    """The static definition of a complete DAG pipeline.

    Ingestion has no separate concept: a root step (no `depends_on`) *is*
    the source. A `shape="expand"` root yields the initial lanes, re-running
    every run (a parentless generator function infers this shape
    automatically — see `docs/concepts/sources.md`); a `shape="map"` root
    mints a single lane from its params (or a constant). A pipeline may
    declare several roots — `join` doesn't care that its parents are roots.

    `name` is the pipeline's sole identity (there is no separate `id`): the
    ledger's `pipeline_id` column stores it verbatim, and `Selection`'s
    `pipeline:` term matches against it. Built and validated by
    `Pipeline`/`pipeline()` in `pipeline.py` — this class stays plain data.
    """
    name: str
    steps: List[StepSpec]
    params_model: Optional[Type[BaseModel]] = None
    # Retention policy: keep only this pipeline's last N *terminal* runs'
    # outputs; older, no-longer-referenced generations are pruned. None = keep
    # everything. Rides the definition() snapshot each run records, so the ops
    # path (rubedo gc) reads it without importing user code.
    retention: Optional[int] = None
    # secrets=/env= (TODO 21): declarations only, executable documentation of
    # what this pipeline needs from its environment — secrets are vault-
    # injected/log-masked in cloud, env is deploy-config-injected/visible.
    # Locally both still come from the shell/.env exactly as before; these
    # names have zero effect on execution or cache identity (validated and
    # stored here, never hashed into any step's address — see
    # `planning.py`'s address computation, which only ever reads StepSpec).
    # `rubedo check` reads them statically off a file's `pipeline(...)` call
    # without importing it.
    secrets: Tuple[str, ...] = ()
    env: Tuple[str, ...] = ()


def _hash_source(fn: Callable) -> Optional[str]:
    """Extract and hash the source code of a function for code drift detection."""
    import inspect

    from .hashing import hash_text

    try:
        return hash_text(inspect.getsource(fn))
    except (OSError, TypeError):
        return None


def _get_source(fn: Callable) -> Optional[str]:
    """Extract the raw source text of a function, for the definition snapshot."""
    import inspect

    try:
        return inspect.getsource(fn).strip()
    except (OSError, TypeError):
        return None


def step(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    version: str = "0",
    depends_on: Optional[Union[List[str], Dict[str, str]]] = None,
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
    shape: Optional[str] = None,
    executor: str = "thread",
    group_key: Optional[str] = None,
    join_on: Optional[Dict[str, str]] = None,
    output_model: Optional[Type[BaseModel]] = None,
    assertions: Optional[List[Callable[[Any], None]]] = None,
    on_failed: Literal["use_passed", "block"] = "use_passed",
):
    """Declare a step. Works bare (`@step`) or called (`@step()`,
    `@step(version="2")`, ...) — both mint the same StepSpec.

    name defaults to the decorated function's `__name__`; pass it
    explicitly only when two steps would otherwise
    collide (two functions named the same across modules) or when the
    function name isn't the name you want in the ledger. Two steps that
    resolve to the same name — whether given explicitly or defaulted from
    the function — fail loudly at pipeline-construction time, naming both
    functions so you can tell where the collision came from.

    shape, depends_on, join_on, and group_key restate what the code already
    implies, so each has an inferred default (any explicit value always
    wins, and an explicit value that contradicts what the code implies
    raises):
      - A generator function defaults to `shape="expand"` — it's a fan-out
        by construction. An explicit non-"expand" shape on a generator
        raises (a generator under map/reduce/join is already broken; better
        to fail at decoration than mid-run).
      - `join_on=` defaults `shape` to "join"; `group_key=` defaults it to
        "reduce". A plain `@all` reduce (no `group_key`) still needs an
        explicit `shape="reduce"` — nothing else implies it.
      - `depends_on` (when omitted entirely) is inferred at pipeline-build
        time (`_build_spec`, once every sibling step's name is known, not
        here): every parameter of the decorated function other than
        `params` must name a registered step and becomes a dependency, in
        signature order. An unmatched parameter raises `ValueError` naming
        the step, the parameter, and the available step names. A signature
        using `*args`/`**kwargs` skips inference entirely (pass `depends_on=`
        explicitly if such a step has parents). A step with no non-`params`
        parameters is a root. Passing `depends_on=` explicitly — as a list
        (unchanged) or as `{"param_name": "step_name"}` to bind a parent's
        output to a differently-named parameter — disables inference for
        that step.

    version defaults to "0". It's the step's semantic identity — bump it
    for deliberate behavior changes (also the escape hatch for edits code
    hashing can't see, like helpers the step calls). `code="warn"` (the
    default either way) means an unbumped version never silently
    recomputes on a code edit — it warns instead (see below) — so leaving
    version at its default is exactly as safe as pinning it to "1" by hand.

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

    on_failed controls the partial fan-in behavior for collective steps
    (reduce/join). "use_passed" (default) allows the step to proceed with
    the surviving lanes if some parent lanes fail or are blocked. "block"
    halts the entire step if any parent lane is unavailable. Note that
    "use_passed" is literal: a multi-parent reduce whose parents all failed
    for one dep still runs, receiving an empty dict for that kwarg — declare
    on_failed="block" if every parent must contribute.
    """

    def decorator(f: Callable) -> StepSpec:
        step_name = name if name is not None else f.__name__

        # depends_on: list form is unchanged; dict form ({"param": "step"})
        # is an alias — the step name (for depends_on/planning, everywhere
        # else in the engine) plus a reverse param-name mapping execution
        # uses to bind the parent's value to the right kwarg. Either form,
        # or an empty list, is "explicit" and disables signature inference
        # (which happens later, in `pipeline.py::_build_spec`, once sibling
        # step names are known).
        depends_on_explicit = depends_on is not None
        if isinstance(depends_on, dict):
            depends_on_list = list(depends_on.values())
            depends_on_aliases = {step: param for param, step in depends_on.items()}
        else:
            depends_on_list = list(depends_on) if depends_on is not None else []
            depends_on_aliases = None

        # join_on keys name the parents (they ARE the depends_on set — the
        # set-equality check below is a tautology when both come from
        # join_on), so a join that omits depends_on= can be validated at
        # decoration time by borrowing join_on's keys. Build-time signature
        # inference (pipeline.py::_build_spec) will then confirm the
        # function's parameters actually name those parents.
        if depends_on is None and join_on is not None:
            depends_on_list = list(join_on.keys())

        # shape: explicit always wins. Otherwise a generator function is a
        # fan-out by construction (shape="expand"); join_on=/group_key=
        # otherwise imply "join"/"reduce"; anything else is a plain "map".
        is_generator = inspect.isgeneratorfunction(f)
        resolved_shape = shape
        if resolved_shape is None:
            if join_on is not None:
                resolved_shape = "join"
            elif group_key is not None:
                resolved_shape = "reduce"
            elif is_generator:
                resolved_shape = "expand"
            else:
                resolved_shape = "map"
        if is_generator and resolved_shape != "expand":
            raise ValueError(
                f"Step '{step_name}': a generator function must use "
                f"shape='expand' (got shape={resolved_shape!r}) — a generator "
                "under any other shape never runs to completion as intended"
            )

        if code not in ("warn", "auto"):
            raise ValueError(f"Step '{step_name}': code must be 'warn' or 'auto', got {code!r}")
        if resolved_shape not in ("map", "reduce", "expand", "join"):
            raise ValueError(
                f"Step '{step_name}': shape must be 'map', 'reduce', 'expand', or 'join', "
                f"got {resolved_shape!r}"
            )
        if resolved_shape == "join":
            if not join_on:
                raise ValueError(
                    f"Step '{step_name}': shape='join' requires join_on={{parent: field}}"
                )
            if len(depends_on_list) < 2:
                raise ValueError(
                    f"Step '{step_name}': shape='join' requires at least two parents in "
                    "depends_on (N-way star join on a shared value)"
                )
            if set(join_on) != set(depends_on_list):
                raise ValueError(
                    f"Step '{step_name}': join_on keys {sorted(join_on)} must match "
                    f"depends_on {sorted(depends_on_list)}"
                )
        if join_on is not None and resolved_shape != "join":
            raise ValueError(f"Step '{step_name}': join_on requires shape='join'")
        if resolved_shape == "expand" and skip_cache:
            raise ValueError(
                f"Step '{step_name}': skip_cache is not supported with shape='expand'"
            )
        if resolved_shape == "expand" and len(depends_on_list) > 1:
            raise ValueError(
                f"Step '{step_name}': shape='expand' takes at most one parent — none = a "
                "root (a source that yields the initial lanes); two+ would be a join"
            )
        if executor not in ("thread", "process"):
            raise ValueError(f"Step '{step_name}': executor must be 'thread' or 'process', got {executor!r}")
        if resolved_shape == "reduce" and skip_cache:
            raise ValueError(f"Step '{step_name}': skip_cache is meaningless with shape='reduce' (reductions must be materialized)")
        if group_key is not None and resolved_shape != "reduce":
            raise ValueError(
                f"Step '{step_name}': group_key requires shape='reduce' (it partitions a "
                "reduction's input lanes by an indexed field)"
            )
        if version == "auto":
            raise ValueError(
                f"Step '{step_name}': version is a semantic label; use code='auto' "
                "to derive cache identity from the source instead"
            )
        if retries < 0:
            raise ValueError(f"Step '{step_name}': retries must be >= 0")
        if skip_cache and stale_after is not None:
            raise ValueError(
                f"Step '{step_name}': stale_after is meaningless with skip_cache — "
                "nothing is stored to expire"
            )
        if skip_cache and index:
            raise ValueError(
                f"Step '{step_name}': index is meaningless with skip_cache — "
                "nothing is stored to search"
            )
        if on_failed not in ("use_passed", "block"):
            raise ValueError(
                f"Step '{step_name}': on_failed must be 'use_passed' or 'block', got {on_failed!r}"
            )
        resolved_retry_on = (retry_on,) if isinstance(retry_on, type) and issubclass(retry_on, BaseException) else retry_on
        parsed_rate = parse_rate_limit(rate_limit) if rate_limit else None
        parsed_stale = parse_duration(stale_after) if stale_after else None

        if assertions is not None:
            if not isinstance(assertions, (list, tuple)) or not all(callable(a) for a in assertions):
                raise ValueError(
                    f"Step '{step_name}': assertions must be a list of callables"
                )

        code_hash = _hash_source(f)
        if code == "auto" and code_hash is None:
            raise ValueError(
                f"Step '{step_name}': code='auto' requires an inspectable "
                "function source"
            )

        return StepSpec(
            name=step_name,
            fn=f,
            version=version,
            depends_on=depends_on_list,
            depends_on_explicit=depends_on_explicit,
            depends_on_aliases=depends_on_aliases,
            params_model=params_model,
            workers=workers,
            code_hash=code_hash,
            code_mode=code,
            retries=retries,
            retry_on=tuple(resolved_retry_on),
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            rate_limit=parsed_rate,
            stale_after=parsed_stale,
            skip_cache=skip_cache,
            index=tuple(index or ()),
            shape=resolved_shape,
            executor=executor,
            group_key=group_key,
            join_on=join_on,
            output_model=output_model,
            assertions=list(assertions) if assertions else None,
            on_failed=on_failed,
        )

    return decorator(fn) if fn is not None else decorator


def definition(spec: PipelineSpec) -> Dict[str, Any]:
    """JSON-safe snapshot of a pipeline's structure and policies.

    Recorded on every Run row so the ledger knows what DAG produced each
    run's outputs, and rendered by describe(). The "id" key mirrors "name"
    for schema stability with existing definition() consumers.
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
        source = _get_source(s.fn) if s.fn is not None else None
        if source:
            entry["source"] = source
        if s.depends_on_aliases:
            # Only the dict alias form produces this — additive, so the
            # common (list-form or inferred) case's snapshot is unchanged.
            entry["depends_on_aliases"] = dict(s.depends_on_aliases)
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
            if s.on_failed != "use_passed":
                entry["on_failed"] = s.on_failed
        if s.group_key is not None:
            entry["group_key"] = s.group_key
        if s.join_on is not None:
            entry["join_on"] = dict(s.join_on)
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

    snapshot: Dict[str, Any] = {
        "id": spec.name,
        "name": spec.name,
        "steps": steps,
        # Emitted unconditionally (even empty): these are declarations, not
        # policy toggles, and dashboards/tooling read definition_json as the
        # authoritative list of a pipeline's environment surface.
        "secrets": list(spec.secrets),
        "env": list(spec.env),
    }
    if spec.retention is not None:
        # The ops path (rubedo gc / auto-prune) reads each pipeline's policy from
        # its latest run's definition_json — never by importing user code.
        snapshot["retention"] = spec.retention
    return snapshot
