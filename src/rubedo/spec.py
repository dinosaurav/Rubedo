"""
Pipeline and step specification definitions.
"""
import inspect
import re
from typing import Callable, Optional, Dict, Any, Tuple, Type, List, Literal, Union, get_type_hints
from pydantic import BaseModel
from dataclasses import dataclass

ExecutorSpec = Union[
    Literal["thread", "process"],
    Callable[[], Any],
]

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


SHAPES = ("map", "expand", "aggregate", "fold", "join", "join_table")
COLLECTIVE_SHAPES = frozenset({"aggregate", "fold", "join", "join_table"})
JOIN_SHAPES = frozenset({"join", "join_table"})


def _is_table_annotation(ann: Any) -> bool:
    """True if ``ann`` names a polars/pandas DataFrame or pyarrow Table."""
    if ann is inspect.Parameter.empty or ann is None:
        return False
    if isinstance(ann, str):
        tail = ann.rsplit(".", 1)[-1]
        return tail in ("DataFrame", "Table") or ann in (
            "pl.DataFrame", "pd.DataFrame", "pa.Table",
        )
    name = getattr(ann, "__name__", "") or ""
    mod = getattr(ann, "__module__", "") or ""
    if name == "DataFrame" and ("polars" in mod or "pandas" in mod):
        return True
    if name == "Table" and "pyarrow" in mod:
        return True
    return False


def _table_input_from_fn(fn: Optional[Callable], parent_params: List[str]) -> bool:
    """Infer aggregate-as-table from a parent parameter annotation."""
    if fn is None:
        return False
    try:
        hints = get_type_hints(fn)
    except Exception:
        hints = {
            n: p.annotation
            for n, p in inspect.signature(fn).parameters.items()
        }
    for name in parent_params:
        if _is_table_annotation(hints.get(name, inspect.Parameter.empty)):
            return True
    # Positional parent (first non-params, non-accum for fold is not used here)
    sig = inspect.signature(fn)
    for n, p in sig.parameters.items():
        if n == "params":
            continue
        if _is_table_annotation(p.annotation) or _is_table_annotation(hints.get(n, inspect.Parameter.empty)):
            return True
        break
    return False


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
    check_cache: bool = True  # when False, always re-execute (still commits, like --force for one step)
    shape: str = "map"  # map | expand | aggregate | fold | join | join_table
    as_table: bool = False  # output is one table-valued cache entry
    table_input: bool = False  # aggregate: pass parent lanes as pa.Table
    executor: ExecutorSpec = "thread"
    group_key: Optional[str] = None  # aggregate/fold: field to group lanes by
    join_on: Optional[Dict[str, str]] = None  # join / join_table: {parent: field}
    join_mode: Literal["intersect", "union"] = "intersect"
    row_key: Optional[str] = None  # expand-from-table: identity column
    fold_init: Any = None  # fold: initial accumulator (required when shape="fold")
    declarative: bool = False  # no fn — engine builds the output
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
    the source. An expand root (a parentless generator whose shape is
    inferred automatically — see `docs/concepts/sources.md`) yields the
    initial lanes; a map root mints a single `@root` lane from its params
    (or a constant). A pipeline may declare several roots — `join` doesn't
    care that its parents are roots.

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
    from .hashing import hash_text

    try:
        return hash_text(inspect.getsource(fn))
    except (OSError, TypeError):
        return None


def _get_source(fn: Callable) -> Optional[str]:
    """Extract the raw source text of a function, for the definition snapshot."""
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
    check_cache: bool = True,
    shape: Optional[str] = None,
    as_table: bool = False,
    executor: ExecutorSpec = "thread",
    group_key: Optional[str] = None,
    join_on: Optional[Dict[str, str]] = None,
    join_mode: Literal["intersect", "union"] = "intersect",
    row_key: Optional[str] = None,
    fold_init: Any = None,
    output_model: Optional[Type[BaseModel]] = None,
    assertions: Optional[List[Callable[[Any], None]]] = None,
    on_failed: Literal["use_passed", "block"] = "use_passed",
):
    """Declare a step. Works bare (`@step`) or called (`@step()`,
    `@step(version="2")`, ...) — both mint the same StepSpec.

    `name` defaults to the decorated function's `__name__`; pass it
    explicitly only when two steps would otherwise collide (two functions
    named the same across modules) or when the function name isn't the
    name you want in the ledger. Two steps that resolve to the same name
    — whether given explicitly or defaulted from the function — fail
    loudly at pipeline-construction time, naming both functions so you
    can tell where the collision came from.

    `shape` is the lane cardinality of the step:

    | `shape=`      | meaning |
    | ------------- | ------- |
    | `map`         | 1:1 zip with parent coordinates (default) |
    | `expand`      | mint N lanes (`yield`, `yield from`, or `return list`) |
    | `aggregate`   | N:1 fan-in (`group_key=` implies this) |
    | `fold`        | sequential N:1 (`fold_init=` implies this) |
    | `join`        | mint pair lanes (`join_on=` implies this) |
    | `join_table`  | equijoin that emits one table-valued coordinate |

    Inference (an explicit `shape=` that contradicts the code raises):

    - A generator defaults to `shape="expand"`.
    - `join_on=` defaults to `shape="join"` (pass `shape="join_table"` to
      emit one table instead of pair lanes).
    - `group_key=` defaults to `shape="aggregate"`.
    - `fold_init=` defaults to `shape="fold"`.
    - A plain `@all` aggregate still needs `shape="aggregate"`.

    `as_table=True` marks the output as one table-valued cache entry
    (must return a DataFrame / `pa.Table`). Returning a DataFrame without
    it raises — don't guess explode vs keep. `join_table` implies it.

    Aggregate *input* as a table is inferred from a parent-parameter
    annotation (`pl.DataFrame`, `pa.Table`, `pd.DataFrame`).

    `row_key=` on expand: identity column when minting lanes from a
    table parent (missing/duplicate keys raise). Omit it to hash each
    yielded payload as today.

    `depends_on` (when omitted entirely) is inferred at pipeline-build
    time (`_build_spec`, once every sibling step's name is known, not
    here): every parameter of the decorated function other than
    `params` must name a registered step and becomes a dependency, in
    signature order. An unmatched parameter raises `ValueError` naming
    the step, the parameter, and the available step names. A signature
    using `*args`/`**kwargs` skips inference entirely (pass
    `depends_on=` explicitly if such a step has parents). A step with
    no non-`params` parameters is a root. Passing `depends_on=`
    explicitly — as a list (unchanged) or as
    `{"param_name": "step_name"}` to bind a parent's output to a
    differently-named parameter — disables inference for that step.

    `version` defaults to `"0"`. It's the step's semantic identity —
    bump it for deliberate behavior changes (also the escape hatch for
    edits code hashing can't see, like helpers the step calls).
    `code="warn"` (the default either way) means an unbumped version
    never silently recomputes on a code edit — it warns instead (see
    below) — so leaving `version` at its default is exactly as safe as
    pinning it to `"1"` by hand.

    `code` decides what a *source edit* means, independently of version:

    - `"warn"` (default): edits never recompute; reusing an output whose
      code has since changed produces a loud warning. Right for
      expensive/non-deterministic steps.
    - `"auto"`: the function's source hash joins the cache identity, so
      any edit recomputes — no version bump needed. Right for cheap,
      deterministic steps.

    `retries` re-runs a failed execution up to `retries` extra times,
    but only for exceptions matching `retry_on` — narrow it to transient
    error types (timeouts, rate-limit responses); retrying a
    deterministic bug on an expensive step just multiplies its cost.
    `retry_delay` seconds separate attempts, multiplied by
    `retry_backoff` each time. Attempts are recorded as run events.

    `rate_limit` (`"10/min"`, `"2/s"`, `"500/hour"`) paces the step's
    executions across all of its workers, retries included.

    `stale_after` (`"24h"`, `"30min"`, `"7d"`) expires outputs: a cached
    output older than this re-executes on the next run. A recompute that
    produces different bytes supersedes the old generation; identical
    bytes refresh its clock. Natural for scraped or otherwise
    time-sensitive data.

    `skip_cache` marks an inline util: the step is never materialized or
    recorded — its identity (version/code/config) fuses into its
    consumers' cache keys, and it executes lazily (memoized per run)
    only when a consumer actually runs. Intended for quick, idempotent
    helpers that exist to keep other steps readable. Values pass in
    memory without a serialization round-trip, and execution policies
    (`retries`, `rate_limit`) are not applied — if a step needs those,
    it deserves materialization.

    `check_cache` (default `True`) controls whether a step's plan phase
    checks the cache for a reusable output. When `False`, the step
    always re-executes — but still commits its result to cache, so
    downstream steps can reuse and a subsequent run with
    `check_cache=True` sees the fresh output. This is the per-step
    equivalent of `force=True`: right for source roots that must
    re-scan the world every run (a filesystem crawl, an API poll) but
    whose outputs are stable when the upstream hasn't changed
    (content-addressed lanes collapse onto the same addresses, so
    downstream reuse is unaffected).

    `on_failed` controls the partial fan-in behavior for collective
    steps (aggregate/join). `"use_passed"` (default) allows the step to
    proceed with the surviving lanes if some parent lanes fail or are
    blocked. `"block"` halts the entire step if any parent lane is
    unavailable. Note that `"use_passed"` is literal: a multi-parent
    aggregate whose parents all failed for one dep still runs,
    receiving an empty dict for that kwarg — declare
    `on_failed="block"` if every parent must contribute.
    """

    def decorator(f: Callable) -> StepSpec:
        step_name = name if name is not None else f.__name__

        depends_on_explicit = depends_on is not None
        if isinstance(depends_on, dict):
            depends_on_list = list(depends_on.values())
            depends_on_aliases = {step: param for param, step in depends_on.items()}
        else:
            depends_on_list = list(depends_on) if depends_on is not None else []
            depends_on_aliases = None

        if depends_on is None and join_on is not None:
            depends_on_list = list(join_on.keys())

        is_generator = inspect.isgeneratorfunction(f)

        # Precedence: explicit shape= > join_on= / group_key= / fold_init= /
        # generator > default map.
        resolved = shape
        if resolved is not None and resolved not in SHAPES:
            raise ValueError(
                f"Step '{step_name}': shape must be one of {list(SHAPES)}, "
                f"got {resolved!r}"
            )

        if resolved is None:
            if join_on is not None:
                resolved = "join"
            elif fold_init is not None:
                resolved = "fold"
            elif group_key is not None:
                resolved = "aggregate"
            elif is_generator:
                resolved = "expand"
            else:
                resolved = "map"
        else:
            if join_on is not None and resolved not in JOIN_SHAPES:
                raise ValueError(
                    f"Step '{step_name}': join_on= requires shape='join' or "
                    f"'join_table' (got shape={resolved!r})"
                )
            if group_key is not None and resolved not in ("aggregate", "fold"):
                raise ValueError(
                    f"Step '{step_name}': group_key= requires shape='aggregate' or 'fold' "
                    f"(got shape={resolved!r})"
                )
            if is_generator and resolved != "expand":
                raise ValueError(
                    f"Step '{step_name}': a generator function must have shape='expand' "
                    f"(got shape={resolved!r}) — a generator under any other "
                    "shape never runs to completion as intended"
                )

        assert resolved is not None

        if is_generator and resolved != "expand":
            raise ValueError(
                f"Step '{step_name}': a generator function must have shape='expand' "
                f"(got shape={resolved!r})"
            )

        if code not in ("warn", "auto"):
            raise ValueError(f"Step '{step_name}': code must be 'warn' or 'auto', got {code!r}")

        if join_mode not in ("intersect", "union"):
            raise ValueError(
                f"Step '{step_name}': join_mode must be 'intersect' or 'union', "
                f"got {join_mode!r}"
            )
        if resolved in JOIN_SHAPES:
            if not join_on:
                raise ValueError(
                    f"Step '{step_name}': shape={resolved!r} requires join_on={{parent: field}}"
                )
            if len(depends_on_list) < 2:
                raise ValueError(
                    f"Step '{step_name}': shape={resolved!r} requires at least two parents in "
                    "depends_on (N-way star join on a shared value)"
                )
            if set(join_on) != set(depends_on_list):
                raise ValueError(
                    f"Step '{step_name}': join_on keys {sorted(join_on)} must match "
                    f"depends_on {sorted(depends_on_list)}"
                )
        elif join_mode != "intersect":
            raise ValueError(
                f"Step '{step_name}': join_mode requires shape='join' or 'join_table'"
            )
        if join_on is not None and resolved not in JOIN_SHAPES:
            raise ValueError(
                f"Step '{step_name}': join_on requires shape='join' or 'join_table'"
            )

        if resolved == "expand" and skip_cache:
            raise ValueError(
                f"Step '{step_name}': skip_cache is not supported with shape='expand'"
            )
        if resolved == "expand" and len(depends_on_list) > 1:
            raise ValueError(
                f"Step '{step_name}': shape='expand' takes at most one parent — "
                "none = a root (a source that yields the initial lanes); two+ would be a join"
            )
        if isinstance(executor, str):
            if executor not in ("thread", "process"):
                raise ValueError(
                    f"Step '{step_name}': executor must be 'thread', "
                    f"'process', or a zero-argument pool factory, got "
                    f"{executor!r}"
                )
        elif callable(executor):
            try:
                inspect.signature(executor).bind()
            except TypeError as exc:
                raise ValueError(
                    f"Step '{step_name}': executor factory must accept "
                    "zero arguments"
                ) from exc
            except (ValueError, AttributeError):
                pass
        else:
            raise ValueError(
                f"Step '{step_name}': executor must be 'thread', 'process', "
                f"or a zero-argument pool factory, got {executor!r}"
            )
        if resolved in ("aggregate", "fold") and skip_cache:
            raise ValueError(
                f"Step '{step_name}': skip_cache is meaningless with shape={resolved!r} "
                "(collective steps must be materialized)"
            )
        if group_key is not None and resolved not in ("aggregate", "fold"):
            raise ValueError(
                f"Step '{step_name}': group_key requires shape='aggregate' or 'fold' "
                f"(got shape={resolved!r}) — it partitions a collective's input lanes "
                "by an indexed field"
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
        if not check_cache and skip_cache:
            raise ValueError(
                f"Step '{step_name}': check_cache=False is contradictory with skip_cache "
                "— a skip_cache step is never materialized, so there is no cache to skip"
            )
        if not check_cache and stale_after is not None:
            raise ValueError(
                f"Step '{step_name}': stale_after is meaningless with check_cache=False "
                "— the step always re-executes anyway"
            )
        if on_failed not in ("use_passed", "block"):
            raise ValueError(
                f"Step '{step_name}': on_failed must be 'use_passed' or 'block', got {on_failed!r}"
            )
        if fold_init is not None and resolved != "fold":
            raise ValueError(
                f"Step '{step_name}': fold_init is only valid with shape='fold' "
                f"(got shape={resolved!r})"
            )
        if resolved == "fold":
            if fold_init is None:
                raise ValueError(
                    f"Step '{step_name}': shape='fold' requires fold_init"
                )
            if len(depends_on_list) > 1:
                raise ValueError(
                    f"Step '{step_name}': shape='fold' takes exactly one parent "
                    "(accumulator + one lane value)"
                )
            try:
                import json

                json.dumps(fold_init)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"Step '{step_name}': fold_init must be JSON-serializable"
                ) from e
            fold_params = [
                n for n in inspect.signature(f).parameters if n != "params"
            ]
            if _table_input_from_fn(f, fold_params):
                raise ValueError(
                    f"Step '{step_name}': table-typed annotations are only valid "
                    "on shape='aggregate' (fold receives one lane at a time)"
                )

        if as_table and resolved == "expand":
            raise ValueError(
                f"Step '{step_name}': as_table=True is not valid with shape='expand' "
                "— a table is one cache entry (use a map) or mint lanes with yield / "
                "return list / row_key="
            )
        if row_key is not None and resolved != "expand":
            raise ValueError(
                f"Step '{step_name}': row_key= is only valid with shape='expand' "
                f"(got shape={resolved!r})"
            )

        resolved_as_table = as_table or resolved == "join_table"
        table_input = False
        if resolved == "aggregate":
            parent_params: List[str] = []
            if depends_on_aliases:
                parent_params = [depends_on_aliases.get(d, d) for d in depends_on_list]
            else:
                parent_params = list(depends_on_list)
            table_input = _table_input_from_fn(f, parent_params)

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
            check_cache=check_cache,
            shape=resolved,
            as_table=resolved_as_table,
            table_input=table_input,
            executor=executor,
            group_key=group_key,
            join_on=join_on,
            join_mode=join_mode,
            row_key=row_key,
            fold_init=fold_init,
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
            entry["depends_on_aliases"] = dict(s.depends_on_aliases)
        if s.skip_cache:
            entry["skip_cache"] = True
        if not s.check_cache:
            entry["check_cache"] = False
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
        if s.as_table:
            entry["as_table"] = True
        if s.table_input:
            entry["table_input"] = True
        if s.row_key is not None:
            entry["row_key"] = s.row_key
        if s.group_key is not None:
            entry["group_key"] = s.group_key
        if s.shape == "fold":
            entry["fold_init"] = s.fold_init
        if s.join_on is not None:
            entry["join_on"] = dict(s.join_on)
            if s.join_mode != "intersect":
                entry["join_mode"] = s.join_mode
        if s.executor != "thread":
            if isinstance(s.executor, str):
                entry["executor"] = s.executor
            else:
                module = getattr(
                    s.executor,
                    "__module__",
                    type(s.executor).__module__,
                )
                qualname = getattr(
                    s.executor,
                    "__qualname__",
                    type(s.executor).__qualname__,
                )
                name = f"{module}.{qualname}" if module else qualname
                entry["executor"] = f"external:{name}"
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
        "secrets": list(spec.secrets),
        "env": list(spec.env),
    }
    if spec.retention is not None:
        snapshot["retention"] = spec.retention
    return snapshot
