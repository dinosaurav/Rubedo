"""Execute phase: running step functions.

No database access — inputs come in as refs, results go out as
ExecutionOutcome values for the ledger to persist. The unit of execution
is one (step, lane) call — _process_decision — which the runner's segment
executor dispatches onto pools; per-step machinery that must be shared
across a run's calls (the rate limiter, the _RunMemo) is created by the
runner and passed in.
"""

import inspect
import threading
import time
import traceback
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING


from .hashing import hash_json, hash_bytes
from .models import Filtered
from .payload_refs import (
    PayloadRefsState,
    SpilledResult,
    StoreRef,
    _ref_call,
    parent_as_ref_or_value,
)
from .planning import (
    EphemeralRef,
    MatRef,
    StepDecision,
    _build_step_params,
    _step_accepts_params,
    expand_anchor_address,
    expand_child_coord,
    expand_child_identity,
    ROOT_LANE,
)
from .spec import StepSpec, _is_dict_annotation, _param_annotation
from .store import _from_arrow_table, _try_arrow, _to_arrow_table

if TYPE_CHECKING:
    from .home import Home


def _resolve_parent_table(
    memo: "_RunMemo", pipeline_id: str, parent_step: str, lane_refs: Dict[str, Any]
):
    """Resolve an aggregate parent's output as a pa.Table — the struct column
    flattened into columns. Falls back to dict-of-lanes if the output
    column is not a struct (string fallback for spilled/mixed values)."""
    lane_keys = list(lane_refs.keys())
    table = memo.home.lanes.output_column_as_table(pipeline_id, parent_step, lane_keys)
    if table is not None:
        return table
    # Fallback: resolve each lane to a Python dict and build a table
    import pyarrow as pa

    rows = []
    for lane, ref in lane_refs.items():
        val = memo.home.store.read_output(
            getattr(ref, "output", None), getattr(ref, "content_type", None)
        )
        if val is not None:
            rows.append(val)
    if not rows:
        return pa.table({})
    return pa.Table.from_pylist(rows)


class _RateLimiter:
    """Paces calls evenly across a step's worker threads."""

    def __init__(self, count: int, period_seconds: float):
        """
        Initialize the rate limiter.

        Args:
            count (int): Number of allowed calls per period.
            period_seconds (float): Time period in seconds.
        """
        self.min_interval = period_seconds / count
        self._lock = threading.Lock()
        self._next_free = 0.0

    def acquire(self):
        """Wait until it is safe to proceed according to the rate limit."""
        with self._lock:
            now = time.monotonic()
            wait = self._next_free - now
            self._next_free = max(now, self._next_free) + self.min_interval
        if wait > 0:
            time.sleep(wait)


class _RunMemo:
    """Per-run memo so an ephemeral step runs at most once per coordinate.

    Reentrant lock: chained use_cache=False steps resolve recursively on the
    same worker thread. Exceptions are memoized too, so every consumer of
    a failed util sees the same failure.
    """

    def __init__(self, home: "Home", refs_state: Optional[PayloadRefsState] = None):
        """Initialize the run memoizer with a per-key locking scheme."""
        self.home = home
        self.refs_state = refs_state or PayloadRefsState(enabled=False)
        self._lock = threading.Lock()
        self._values: Dict[Tuple[str, str], Tuple[str, Any]] = {}
        self._refs_warn: Optional[Callable[[str], None]] = None

    def set_refs_warning_sink(self, sink: Callable[[str], None]) -> None:
        self._refs_warn = sink

    def _emit_refs_warning(self, message: str) -> None:
        if self._refs_warn is not None:
            self._refs_warn(message)
    def compute(self, key: Tuple[str, str], producer: Callable[[], Any]) -> Any:
        """
        Compute or retrieve a memoized value for the given key.

        Args:
            key: (step_name, coordinate) — see _compute_ephemeral's call site.
            producer: A zero-argument function that produces the value.

        Returns:
            Any: The produced or cached value.
        """
        with self._lock:
            state = self._values.get(key)
            if state is None:
                event = threading.Event()
                self._values[key] = ("computing", event)
            elif state[0] == "done":
                kind, value = state[1]
                if kind == "err":
                    raise value
                return value
            else:
                event = state[1]

        if state is not None:
            event.wait()
            kind, value = self._values[key][1]
            if kind == "err":
                raise value
            return value

        try:
            val = producer()
            res = ("ok", val)
        except Exception as e:
            res = ("err", e)

        with self._lock:
            self._values[key] = ("done", res)
            event.set()

        kind, value = res
        if kind == "err":
            raise value
        return value


@dataclass
class ExecutionOutcome:
    """Represents the final result of attempting to execute a step for a coordinate."""
    decision: StepDecision
    success: bool
    result: Any = None
    error_trace: Optional[str] = None
    attempts: int = 1
    attempt_errors: List[str] = field(default_factory=list)
    # An expand step's cache anchor (the child content hashes, addressed by
    # the parent): stored so a re-run can skip the fn, but it is not a lane —
    # no status, count, edge, or coord_step_mats entry.
    is_anchor: bool = False
    # Arrow data already written to the lane store's arrow batch buffer —
    # the ledger should skip serialize_output + append_filled (the Arrow
    # table is already in the buffer).  SQLite writes (IHU, edges, RCS)
    # still happen per-outcome.
    arrow_batched: bool = False


def _dep_kwarg(step: StepSpec, dep: str) -> str:
    """The parameter name a parent's value binds to when calling `step.fn`
    — the step (dependency) name itself, unless `depends_on={"param":
    "step"}` (the dict alias form) renamed it."""
    if step.depends_on_aliases:
        return step.depends_on_aliases.get(dep, dep)
    return dep


def _resolve_parent_value(ref, params: Optional[dict], memo: _RunMemo):
    """
    Resolve the output value of a parent step, computing it lazily if ephemeral.

    Args:
        ref: The reference to the parent output (MatRef or EphemeralRef).
        params (Optional[dict]): Run parameters.
        memo (_RunMemo): The run memoizer for ephemeral steps.

    Returns:
        Any: The resolved output value.
    """
    if isinstance(ref, EphemeralRef):
        return _compute_ephemeral(ref, params, memo)
    return memo.home.store.read_output(
        getattr(ref, "output", None), getattr(ref, "content_type", None)
    )


def _fused_row_kernel_parent(step: StepSpec, kwargs: Dict[str, Any]) -> Optional[str]:
    """Parent kwarg to apply as a row kernel, or None.

    A fused map never mints. When it has exactly one parent whose value
    is a table and whose parameter is annotated ``dict``, call the fn
    once per inner row and stack — not one call with the DataFrame, and
    not N coordinates.
    """
    if step.use_cache is not False or step.shape != "map" or step.declarative:
        return None
    parent_kwargs = [k for k in kwargs if k != "params"]
    if len(parent_kwargs) != 1:
        return None
    name = parent_kwargs[0]
    if not _try_arrow(kwargs[name]):
        return None
    if _is_dict_annotation(_param_annotation(step.fn, name)):
        return name
    return None


def _apply_fused_row_kernel(
    step: StepSpec, parent_kw: str, parent_value: Any, extra_kwargs: Dict[str, Any]
) -> Any:
    """Apply a fused map to each inner row of a table parent; never mint."""
    import pyarrow as pa

    tbl, kind = _to_arrow_table(parent_value)
    rows = tbl.to_pylist()
    if not rows:
        return parent_value

    out: List[Any] = []
    saw_dict = False
    saw_scalar = False
    for row in rows:
        result = step.fn(**{parent_kw: row, **extra_kwargs})
        if isinstance(result, Filtered):
            raise RuntimeError(
                f"use_cache=False step '{step.name}' returned Filtered: filtering "
                "is a cacheable decision, so filter steps must be materialized"
            )
        if _try_arrow(result):
            raise ValueError(
                f"use_cache=False step {step.name!r} returned a DataFrame/Table "
                "from a per-row call; return a scalar (adds column "
                f"{step.name!r}) or a dict (stacks a table), or annotate the "
                "parent as a DataFrame to receive the frame in one call"
            )
        if isinstance(result, dict):
            saw_dict = True
            out.append(result)
        else:
            saw_scalar = True
            out.append(result)
    if saw_dict and saw_scalar:
        raise ValueError(
            f"use_cache=False step {step.name!r} mixed dict and scalar "
            "returns across rows; return one or the other"
        )
    if saw_dict:
        stacked = pa.Table.from_pylist(out)
    else:
        if step.name in tbl.column_names:
            tbl = tbl.drop_columns(step.name)
        stacked = tbl.append_column(step.name, pa.array(out))
    return _from_arrow_table(stacked, kind)


def _compute_ephemeral(ref: EphemeralRef, params: Optional[dict], memo: _RunMemo):
    """Lazily compute a use_cache=False step's value, at most once per run."""

    def produce():
        step = ref.step
        # A root step (map or expand) reads no payload — it mints its own
        # lane(s) and receives only params. A dependent step gets parent
        # outputs by parameter name.
        args: List[Any] = []
        kwargs = (
            {}
            if not step.depends_on
            else {
                _dep_kwarg(step, dep): _resolve_parent_value(ref.parent_refs[dep], params, memo)
                for dep in step.depends_on
            }
        )
        if _step_accepts_params(step):
            kwargs["params"] = _build_step_params(step, params)
        parent_kw = _fused_row_kernel_parent(step, kwargs)
        if parent_kw is not None:
            extra = {k: v for k, v in kwargs.items() if k != parent_kw}
            return _apply_fused_row_kernel(step, parent_kw, kwargs[parent_kw], extra)
        result = step.fn(*args, **kwargs)
        if isinstance(result, Filtered):
            raise RuntimeError(
                f"use_cache=False step '{step.name}' returned Filtered: filtering "
                "is a cacheable decision, so filter steps must be materialized"
            )
        return result

    return memo.compute((ref.step.name, ref.item.coordinate), produce)


def _materialized_ancestors(parent_refs: Dict[str, Any]) -> Dict[str, MatRef]:
    """Nearest materialized ancestors, skipping through ephemeral hops.
    Keyed by output_address (the identity for edge writes)."""
    out: Dict[str, MatRef] = {}
    for ref in parent_refs.values():
        if isinstance(ref, EphemeralRef):
            out.update(_materialized_ancestors(ref.parent_refs))
        else:
            out[ref.output_address] = ref
    return out


def _validate_output(step: StepSpec, value: Any) -> None:
    """Run data quality assertions on a step's output."""
    if step.output_model is not None:
        step.output_model.model_validate(value)
    if step.assertions:
        for assertion in step.assertions:
            assertion(value)


def _validate_table_grain(step: StepSpec, value: Any) -> None:
    """Require as_table=True to return a DataFrame; require a table when set."""
    is_table = _try_arrow(value)
    if step.as_table:
        if not is_table:
            raise ValueError(
                f"step {step.name!r} has as_table=True but returned "
                f"{type(value).__name__}; must return a DataFrame/Table"
            )
    elif is_table:
        raise ValueError(
            f"step {step.name!r} returned a DataFrame/Table without "
            "as_table=True; set as_table=True to keep one table-valued "
            "cache entry, or yield/return a list to mint row lanes"
        )


def _prepare_join_table_parent(step: StepSpec, dep: str, value: Any) -> Any:
    """Require a table-valued parent and enforce null/duplicate join-key rules."""
    if not _try_arrow(value):
        raise ValueError(
            f"join_table step {step.name!r} parent {dep!r} must return a "
            "DataFrame/Table (as_table=True)"
        )
    tbl, _ = _to_arrow_table(value)
    field = (step.join_on or {}).get(dep)
    if not field:
        return value
    if field not in tbl.column_names:
        raise ValueError(
            f"join_table step {step.name!r}: side {dep!r} has no column "
            f"{field!r} to join on"
        )
    import pyarrow as pa
    import pyarrow.compute as pc

    col = tbl.column(field)
    nulls = pc.sum(pc.cast(pc.is_null(col), pa.int64())).as_py()
    if nulls:
        raise ValueError(
            f"join_table step {step.name!r}: side {dep!r} has a null value "
            f"for join field {field!r}. The field must exist and be non-None "
            "in the parent's table (null join keys are rejected so they "
            "cannot cartesian-match each other)."
        )
    nuniq = pc.count_distinct(col).as_py()
    if nuniq < tbl.num_rows:
        warnings.warn(
            f"join_table step {step.name!r}: join key {field!r} on {dep!r} "
            "has duplicate rows; the join emits the cartesian product of "
            "matching keys",
            UserWarning,
            stacklevel=2,
        )
    return value


def _join_table_parent_value(
    step: StepSpec, dep: str, lanes: Dict[str, Any], params: Optional[dict], memo: "_RunMemo"
) -> Any:
    """Hydrate a join_table parent: exactly one table-valued lane."""
    if not isinstance(lanes, dict) or len(lanes) != 1:
        n = 0 if not isinstance(lanes, dict) else len(lanes)
        raise ValueError(
            f"join_table step {step.name!r} parent {dep!r} must be a single "
            f"table-valued lane, got {n}"
        )
    ref = next(iter(lanes.values()))
    val = _resolve_parent_value(ref, params, memo)
    return _prepare_join_table_parent(step, dep, val)


def _engine_join_tables(step: StepSpec, tables_by_parent: Dict[str, Any]) -> Any:
    """Declarative equijoin of table-valued parents (intersect=inner, union=outer)."""
    deps = list((step.join_on or {}).keys())
    converted = []
    kinds = []
    for dep in deps:
        t, kind = _to_arrow_table(tables_by_parent[dep])
        converted.append(t)
        kinds.append(kind)
    join_type = "inner" if step.join_mode == "intersect" else "full outer"
    acc = converted[0]
    left_key = step.join_on[deps[0]]  # type: ignore[index]
    for dep, right in zip(deps[1:], converted[1:]):
        right_key = step.join_on[dep]  # type: ignore[index]
        acc = acc.join(
            right,
            keys=left_key,
            right_keys=right_key,
            join_type=join_type,
            coalesce_keys=True,
        )
    return _from_arrow_table(acc, kinds[0])


def _process_decision(
    step: StepSpec,
    decision: StepDecision,
    params: Optional[dict],
    accepts_params: bool,
    params_hash: str,
    memo: _RunMemo,
    limiter: Optional[_RateLimiter],
    process_pool: Optional[Any] = None,
    pipeline_id: str = "",
    run_id: str = "",
) -> List[ExecutionOutcome]:
    """Run the step function for one execute decision — the (step, lane) unit.

    Honors the step's rate limit (`limiter` is one instance per step per
    run, shared across every call the runner dispatches for that step,
    retries included) and retry policy (only exceptions matching retry_on
    are retried). `process_pool`, when the step declares executor="process",
    is where the step body runs — retries and rate limiting stay in the
    calling (thread) layer.

    Returns a list because an expand fans one parent lane into an anchor
    plus N children; every other shape returns exactly one outcome.
    """

    def _declarative_result(decision: StepDecision) -> Any:
        """Build the output for a declarative step (no fn) from the
        parents' output values directly.

        - Declarative join: nest each parent's output under its step name
          -> {"orders": {...}, "customers": {...}}
        - Declarative union (map shape): pass through the single present
          parent's output unchanged
        """
        if step.shape == "join":
            return {
                dep: (
                    _resolve_parent_value(decision.parent_mats[dep], params, memo)
                    if dep in decision.parent_mats
                    else None
                )
                for dep in step.depends_on
            }
        if step.shape == "join_table":
            tables = {}
            for dep in step.depends_on:
                lanes = decision.parent_mats.get(dep) or {}
                tables[dep] = _join_table_parent_value(step, dep, lanes, params, memo)
            return _engine_join_tables(step, tables)
        # Declarative map (union) — passthrough the one parent that has
        # this lane (parent_mats only contains present parents)
        dep = list(decision.parent_mats.keys())[0]
        return _resolve_parent_value(decision.parent_mats[dep], params, memo)

    def call(decision: StepDecision, pool: Optional[Any] = None):
        if step.declarative:
            return _declarative_result(decision)

        refs_state = memo.refs_state
        use_refs = (
            pool is not None
            and refs_state.pool_allows_refs(step, pool)
            and refs_state.ensure_probe(pool, emit_warning=memo._emit_refs_warning)
        )

        def bind_parent(ref):
            if use_refs:
                return parent_as_ref_or_value(ref, params, memo)
            return _resolve_parent_value(ref, params, memo)

        # Dependent steps get parent outputs by parameter name; either kind
        # may declare `params`. A root step (map or expand) reads no
        # payload — it mints its own lane(s) from its params/generator.
        args: List[Any] = []
        if not step.depends_on:
            kwargs: Dict[str, Any] = {}
        elif step.shape == "aggregate":
            if step.table_input:
                # Arrow aggregate path stays coordinator-side (table build).
                kwargs = {
                    _dep_kwarg(step, dep): _resolve_parent_table(
                        memo, pipeline_id, dep, decision.parent_mats[dep]
                    )
                    for dep in step.depends_on
                }
                use_refs = False
            else:
                kwargs = {
                    _dep_kwarg(step, dep): {
                        lane: bind_parent(ref)
                        for lane, ref in decision.parent_mats[dep].items()
                    }
                    for dep in step.depends_on
                }
        elif step.shape == "fold":
            # A fold is the aggregate cache/plan shape with a different
            # execution strategy: deterministic, one-lane-at-a-time calls.
            # The current fold API is unary (accumulator + one parent value).
            # Copy the declared JSON value: mutable initial accumulators must
            # reset independently for each group and each execution.
            import copy

            dep = step.depends_on[0]
            accumulator = copy.deepcopy(step.fold_init)
            for lane, ref in sorted(decision.parent_mats[dep].items()):
                value = bind_parent(ref)
                if use_refs and isinstance(value, StoreRef):
                    assert pool is not None
                    refs_state.shim_submissions += 1
                    submit_kw = {}
                    if accepts_params:
                        submit_kw["params"] = _build_step_params(step, params)
                    accumulator = pool.submit(
                        _ref_call,
                        refs_state.store_config,
                        refs_state.client_factory,
                        step.fn,
                        tuple(step.assertions or ()),
                        step.output_model,
                        run_id,
                        decision.coordinate,
                        accumulator,
                        value,
                        **submit_kw,
                    ).result()
                    if isinstance(accumulator, SpilledResult):
                        # Fold accumulator must stay a live value between
                        # lanes — spilled mid-fold is not supported; fall
                        # back would require re-fetch. Treat as error.
                        raise RuntimeError(
                            f"fold step {step.name!r} spilled an intermediate "
                            "accumulator under payload refs; use inline "
                            "accumulators or payload_refs=False"
                        )
                elif accepts_params:
                    if pool is not None:
                        accumulator = pool.submit(
                            step.fn, accumulator, value,
                            params=_build_step_params(step, params),
                        ).result()
                    else:
                        accumulator = step.fn(
                            accumulator, value,
                            params=_build_step_params(step, params),
                        )
                elif pool is not None:
                    accumulator = pool.submit(step.fn, accumulator, value).result()
                else:
                    accumulator = step.fn(accumulator, value)
            return accumulator
        elif step.shape == "join_table":
            use_refs = False
            kwargs = {
                _dep_kwarg(step, dep): _join_table_parent_value(
                    step, dep, decision.parent_mats.get(dep) or {}, params, memo
                )
                for dep in step.depends_on
            }
        else:
            kwargs = {
                _dep_kwarg(step, dep): (
                    bind_parent(decision.parent_mats[dep])
                    if dep in decision.parent_mats
                    else None
                )
                for dep in step.depends_on
            }
        if accepts_params:
            kwargs["params"] = _build_step_params(step, params)

        # Expand stays by-value (TODO 13): never shim generator/table fan-out.
        if step.shape == "expand":
            use_refs = False

        ships_ref = any(
            isinstance(v, StoreRef)
            or (isinstance(v, dict) and any(isinstance(x, StoreRef) for x in v.values()))
            for v in kwargs.values()
            if v is not kwargs.get("params")
        )
        if pool is not None and use_refs and ships_ref:
            refs_state.shim_submissions += 1
            return pool.submit(
                _ref_call,
                refs_state.store_config,
                refs_state.client_factory,
                step.fn,
                tuple(step.assertions or ()),
                step.output_model,
                run_id,
                decision.coordinate,
                *args,
                **kwargs,
            ).result()
        if pool is not None:
            return pool.submit(step.fn, *args, **kwargs).result()
        return step.fn(*args, **kwargs)

    def _expand_outcomes(
        decision: StepDecision, values: List[Any], attempt: int, attempt_errors: List[str]
    ) -> List[ExecutionOutcome]:
        """Fan one parent lane's yielded payloads into content-addressed lanes.

        Emits the cache anchor first (the child content hashes, addressed by
        the parent — or ROOT_LANE for a root expand — so a re-run can skip
        the fn). Then one child per distinct payload — each a content-
        addressed lane `row-<hash>`. Without ``row_key``, identical payloads
        collapse; with ``row_key``, identity is that field (missing/dup raise).
        """
        seen: set = set()
        children: List[tuple] = []  # (child_hash, value)
        for value in values:
            if _try_arrow(value):
                raise ValueError(
                    f"expand step {step.name!r} cannot yield a DataFrame/Table; "
                    "use as_table=True on a map, or yield dicts / return a list"
                )
            _validate_output(step, value)
            if step.row_key is not None:
                if not isinstance(value, dict):
                    raise ValueError(
                        f"expand step {step.name!r}: row_key={step.row_key!r} "
                        f"requires dict payloads, got {type(value).__name__}"
                    )
                if step.row_key not in value or value[step.row_key] is None:
                    raise ValueError(
                        f"expand step {step.name!r}: missing row_key "
                        f"{step.row_key!r}"
                    )
                child_hash = hash_json(value[step.row_key])
                if child_hash in seen:
                    raise ValueError(
                        f"expand step {step.name!r}: duplicate row_key "
                        f"{step.row_key!r}={value[step.row_key]!r}"
                    )
            elif isinstance(value, bytes):
                child_hash = "b:" + hash_bytes(value)
            else:
                child_hash = hash_json(value)
            if child_hash in seen:
                continue  # identical payload — one lane
            seen.add(child_hash)
            children.append((child_hash, value))

        outcomes: List[ExecutionOutcome] = []
        # Anchor: the child hashes, addressed by the parent (or ROOT_LANE
        # for a root expand). Not a lane — just the cache entry that lets
        # a re-run skip the generator.
        if step.depends_on:
            parent_hash = decision.parent_mats[step.depends_on[0]].output_content_hash
        else:
            parent_hash = ROOT_LANE
        anchor = StepDecision(
            coordinate=decision.coordinate,
            action="execute",
            input_hash=parent_hash,
            output_address=expand_anchor_address(
                step, parent_hash, params_hash, accepts_params, pipeline_id
            ),
            parent_mats=decision.parent_mats,
        )
        outcomes.append(
            ExecutionOutcome(
                anchor, True, result=[h for h, _ in children],
                attempts=attempt, attempt_errors=attempt_errors, is_anchor=True,
            )
        )

        for child_hash, value in children:
            input_hash, child_address = expand_child_identity(
                step, child_hash, params_hash, accepts_params, pipeline_id
            )
            child = StepDecision(
                coordinate=expand_child_coord(child_hash),
                action="execute",
                input_hash=input_hash,
                output_address=child_address,
                parent_mats=decision.parent_mats,
            )
            outcomes.append(
                ExecutionOutcome(
                    child, True, result=value, attempts=attempt,
                    attempt_errors=attempt_errors,
                )
            )
        return outcomes

    def _expand_result(
        decision: StepDecision, result: Any, attempt: int, attempt_errors: List[str]
    ) -> List[ExecutionOutcome]:
        if _try_arrow(result):
            if not step.row_key:
                raise ValueError(
                    f"expand step {step.name!r} cannot return a DataFrame/Table; "
                    "use as_table=True on a map to keep one table-valued cache "
                    "entry, or yield / return a list to mint row lanes, or set "
                    "row_key= to mint lanes from the table"
                )
            src, _ = _to_arrow_table(result)
            values = src.to_pylist()
            return _expand_outcomes(decision, values, attempt, attempt_errors)
        if inspect.isgenerator(result):
            values = list(result)
        elif isinstance(result, (list, tuple)):
            values = list(result)
        else:
            raise ValueError(
                f"expand step {step.name!r} must yield or return a list/tuple "
                f"(got {type(result).__name__})"
            )
        return _expand_outcomes(decision, values, attempt, attempt_errors)

    def process(  # type: ignore
        decision: StepDecision, pool: Optional[Any] = None
    ) -> List[ExecutionOutcome]:
        attempt_errors: List[str] = []
        delay = step.retry_delay
        for attempt in range(1, step.retries + 2):
            if limiter:
                limiter.acquire()
            try:
                result = call(decision, pool)
                if step.shape == "expand":
                    return _expand_result(
                        decision, result, attempt, attempt_errors
                    )
                # SpilledResult was validated + spilled worker-side.
                if not isinstance(result, SpilledResult):
                    _validate_output(step, result)
                    _validate_table_grain(step, result)
                return [
                    ExecutionOutcome(
                        decision,
                        True,
                        result=result,
                        attempts=attempt,
                        attempt_errors=attempt_errors,
                    )
                ]
            except Exception as e:
                trace = traceback.format_exc()
                retryable = attempt <= step.retries and isinstance(e, step.retry_on)
                if not retryable:
                    return [
                        ExecutionOutcome(
                            decision,
                            False,
                            error_trace=trace,
                            attempts=attempt,
                            attempt_errors=attempt_errors,
                        )
                    ]
                attempt_errors.append(trace)
                if delay > 0:
                    time.sleep(delay)
                delay *= step.retry_backoff

    return process(decision, process_pool)
