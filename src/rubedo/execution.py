"""Execute phase: running step functions.

No database access — inputs come in as refs, results go out as
ExecutionOutcome values for the ledger to persist. The unit of execution
is one (step, lane) call — _process_decision — which the runner's segment
executor dispatches onto pools; per-step machinery that must be shared
across a run's calls (the rate limiter, the _RunMemo) is created by the
runner and passed in.
"""

import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


from .hashing import hash_json, hash_bytes
from .models import Filtered
from .planning import (
    EphemeralRef,
    MatRef,
    StepDecision,
    _build_step_params,
    _step_accepts_params,
    expand_anchor_address,
    expand_child_coord,
    expand_child_identity,
)
from .spec import StepSpec
from .store import _try_arrow, _to_arrow_table, read_output


def _resolve_parent_table(
    pipeline_id: str, parent_step: str, lane_refs: Dict[str, Any]
):
    """Resolve a reduce parent's output as a pa.Table — the struct column
    flattened into columns. Falls back to dict-of-lanes if the output
    column is not a struct (string fallback for spilled/mixed values)."""
    from . import lane_store

    lane_keys = list(lane_refs.keys())
    table = lane_store.output_column_as_table(pipeline_id, parent_step, lane_keys)
    if table is not None:
        return table
    # Fallback: resolve each lane to a Python dict and build a table
    import pyarrow as pa

    rows = []
    for lane, ref in lane_refs.items():
        val = read_output(getattr(ref, "output", None), getattr(ref, "content_type", None))
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

    Reentrant lock: chained skip_cache steps resolve recursively on the
    same worker thread. Exceptions are memoized too, so every consumer of
    a failed util sees the same failure.
    """

    def __init__(self):
        """Initialize the run memoizer with a per-key locking scheme."""
        self._lock = threading.Lock()
        self._values: Dict[Tuple[str, str], Tuple[str, Any]] = {}

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
    return read_output(getattr(ref, "output", None), getattr(ref, "content_type", None))


def _compute_ephemeral(ref: EphemeralRef, params: Optional[dict], memo: _RunMemo):
    """Lazily compute a skip_cache step's value, at most once per run."""

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
        result = step.fn(*args, **kwargs)
        if isinstance(result, Filtered):
            raise RuntimeError(
                f"skip_cache step '{step.name}' returned Filtered: filtering "
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
                dep: _resolve_parent_value(
                    decision.parent_mats[dep], params, memo
                )
                for dep in step.depends_on
            }
        # Declarative map (union) — passthrough the one parent that has
        # this lane (parent_mats only contains present parents)
        dep = list(decision.parent_mats.keys())[0]
        return _resolve_parent_value(decision.parent_mats[dep], params, memo)

    def call(decision: StepDecision, pool: Optional[Any] = None):
        if step.declarative:
            return _declarative_result(decision)

        # Dependent steps get parent outputs by parameter name; either kind
        # may declare `params`. A root step (map or expand) reads no
        # payload — it mints its own lane(s) from its params/generator.
        args: List[Any] = []
        if not step.depends_on:
            kwargs = {}
        elif step.shape == "reduce":
            if step.arrow_reduce:
                kwargs = {
                    _dep_kwarg(step, dep): _resolve_parent_table(
                        pipeline_id, dep, decision.parent_mats[dep]
                    )
                    for dep in step.depends_on
                }
            else:
                kwargs = {
                    _dep_kwarg(step, dep): {
                        lane: _resolve_parent_value(ref, params, memo)
                        for lane, ref in decision.parent_mats[dep].items()
                    }
                    for dep in step.depends_on
                }
        else:
            kwargs = {
                _dep_kwarg(step, dep): _resolve_parent_value(
                    decision.parent_mats[dep], params, memo
                )
                for dep in step.depends_on
            }
        if accepts_params:
            kwargs["params"] = _build_step_params(step, params)
            
        if pool is not None:
            return pool.submit(step.fn, *args, **kwargs).result()
        return step.fn(*args, **kwargs)

    def _expand_outcomes(
        decision: StepDecision, values: List[Any], attempt: int, attempt_errors: List[str]
    ) -> List[ExecutionOutcome]:
        """Fan one parent lane's yielded payloads into content-addressed lanes.

        A dependent expand emits the cache anchor first (the child content
        hashes, addressed by the parent so a re-run can skip the fn). A *root*
        expand (a source) has no parent to cache against, so it writes no
        anchor and always re-runs. Then one child per distinct payload — each a
        content-addressed lane `row-<hash>`; identical payloads collapse.
        """
        seen: set = set()
        children: List[tuple] = []  # (child_hash, value)
        for value in values:
            _validate_output(step, value)
            if isinstance(value, bytes):
                child_hash = "b:" + hash_bytes(value)
            else:
                child_hash = hash_json(value)
            if child_hash in seen:
                continue  # identical payload — one lane
            seen.add(child_hash)
            children.append((child_hash, value))

        outcomes: List[ExecutionOutcome] = []
        if step.depends_on:
            # Anchor: the child hashes, addressed by the parent. Not a lane.
            parent_hash = decision.parent_mats[step.depends_on[0]].output_content_hash
            anchor = StepDecision(
                coordinate=decision.coordinate,
                action="execute",
                input_hash=parent_hash,
                output_address=expand_anchor_address(
                    step, parent_hash, params_hash, accepts_params
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
                step, child_hash, params_hash, accepts_params
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

    def _expand_table_outcomes(
        decision: StepDecision, source_table: Any,
        attempt: int, attempt_errors: List[str]
    ) -> List[ExecutionOutcome]:
        """Fan a table-return expand into content-addressed lanes, keeping
        the data in Arrow throughout.  One ``to_pylist()`` for hashing only;
        the struct column is written directly to the lane store's arrow batch
        buffer — no Python dict → Arrow round trip at flush time.
        """
        import pyarrow as pa
        import pyarrow.compute as pc
        from datetime import datetime, timezone
        from . import lane_store

        # One bulk conversion for hashing only
        src_pa_table, _ = _to_arrow_table(source_table)
        rows = src_pa_table.to_pylist()
        seen: set = set()
        children: List[tuple] = []  # (row_idx, child_hash, lane_key, input_hash, address)
        for idx, row in enumerate(rows):
            _validate_output(step, row)
            child_hash = hash_json(row)
            if child_hash in seen:
                continue
            seen.add(child_hash)
            lane_key = expand_child_coord(child_hash)
            input_hash, child_address = expand_child_identity(
                step, child_hash, params_hash, accepts_params
            )
            children.append((idx, child_hash, lane_key, input_hash, child_address))

        # Build the lane store Arrow table directly from the source table's
        # struct column + computed metadata — no Python dict buffer.
        if children:
            ts = datetime.now(timezone.utc)
            # Extract the struct array for the deduped rows
            # src_pa_table is already a pa.Table (converted above)
            # Build the struct column by selecting deduped rows
            row_indices = [c[0] for c in children]
            struct_type = pa.struct([
                pa.field(name, src_pa_table.column(name).type)
                for name in src_pa_table.column_names
            ])
            cols = []
            for name in src_pa_table.column_names:
                col = src_pa_table.column(name)
                if isinstance(col, pa.ChunkedArray):
                    combined = pa.concat_arrays(col.chunks)
                else:
                    combined = col
                cols.append(combined)
            struct_arr = pa.StructArray.from_arrays(
                [pc.take(c, row_indices) for c in cols],
                fields=[pa.field(n, c.type) for n, c in zip(src_pa_table.column_names, cols)],
            )

            lane_keys = [c[2] for c in children]
            addresses = [c[4] for c in children]
            input_hashes = [c[3] for c in children]
            output_identities = [c[1] for c in children]  # child_hash == _identity_of(row)
            row_ids = [
                lane_store._make_row_id(pipeline_id, step.name, lk, ts)
                for lk in lane_keys
            ]

            batch_table = pa.table({
                "row_id": pa.array(row_ids, type=pa.string()),
                "lane_key": pa.array(lane_keys, type=pa.string()),
                "address": pa.array(addresses, type=pa.string()),
                "input_hash": pa.array(input_hashes, type=pa.string()),
                "code_version": pa.array([step.version] * len(children), type=pa.string()),
                "output": struct_arr,
                "output_identity": pa.array(output_identities, type=pa.string()),
                "content_type": pa.array(["json"] * len(children), type=pa.string()),
                "code_hash": pa.array([step.code_hash] * len(children), type=pa.string()),
                "ts": pa.array([ts] * len(children), type=pa.timestamp("us", tz="UTC")),
                "run_id": pa.array([""] * len(children), type=pa.string()),  # filled by ledger
                "filtered": pa.array([False] * len(children), type=pa.bool_()),
                "index_values": pa.array([None] * len(children), type=pa.map_(pa.string(), pa.list_(pa.string()))),
            }, schema=lane_store._schema(pa, struct_type))

            lane_store.append_arrow_batch(pipeline_id, step.name, batch_table)

        # Build outcomes — no dict values, arrow_batched=True
        outcomes: List[ExecutionOutcome] = []
        if step.depends_on:
            parent_hash = decision.parent_mats[step.depends_on[0]].output_content_hash
            anchor = StepDecision(
                coordinate=decision.coordinate,
                action="execute",
                input_hash=parent_hash,
                output_address=expand_anchor_address(
                    step, parent_hash, params_hash, accepts_params
                ),
                parent_mats=decision.parent_mats,
            )
            outcomes.append(
                ExecutionOutcome(
                    anchor, True, result=[c[1] for c in children],
                    attempts=attempt, attempt_errors=attempt_errors, is_anchor=True,
                )
            )

        for _, child_hash, lane_key, input_hash, child_address in children:
            child = StepDecision(
                coordinate=lane_key,
                action="execute",
                input_hash=input_hash,
                output_address=child_address,
                parent_mats=decision.parent_mats,
            )
            outcomes.append(
                ExecutionOutcome(
                    child, True, result=None, attempts=attempt,
                    attempt_errors=attempt_errors, arrow_batched=True,
                )
            )
        return outcomes

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
                    if _try_arrow(result):
                        # Table-return expand: keep data in Arrow, one
                        # to_pylist for hashing only, struct column
                        # written directly to the arrow batch buffer.
                        return _expand_table_outcomes(
                            decision, result, attempt, attempt_errors
                        )
                    else:
                        values = list(result)
                        return _expand_outcomes(
                            decision, values, attempt, attempt_errors
                        )
                _validate_output(step, result)
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
