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
from .models import Filtered, ProcessResult
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
from .store import read_materialization_output


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
    return read_materialization_output(ref)


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
                dep: _resolve_parent_value(ref.parent_refs[dep], params, memo)
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
        # Consumers of materialized steps receive the unwrapped value;
        # keep the contract identical (minus the serialization round-trip)
        if isinstance(result, ProcessResult):
            result = result.value
        return result

    return memo.compute((ref.step.name, ref.item.coordinate), produce)


def _materialized_ancestors(parent_refs: Dict[str, Any]) -> Dict[int, MatRef]:
    """Nearest materialized ancestors, skipping through ephemeral hops."""
    out: Dict[int, MatRef] = {}
    for ref in parent_refs.values():
        if isinstance(ref, EphemeralRef):
            out.update(_materialized_ancestors(ref.parent_refs))
        else:
            out[ref.id] = ref
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

    def call(decision: StepDecision, pool: Optional[Any] = None):
        # Dependent steps get parent outputs by parameter name; either kind
        # may declare `params`. A root step (map or expand) reads no
        # payload — it mints its own lane(s) from its params/generator.
        args: List[Any] = []
        if not step.depends_on:
            kwargs = {}
        elif step.shape == "reduce":
            kwargs = {
                dep: {
                    lane: _resolve_parent_value(ref, params, memo)
                    for lane, ref in decision.parent_mats[dep].items()
                }
                for dep in step.depends_on
            }
        else:
            kwargs = {
                dep: _resolve_parent_value(
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
                    # Consume the iterable inside the try so a failing
                    # generator body is caught and retried like any other.
                    return _expand_outcomes(
                        decision, list(result), attempt, attempt_errors
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
