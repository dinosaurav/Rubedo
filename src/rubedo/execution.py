"""Execute phase: running step functions.

No database access — inputs come in as refs, results go out as
ExecutionOutcome values for the ledger to persist.
"""

import concurrent.futures
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple

import loky


from .hashing import hash_json
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
from .sources import Source
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
        """Initialize the run memoizer with a reentrant lock."""
        self._lock = threading.RLock()
        self._values: Dict[Tuple[str, str], Tuple[Literal["ok", "err"], Any]] = {}

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
            if key not in self._values:
                try:
                    self._values[key] = ("ok", producer())
                except Exception as e:
                    self._values[key] = ("err", e)
        kind, value = self._values[key]
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
    # An expand step's cache anchor (the full yielded list, addressed by the
    # parent): stored so a re-run can skip the fn, but it is not a lane — no
    # status, count, edge, or coord_step_mats entry.
    is_anchor: bool = False


def _resolve_parent_value(
    ref, step_sources: Dict[str, Source], params: Optional[dict], memo: _RunMemo
):
    """
    Resolve the output value of a parent step, computing it lazily if ephemeral.

    Args:
        ref: The reference to the parent output (MatRef or EphemeralRef).
        step_sources: step name -> the Source it reads (root/skip_cache roots).
        params (Optional[dict]): Run parameters.
        memo (_RunMemo): The run memoizer for ephemeral steps.

    Returns:
        Any: The resolved output value.
    """
    if isinstance(ref, EphemeralRef):
        return _compute_ephemeral(ref, step_sources, params, memo)
    return read_materialization_output(ref)


def _compute_ephemeral(
    ref: EphemeralRef, step_sources: Dict[str, Source], params: Optional[dict],
    memo: _RunMemo,
):
    """Lazily compute a skip_cache step's value, at most once per run."""

    def produce():
        step = ref.step
        if not step.depends_on:
            args = [step_sources[step.name].load(ref.item)]
            kwargs = {}
        else:
            args = []
            kwargs = {
                dep: _resolve_parent_value(
                    ref.parent_refs[dep], step_sources, params, memo
                )
                for dep in step.depends_on
            }
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


def _execute_step(
    step: StepSpec,
    decisions: List[StepDecision],
    step_sources: Dict[str, Source],
    params: Optional[dict],
    accepts_params: bool,
    workers: Optional[int],
    memo: _RunMemo,
) -> Iterator[ExecutionOutcome]:
    """Run the step function for each execute decision; yield as completed.

    Honors the step's rate limit (shared across workers, retries included)
    and retry policy (only exceptions matching retry_on are retried).
    `step_sources` maps a step name to the Source it reads (root/skip_cache
    roots), so each root loads from its own source in a multi-source pipeline.
    """
    limiter = _RateLimiter(*step.rate_limit) if step.rate_limit else None
    params_hash = hash_json(params or {})

    def call(decision: StepDecision, pool: Optional[Any] = None):
        # Root steps get the source payload positionally; dependent steps
        # get parent outputs by parameter name. Either kind may declare
        # `params`.
        if not step.depends_on:
            args = [step_sources[step.name].load(decision.item)]
            kwargs = {}
        elif step.shape == "reduce":
            args = []
            kwargs = {
                dep: {
                    lane: _resolve_parent_value(
                        ref, step_sources, params, memo
                    )
                    for lane, ref in decision.parent_mats[dep].items()
                }
                for dep in step.depends_on
            }
        else:
            args = []
            kwargs = {
                dep: _resolve_parent_value(
                    decision.parent_mats[dep], step_sources, params, memo
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

        Emits the cache anchor first (the list of child content hashes,
        addressed by the parent so a re-run can skip the fn), then one child
        per distinct payload — each minting a content-addressed lane
        `row-<hash>`. Identical payloads collapse to one child.
        """
        dep = step.depends_on[0]
        parent_hash = decision.parent_mats[dep].output_content_hash

        seen: set = set()
        children: List[tuple] = []  # (child_hash, value)
        for value in values:
            child_hash = hash_json(value)
            if child_hash in seen:
                continue  # identical payload — one lane
            seen.add(child_hash)
            children.append((child_hash, value))

        # Anchor: the child hashes, addressed by the parent. Not a lane.
        anchor = StepDecision(
            coordinate=decision.coordinate,
            action="execute",
            input_hash=parent_hash,
            output_address=expand_anchor_address(
                step, parent_hash, params_hash, accepts_params
            ),
            parent_mats=decision.parent_mats,
        )
        outcomes: List[ExecutionOutcome] = [
            ExecutionOutcome(
                anchor,
                True,
                result=[h for h, _ in children],
                attempts=attempt,
                attempt_errors=attempt_errors,
                is_anchor=True,
            )
        ]

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

    def process(
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

    # Never spin up more workers (or subprocesses) than there is work.
    pool_size = min(workers or step.workers, len(decisions))
    if pool_size < 1:
        return

    # Two layers, on purpose. The thread pool is the orchestrator: it drives
    # the retry loop and the parent-shared rate limiter for every lane. A
    # process pool, when requested, is only where the CPU-bound step body
    # runs — retries and rate limiting must stay in the parent, so process
    # steps still need the thread layer to feed them.
    process_pool = (
        loky.ProcessPoolExecutor(max_workers=pool_size)
        if step.executor == "process"
        else None
    )
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=pool_size) as executor:
            futures = [executor.submit(process, d, process_pool) for d in decisions]
            for future in concurrent.futures.as_completed(futures):
                yield from future.result()
    finally:
        if process_pool is not None:
            process_pool.shutdown()
