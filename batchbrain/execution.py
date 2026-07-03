"""Execute phase: running step functions.

No database access — inputs come in as refs, results go out as
ExecutionOutcome values for the ledger to persist.
"""

import concurrent.futures
import threading
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional

from .models import Filtered, ProcessResult
from .planning import (
    EphemeralRef,
    MatRef,
    StepDecision,
    _build_step_params,
    _step_accepts_params,
)
from .spec import StepSpec
from .sources import Source
from .store import read_materialization_output


class _RateLimiter:
    """Paces calls evenly across a step's worker threads."""

    def __init__(self, count: int, period_seconds: float):
        self.min_interval = period_seconds / count
        self._lock = threading.Lock()
        self._next_free = 0.0

    def acquire(self):
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
        self._lock = threading.RLock()
        self._values: Dict[Any, Any] = {}

    def compute(self, key, producer):
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
    decision: StepDecision
    success: bool
    result: Any = None
    error_trace: Optional[str] = None
    attempts: int = 1
    attempt_errors: List[str] = field(default_factory=list)


def _resolve_parent_value(ref, sources: Dict[str, Source], params: Optional[dict], memo: _RunMemo):
    if isinstance(ref, EphemeralRef):
        return _compute_ephemeral(ref, sources, params, memo)
    return read_materialization_output(ref)


def _compute_ephemeral(
    ref: EphemeralRef, sources: Dict[str, Source], params: Optional[dict], memo: _RunMemo
):
    """Lazily compute a skip_cache step's value, at most once per run."""

    def produce():
        step = ref.step
        if not step.depends_on:
            step_source_key = step.source or "default"
            args = [sources[step_source_key].load(ref.item)]
            kwargs = {}
        else:
            args = []
            kwargs = {
                dep: _resolve_parent_value(ref.parent_refs[dep], sources, params, memo)
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
    sources: Dict[str, Source],
    params: Optional[dict],
    accepts_params: bool,
    workers: Optional[int],
    memo: _RunMemo,
) -> Iterator[ExecutionOutcome]:
    """Run the step function for each execute decision; yield as completed.

    Honors the step's rate limit (shared across workers, retries included)
    and retry policy (only exceptions matching retry_on are retried).
    """
    limiter = _RateLimiter(*step.rate_limit) if step.rate_limit else None

    def call(decision: StepDecision, pool: Optional[concurrent.futures.ProcessPoolExecutor] = None):
        # Root steps get the source payload positionally; dependent steps
        # get parent outputs by parameter name. Either kind may declare
        # `params`.
        if not step.depends_on:
            step_source_key = step.source or "default"
            args = [sources[step_source_key].load(decision.item)]
            kwargs = {}
        elif step.shape in ("reduce", "expand"):
            args = []
            kwargs = {
                dep: {
                    lane: _resolve_parent_value(
                        ref, sources, params, memo
                    )
                    for lane, ref in decision.parent_mats[dep].items()
                }
                for dep in step.depends_on
            }
        else:
            args = []
            kwargs = {
                dep: _resolve_parent_value(
                    decision.parent_mats[dep], sources, params, memo
                )
                for dep in step.depends_on
            }
        if accepts_params:
            kwargs["params"] = _build_step_params(step, params)
            
        if pool is not None:
            result = pool.submit(step.fn, *args, **kwargs).result()
        else:
            result = step.fn(*args, **kwargs)
        
        if step.shape == "expand":
            return list(result)
        return result

    def process(decision: StepDecision, pool: Optional[concurrent.futures.ProcessPoolExecutor] = None) -> ExecutionOutcome:
        attempt_errors: List[str] = []
        delay = step.retry_delay
        for attempt in range(1, step.retries + 2):
            if limiter:
                limiter.acquire()
            try:
                result = call(decision, pool)
                return ExecutionOutcome(
                    decision,
                    True,
                    result=result,
                    attempts=attempt,
                    attempt_errors=attempt_errors,
                )
            except Exception as e:
                trace = traceback.format_exc()
                retryable = attempt <= step.retries and isinstance(e, step.retry_on)
                if not retryable:
                    return ExecutionOutcome(
                        decision,
                        False,
                        error_trace=trace,
                        attempts=attempt,
                        attempt_errors=attempt_errors,
                    )
                attempt_errors.append(trace)
                if delay > 0:
                    time.sleep(delay)
                delay *= step.retry_backoff

    workers_count = workers or step.workers
    process_pool = None
    if getattr(step, "executor", "thread") == "process":
        process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=workers_count)

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers_count
        ) as executor:
            futures = [executor.submit(process, d, process_pool) for d in decisions]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
    finally:
        if process_pool is not None:
            process_pool.shutdown()
