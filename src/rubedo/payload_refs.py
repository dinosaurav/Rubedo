"""Pass-by-reference payloads for spilled values (TODO 13).

When the object store is remote (``S3Store``) and a step runs on a process
or factory executor, spilled parent values are shipped as ``objects:<hash>``
refs. Workers rebuild a store from picklable config, GET inputs, run the
step (plus assertions), and PUT spill-worthy results themselves so the
coordinator never hubs those bytes.
"""
from __future__ import annotations

import threading
import warnings
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING

from .models import Filtered
from .store import S3Store, S3StoreConfig, _deserialize_bytes

if TYPE_CHECKING:
    from .home import Home
    from .spec import StepSpec


@dataclass(frozen=True)
class StoreRef:
    """Opaque spill pointer passed to a worker instead of bytes."""

    output: str
    content_type: Optional[str]


@dataclass(frozen=True)
class SpilledResult:
    """Worker already wrote the object; commit skips serialize_output."""

    output: str
    content_type: str
    size_bytes: int


@dataclass
class PayloadRefsState:
    """Per-run refs policy + probe cache (one instance on ``_RunMemo``)."""

    enabled: bool
    store_config: Optional[S3StoreConfig] = None
    client_factory: Optional[Callable[[], Any]] = None
    shim_submissions: int = 0
    _probe_lock: threading.Lock = field(default_factory=threading.Lock)
    _probe_ok: Dict[int, bool] = field(default_factory=dict)
    _probe_warned: set = field(default_factory=set)

    @classmethod
    def from_home(cls, home: "Home", *, payload_refs: bool = True) -> "PayloadRefsState":
        store = home.store
        config = getattr(store, "store_config", None)
        if not payload_refs or not isinstance(config, S3StoreConfig):
            return cls(enabled=False)
        return cls(
            enabled=True,
            store_config=config,
            client_factory=getattr(store, "client_factory", None),
        )

    def pool_allows_refs(self, step: "StepSpec", pool: Optional[Any]) -> bool:
        if not self.enabled or pool is None:
            return False
        return step.executor == "process" or callable(step.executor)

    def ensure_probe(self, pool: Any, *, emit_warning: Callable[[str], None]) -> bool:
        """Return True if this pool may use refs (probe passed or cached)."""
        key = id(pool)
        with self._probe_lock:
            cached = self._probe_ok.get(key)
            if cached is not None:
                return cached
        ok = True
        try:
            fut = pool.submit(_probe_store_access, self.store_config, self.client_factory)
            fut.result()
        except Exception as exc:  # noqa: BLE001 — degrade, never fail the run
            ok = False
            with self._probe_lock:
                if key not in self._probe_warned:
                    self._probe_warned.add(key)
                    msg = (
                        "payload refs probe failed for external pool "
                        f"({type(exc).__name__}: {exc}); routing spilled "
                        "payloads by value for this pool"
                    )
                    warnings.warn(msg, UserWarning, stacklevel=2)
                    emit_warning(msg)
        with self._probe_lock:
            self._probe_ok[key] = ok
            return ok


def _open_worker_store(
    store_config: S3StoreConfig,
    client_factory: Optional[Callable[[], Any]],
) -> S3Store:
    return S3Store.from_config(store_config, client_factory=client_factory)


def _probe_store_access(
    store_config: Optional[S3StoreConfig],
    client_factory: Optional[Callable[[], Any]],
) -> bool:
    if store_config is None:
        raise RuntimeError("payload refs probe missing store_config")
    store = _open_worker_store(store_config, client_factory)
    # Cheap authenticated call — missing key is fine; auth/config errors raise.
    store.exists("0" * 64)
    return True


def is_spill_ref(output: Any) -> bool:
    return isinstance(output, str) and output.startswith("objects:")


def read_output_strict(store: S3Store, output: str, content_type: Optional[str]) -> Any:
    """Like ``read_output``, but missing objects raise (never silent None)."""
    if not is_spill_ref(output):
        return store.read_output(output, content_type)
    content_hash = output[len("objects:") :]
    raw = store.read(content_hash)
    if raw is None:
        raise FileNotFoundError(
            f"spilled object {content_hash} missing from worker store"
        )
    return _deserialize_bytes(raw, content_type)


def _resolve_worker_arg(store: S3Store, value: Any) -> Any:
    if isinstance(value, StoreRef):
        return read_output_strict(store, value.output, value.content_type)
    if isinstance(value, dict):
        # Aggregate: {lane: StoreRef | value}
        return {lane: _resolve_worker_arg(store, item) for lane, item in value.items()}
    return value


def _ref_call(
    store_config: S3StoreConfig,
    client_factory: Optional[Callable[[], Any]],
    fn: Callable[..., Any],
    assertions: Tuple[Callable[[Any], None], ...],
    output_model: Any,
    run_id: str,
    coordinate: str,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Worker-side shim: resolve StoreRefs, run fn + validation, maybe spill."""
    store = _open_worker_store(store_config, client_factory)
    resolved_args = tuple(_resolve_worker_arg(store, a) for a in args)
    resolved_kwargs = {k: _resolve_worker_arg(store, v) for k, v in kwargs.items()}
    result = fn(*resolved_args, **resolved_kwargs)
    if isinstance(result, Filtered):
        return result
    if output_model is not None:
        output_model.model_validate(result)
    for assertion in assertions:
        assertion(result)
    inline, _value, content_type, raw_data = store.prepare_output(result)
    if inline:
        return result
    assert raw_data is not None
    ref = store._spill(run_id, coordinate, raw_data, content_type)
    content_hash = ref[len("objects:") :]
    reported = store.size_of(content_hash)
    size_bytes = reported if reported is not None else len(raw_data)
    return SpilledResult(output=ref, content_type=content_type, size_bytes=size_bytes)


def parent_as_ref_or_value(ref: Any, params: Optional[dict], memo: Any) -> Any:
    """For MatRef spills return StoreRef; else resolve by value (incl. ephemeral)."""
    from .execution import _resolve_parent_value
    from .planning import EphemeralRef, MatRef

    if isinstance(ref, EphemeralRef):
        return _resolve_parent_value(ref, params, memo)
    if isinstance(ref, MatRef) and is_spill_ref(getattr(ref, "output", None)):
        return StoreRef(output=ref.output, content_type=getattr(ref, "content_type", None))
    return _resolve_parent_value(ref, params, memo)
