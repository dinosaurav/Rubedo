"""Pass-by-reference spilled payloads (TODO 13)."""
from __future__ import annotations

import warnings
from typing import Any

import boto3
import pytest
from moto import mock_aws

from rubedo import pipeline, step
from rubedo.home import Home
from rubedo.models import RunCoordinateStatus
from rubedo.store import SPILL_THRESHOLD, S3Store
from test_external_executor import FakePool, POOLS, make_fake_pool
import rubedo.execution as execution
import rubedo.payload_refs as payload_refs


def _blob(tag: str) -> bytes:
    return (tag * ((SPILL_THRESHOLD // len(tag)) + 2)).encode()


def _s3_store(bucket: str, prefix: str) -> S3Store:
    client = boto3.client("s3", region_name="us-east-1")
    client.create_bucket(Bucket=bucket)

    def factory():
        return boto3.client("s3", region_name="us-east-1")

    return S3Store(bucket=bucket, prefix=prefix, client_factory=factory)


def _snapshot(home, run_id: str) -> list[tuple[str, str, str]]:
    with home.session() as session:
        rows = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=run_id)
            .order_by(RunCoordinateStatus.step_name, RunCoordinateStatus.coordinate)
            .all()
        )
        return [
            (str(row.coordinate), str(row.status), str(row.output_address))
            for row in rows
        ]


class _CountingStore:
    """Wrap an ObjectStore counting payload read/write calls.

    Mixin methods on the inner store are bound to the inner instance, so
    ``serialize_output`` / ``read_output`` must be re-implemented here to
    route ``write`` / ``read`` through the counters.
    """

    def __init__(self, inner: S3Store):
        self._inner = inner
        self.store_config = inner.store_config
        self.client_factory = inner.client_factory
        self.reads: list[str] = []
        self.writes: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def read(self, content_hash: str) -> Any:
        self.reads.append(content_hash)
        return self._inner.read(content_hash)

    def write(self, content_hash: str, data: bytes, **kwargs: Any) -> None:
        self.writes.append(content_hash)
        return self._inner.write(content_hash, data, **kwargs)

    def prepare_output(self, result: Any):
        return self._inner.prepare_output(result)

    def serialize_output(self, run_id: str, coordinate: str, result: Any):
        from rubedo.hashing import hash_bytes

        inline, value, content_type, raw_data = self.prepare_output(result)
        if inline:
            return value, content_type
        assert raw_data is not None
        content_hash = hash_bytes(raw_data)
        self.write(content_hash, raw_data, run_id=run_id, coordinate=coordinate)
        return f"objects:{content_hash}", content_type

    def read_output(self, output_value: Any, content_type=None):
        if isinstance(output_value, str) and output_value.startswith("objects:"):
            content_hash = output_value[len("objects:") :]
            raw = self.read(content_hash)
            if raw is None:
                return None
            from rubedo.store import _deserialize_bytes

            return _deserialize_bytes(raw, content_type)
        return self._inner.read_output(output_value, content_type)

    def size_of(self, content_hash: str):
        return self._inner.size_of(content_hash)


@pytest.fixture
def count_shim(monkeypatch):
    calls = {"n": 0}
    real = execution._ref_call

    def counted(*a, **k):
        calls["n"] += 1
        return real(*a, **k)

    monkeypatch.setattr(execution, "_ref_call", counted)
    return calls


@mock_aws
def test_inline_only_never_engages_shim(tmp_path, count_shim):
    POOLS.clear()
    store = _s3_store("rubedo-refs-inline", "i/")
    home = Home.ephemeral(str(tmp_path / "inline_env"), store=store)

    @step
    def root():
        yield {"n": 1}
        yield {"n": 2}

    @step(executor=make_fake_pool)
    def add(root: dict):
        return {"n": root["n"] + 1}

    pipeline(name="refs_inline", steps=[root, add], home=home).run(workers=1)
    assert count_shim["n"] == 0


@mock_aws
def test_spill_parents_skip_runner_hub_bytes_and_match_parity(tmp_path, count_shim):
    POOLS.clear()
    blob = _blob("A")

    counted = _CountingStore(_s3_store("rubedo-refs-parity", "on/"))
    home_on = Home.ephemeral(str(tmp_path / "refs_on_env"), store=counted)

    @step
    def src():
        yield {"tag": "a"}

    @step
    def produce(src: dict):
        return blob + src["tag"].encode()

    @step(executor=make_fake_pool)
    def consume(produce: bytes):
        return produce + b"|ok"

    pipe_on = pipeline(
        name="refs_parity_on", steps=[src, produce, consume], home=home_on
    )
    summary_on = pipe_on.run(workers=1)
    assert count_shim["n"] >= 1
    # produce spilled via the runner-side store; consume's worker spill must
    # not appear as an extra write on the instrumented runner store.
    assert counted.writes, "produce should hub-spill"
    produce_hashes = set(counted.writes)
    # Runner must not GET those parent blobs when resolving consume inputs.
    assert not any(h in counted.reads for h in produce_hashes)
    # Worker PUT goes through a separate S3Store — no additional runner writes.
    assert set(counted.writes) == produce_hashes

    snap_on = _snapshot(home_on, summary_on.run_id)

    POOLS.clear()
    count_shim["n"] = 0
    home_off = Home.ephemeral(
        str(tmp_path / "refs_off_env"),
        store=_s3_store("rubedo-refs-parity-off", "off/"),
    )

    @step
    def src_off():
        yield {"tag": "a"}

    @step
    def produce_off(src_off: dict):
        return blob + src_off["tag"].encode()

    @step(executor=make_fake_pool)
    def consume_off(produce_off: bytes):
        return produce_off + b"|ok"

    summary_off = pipeline(
        name="refs_parity_off",
        steps=[src_off, produce_off, consume_off],
        home=home_off,
    ).run(workers=1, payload_refs=False)
    assert count_shim["n"] == 0
    snap_off = _snapshot(home_off, summary_off.run_id)
    assert [row[:2] for row in snap_on] == [row[:2] for row in snap_off]
    assert summary_on.created_count == summary_off.created_count


@mock_aws
def test_payload_refs_false_forces_hub(tmp_path, count_shim):
    POOLS.clear()
    store = _s3_store("rubedo-refs-force", "x/")
    home = Home.ephemeral(str(tmp_path / "force_env"), store=store)
    blob = _blob("B")

    @step
    def src():
        yield {"t": 1}

    @step
    def produce(src: dict):
        return blob

    @step(executor=make_fake_pool)
    def consume(produce: bytes):
        return len(produce)

    pipeline(name="refs_force", steps=[src, produce, consume], home=home).run(
        workers=1, payload_refs=False
    )
    assert count_shim["n"] == 0


@mock_aws
def test_probe_failure_warns_and_degrades(tmp_path):
    POOLS.clear()

    class BrokenPool(FakePool):
        def submit(self, fn, /, *args, **kwargs):
            if getattr(fn, "__name__", "") == "_probe_store_access":
                fut: Any = __import__("concurrent.futures").futures.Future()
                fut.set_exception(RuntimeError("no credentials"))
                return fut
            return super().submit(fn, *args, **kwargs)

    def make_broken():
        pool = BrokenPool()
        POOLS.append(pool)
        return pool

    store = _s3_store("rubedo-refs-probe", "p/")
    home = Home.ephemeral(str(tmp_path / "probe_env"), store=store)
    blob = _blob("C")

    @step
    def src():
        yield {"t": 1}

    @step
    def produce(src: dict):
        return blob

    @step(executor=make_broken)
    def consume(produce: bytes):
        return len(produce)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        summary = pipeline(
            name="refs_probe", steps=[src, produce, consume], home=home
        ).run(workers=1)
    assert summary.failed_count == 0
    assert any("payload refs probe failed" in str(w.message) for w in caught)


@mock_aws
def test_aggregate_over_spilled_parents_uses_shim(tmp_path, count_shim):
    POOLS.clear()
    store = _s3_store("rubedo-refs-agg", "a/")
    home = Home.ephemeral(str(tmp_path / "agg_env"), store=store)
    blob = _blob("D")

    @step
    def src():
        for i in range(3):
            yield {"i": i}

    @step
    def produce(src: dict):
        return blob + bytes([src["i"]])

    @step(executor=make_fake_pool, in_shape="aggregate", depends_on=["produce"])
    def total(produce: dict):
        return sum(len(v) for v in produce.values())

    summary = pipeline(
        name="refs_agg", steps=[src, produce, total], home=home
    ).run(workers=1)
    assert count_shim["n"] == 1
    assert summary.failed_count == 0
    assert summary.output_for("total") is not None


@mock_aws
def test_local_store_never_enables_refs(tmp_path, count_shim):
    POOLS.clear()
    home = Home.ephemeral(str(tmp_path / "local_env"))
    blob = _blob("E")

    @step
    def src():
        yield {"t": 1}

    @step
    def produce(src: dict):
        return blob

    @step(executor=make_fake_pool)
    def consume(produce: bytes):
        return len(produce)

    pipeline(name="refs_local", steps=[src, produce, consume], home=home).run(
        workers=1
    )
    assert count_shim["n"] == 0
    assert not payload_refs.PayloadRefsState.from_home(home).enabled
