"""Immutable Arrow lane segments on an S3-compatible object store."""
from __future__ import annotations

import json
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional
from urllib.parse import quote, unquote

import pyarrow as pa

from .lane_store import LaneStore, _schema
from .store import S3Store, _is_not_found, _is_precondition_failed


DEFAULT_COMPACTION_THRESHOLD = 16
DEFAULT_LEASE_TTL_SECONDS = 180
DEFAULT_LEASE_RENEW_SECONDS = 60


class PipelineLeaseError(RuntimeError):
    """A cloud pipeline already has an active writer."""


def _safe(value: str) -> str:
    return quote(value, safe="")


def _arrow_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


class CloudLaneStore(LaneStore):
    """LaneStore backed by immutable Arrow objects.

    Every flush writes a new segment. Readers LIST and concatenate segments,
    deduping by ``row_id`` so seeing a compacted base and its not-yet-deleted
    inputs is harmless.
    """

    def __init__(
        self,
        root: str,
        store: S3Store,
        *,
        table_cache_size: int = 16,
        compaction_threshold: int = DEFAULT_COMPACTION_THRESHOLD,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        lease_renew_seconds: int = DEFAULT_LEASE_RENEW_SECONDS,
    ):
        super().__init__(root, table_cache_size=table_cache_size)
        self.store = store
        self.compaction_threshold = compaction_threshold
        self.lease_ttl_seconds = lease_ttl_seconds
        self.lease_renew_seconds = lease_renew_seconds
        self._cloud_versions: Dict[tuple[str, str], tuple] = {}

    def clear_read_caches(self):
        super().clear_read_caches()
        self._cloud_versions.clear()

    def _relative_prefix(
        self, pipeline_id: str, step_name: str, *, anchor: bool = False
    ) -> str:
        kind = "anchor" if anchor else "main"
        return f"tables/{_safe(pipeline_id)}/{_safe(step_name)}/{kind}/"

    def _qualified(self, relative: str) -> str:
        return f"{self.store.prefix}{relative}"

    def _list(self, relative_prefix: str) -> list[dict[str, Any]]:
        client = self.store._get_client()
        prefix = self._qualified(relative_prefix)
        token: Optional[str] = None
        out: list[dict[str, Any]] = []
        while True:
            kwargs: Dict[str, Any] = {
                "Bucket": self.store.bucket,
                "Prefix": prefix,
            }
            if token:
                kwargs["ContinuationToken"] = token
            response = client.list_objects_v2(**kwargs)
            out.extend(response.get("Contents") or [])
            if not response.get("IsTruncated"):
                return out
            token = response.get("NextContinuationToken")

    def _read_key(self, key: str) -> Optional[tuple[bytes, str]]:
        client = self.store._get_client()
        try:
            response = client.get_object(Bucket=self.store.bucket, Key=key)
            etag = str(response.get("ETag") or "").strip('"')
            return response["Body"].read(), etag
        except Exception as exc:
            if _is_not_found(exc):
                return None
            raise

    def _put_conditional(
        self,
        key: str,
        data: bytes,
        *,
        if_none_match: bool = False,
        if_match: Optional[str] = None,
    ) -> Optional[str]:
        kwargs: Dict[str, Any] = {
            "Bucket": self.store.bucket,
            "Key": key,
            "Body": data,
        }
        if if_none_match:
            kwargs["IfNoneMatch"] = "*"
        if if_match:
            kwargs["IfMatch"] = if_match
        try:
            response = self.store._get_client().put_object(**kwargs)
            return str(response.get("ETag") or "").strip('"')
        except Exception as exc:
            if _is_precondition_failed(exc):
                return None
            raise

    def _delete_key(self, key: str, *, if_match: Optional[str] = None) -> bool:
        kwargs: Dict[str, Any] = {"Bucket": self.store.bucket, "Key": key}
        if if_match:
            kwargs["IfMatch"] = if_match
        try:
            self.store._get_client().delete_object(**kwargs)
            return True
        except Exception as exc:
            if _is_not_found(exc) or _is_precondition_failed(exc):
                return False
            # Older S3-compatible SDK models may not expose conditional delete.
            if if_match and "IfMatch" in str(exc):
                current = self._read_key(key)
                if current is None or current[1] != if_match:
                    return False
                self.store._get_client().delete_object(
                    Bucket=self.store.bucket, Key=key
                )
                return True
            raise

    def _segment_objects(
        self, pipeline_id: str, step_name: str, *, anchor: bool = False
    ) -> list[dict[str, Any]]:
        return self._list(
            self._relative_prefix(pipeline_id, step_name, anchor=anchor)
        )

    def _read_cloud_table(
        self, pipeline_id: str, step_name: str, *, anchor: bool = False
    ) -> Optional[pa.Table]:
        cache_key = (
            self._anchor_key(pipeline_id, step_name)
            if anchor
            else (pipeline_id, step_name)
        )
        objects = self._segment_objects(
            pipeline_id, step_name, anchor=anchor
        )
        version = tuple(
            sorted(
                (
                    obj["Key"],
                    str(obj.get("ETag") or ""),
                    int(obj.get("Size", 0)),
                )
                for obj in objects
            )
        )
        if (
            self._cloud_versions.get(cache_key) == version
            and cache_key in self._disk_table_cache
        ):
            self._disk_table_cache.move_to_end(cache_key)
            return self._disk_table_cache[cache_key]
        if not objects:
            self._disk_table_cache.pop(cache_key, None)
            self._address_index_cache.pop(cache_key, None)
            self._cloud_versions[cache_key] = version
            return None

        tables: list[pa.Table] = []
        for obj in sorted(objects, key=lambda item: item["Key"]):
            found = self._read_key(obj["Key"])
            if found is None:
                continue
            try:
                tables.append(pa.ipc.open_file(pa.BufferReader(found[0])).read_all())
            except Exception:
                # Immutable object is corrupt/incomplete: omit it. SQLite
                # liveness will turn an absent address into a recomputation.
                continue
        combined = self._concat_compatible_tables(tables)
        combined = self._dedupe_rows(combined)
        if combined is None:
            return None

        self._disk_table_cache[cache_key] = combined
        self._disk_table_cache.move_to_end(cache_key)
        self._cloud_versions[cache_key] = version
        while len(self._disk_table_cache) > self._disk_table_cache_max:
            evicted, _ = self._disk_table_cache.popitem(last=False)
            self._address_index_cache.pop(evicted, None)
            self._cloud_versions.pop(evicted, None)
        self._address_index_cache.pop(cache_key, None)
        return combined

    @staticmethod
    def _dedupe_rows(table: Optional[pa.Table]) -> Optional[pa.Table]:
        if table is None or table.num_rows < 2:
            return table
        row_ids = table.column("row_id").to_pylist()
        timestamps = table.column("ts").to_pylist()
        latest: Dict[str, int] = {}
        for index, (row_id, timestamp) in enumerate(zip(row_ids, timestamps)):
            previous = latest.get(row_id)
            if previous is None or timestamp >= timestamps[previous]:
                latest[row_id] = index
        indices = sorted(latest.values(), key=lambda index: timestamps[index])
        return table.take(indices)

    def _read_disk_table(self, pipeline_id: str, step_name: str):
        return self._read_cloud_table(pipeline_id, step_name)

    def _read_anchor_disk_table(self, pipeline_id: str, step_name: str):
        return self._read_cloud_table(pipeline_id, step_name, anchor=True)

    def _write_segment(
        self,
        pipeline_id: str,
        step_name: str,
        table: pa.Table,
        *,
        anchor: bool = False,
        compacted: bool = False,
    ) -> None:
        label = "base" if compacted else "segment"
        relative = self._relative_prefix(
            pipeline_id, step_name, anchor=anchor
        )
        key = self._qualified(f"{relative}{label}-{uuid.uuid4().hex}.arrow")
        if self._put_conditional(key, _arrow_bytes(table), if_none_match=True) is None:
            raise RuntimeError(f"cloud lane segment key collision: {key}")
        cache_key = (
            self._anchor_key(pipeline_id, step_name)
            if anchor
            else (pipeline_id, step_name)
        )
        self._disk_table_cache.pop(cache_key, None)
        self._address_index_cache.pop(cache_key, None)
        self._cloud_versions.pop(cache_key, None)

    def flush_step(self, pipeline_id: str, step_name: str):
        rows = self._buffer(pipeline_id, step_name)
        batches = self._arrow_batch_buffer(pipeline_id, step_name)
        if rows or batches:
            buf = self._buffer_table(pipeline_id, step_name)
            arrow = self._arrow_batch_table(pipeline_id, step_name)
            table = self._concat_compatible_tables(
                [item for item in (arrow, buf) if item is not None]
            )
            if table is not None:
                self._write_segment(pipeline_id, step_name, table)
            rows.clear()
            batches.clear()

        anchor_key = self._anchor_key(pipeline_id, step_name)
        anchor_rows = self._run_buffers.get(anchor_key, [])
        if anchor_rows:
            table = pa.Table.from_pylist(
                anchor_rows, schema=_schema(pa, pa.string())
            )
            self._write_segment(
                pipeline_id, step_name, table, anchor=True
            )
            anchor_rows.clear()

    def all_filled_rows(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        objects = self._list("tables/")
        steps: set[tuple[str, str, bool]] = set()
        prefix = self._qualified("tables/")
        for obj in objects:
            relative = obj["Key"][len(prefix):]
            parts = relative.split("/")
            if len(parts) != 4 or parts[2] not in {"main", "anchor"}:
                continue
            steps.add((unquote(parts[0]), unquote(parts[1]), parts[2] == "anchor"))
        for pipeline_id, step_name, anchor in steps:
            table = self._read_cloud_table(
                pipeline_id, step_name, anchor=anchor
            )
            if table is None:
                continue
            for row in table.to_pylist():
                row["pipeline_id"] = pipeline_id
                row["step_name"] = step_name
                result.append(row)
        return result

    def _compact_kind(
        self, pipeline_id: str, step_name: str, *, anchor: bool
    ) -> None:
        objects = self._segment_objects(
            pipeline_id, step_name, anchor=anchor
        )
        if len(objects) <= self.compaction_threshold:
            return
        table = self._read_cloud_table(
            pipeline_id, step_name, anchor=anchor
        )
        if table is None:
            return
        self._write_segment(
            pipeline_id,
            step_name,
            table,
            anchor=anchor,
            compacted=True,
        )
        for obj in objects:
            self._delete_key(obj["Key"])
        cache_key = (
            self._anchor_key(pipeline_id, step_name)
            if anchor
            else (pipeline_id, step_name)
        )
        self._disk_table_cache.pop(cache_key, None)
        self._address_index_cache.pop(cache_key, None)
        self._cloud_versions.pop(cache_key, None)

    def compact_pipeline(self, pipeline_id: str) -> None:
        prefix = f"tables/{_safe(pipeline_id)}/"
        objects = self._list(prefix)
        qualified = self._qualified(prefix)
        steps: set[tuple[str, bool]] = set()
        for obj in objects:
            parts = obj["Key"][len(qualified):].split("/")
            if len(parts) != 3 or parts[1] not in {"main", "anchor"}:
                continue
            steps.add((unquote(parts[0]), parts[1] == "anchor"))
        for step_name, anchor in steps:
            self._compact_kind(
                pipeline_id, step_name, anchor=anchor
            )

    def _lease_key(self, pipeline_id: str) -> str:
        return self._qualified(f"leases/{_safe(pipeline_id)}.json")

    def _lease_payload(self, run_id: str) -> bytes:
        return json.dumps(
            {
                "owner": run_id,
                "expires_at": time.time() + self.lease_ttl_seconds,
            },
            sort_keys=True,
        ).encode()

    def _acquire_lease(self, pipeline_id: str, run_id: str) -> str:
        key = self._lease_key(pipeline_id)
        etag = self._put_conditional(
            key, self._lease_payload(run_id), if_none_match=True
        )
        if etag is not None:
            return etag
        current = self._read_key(key)
        if current is None:
            return self._acquire_lease(pipeline_id, run_id)
        payload = json.loads(current[0])
        if float(payload.get("expires_at", 0)) > time.time():
            raise PipelineLeaseError(
                f"pipeline {pipeline_id!r} already has active writer "
                f"{payload.get('owner')!r}"
            )
        etag = self._put_conditional(
            key, self._lease_payload(run_id), if_match=current[1]
        )
        if etag is None:
            raise PipelineLeaseError(
                f"pipeline {pipeline_id!r} lease changed during takeover"
            )
        return etag

    @contextmanager
    def writer_lease(self, pipeline_id: str, run_id: str) -> Iterator[None]:
        key = self._lease_key(pipeline_id)
        etag = self._acquire_lease(pipeline_id, run_id)
        stop = threading.Event()
        lost = threading.Event()
        state = {"etag": etag}

        def renew() -> None:
            while not stop.wait(self.lease_renew_seconds):
                new_etag = self._put_conditional(
                    key,
                    self._lease_payload(run_id),
                    if_match=state["etag"],
                )
                if new_etag is None:
                    lost.set()
                    return
                state["etag"] = new_etag

        thread = threading.Thread(target=renew, daemon=True)
        thread.start()
        try:
            yield
            if lost.is_set():
                raise PipelineLeaseError(
                    f"pipeline {pipeline_id!r} writer lease was lost"
                )
        finally:
            stop.set()
            thread.join(timeout=1)
            current = self._read_key(key)
            if current is not None:
                try:
                    payload = json.loads(current[0])
                except Exception:
                    payload = {}
                if payload.get("owner") == run_id:
                    self._delete_key(key, if_match=current[1])
