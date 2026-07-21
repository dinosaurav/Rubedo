"""
Content-addressed object store operations + inline/spill serialization.

Phase 4: the Arrow ``output`` column is the sole source of truth for output
content.  Small values are stored inline as JSON strings (zero object-store
I/O); large values spill to the content-addressed object store with a ref
string (``"objects:<hash>"``) in the column.  The reader checks: if the
``output`` string starts with ``"objects:"``, read from the store; else
JSON-parse.

Stateful stores are owned by a ``Home`` (``Home.store``). ``LocalStore`` is
the filesystem backend; ``S3Store`` covers S3-compatible buckets (AWS S3,
R2, B2, MinIO). Both satisfy ``ObjectStore``. Pure serialization helpers
stay module-level.
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
    runtime_checkable,
)
from urllib.parse import parse_qs, unquote, urlparse

import pyarrow as pa

from .hashing import hash_bytes
from .util import _ensure_gitignore

PathLike = Union[str, os.PathLike]

SPILL_THRESHOLD = 4096  # bytes; serialized values larger than this spill

# Default streaming chunk for ``ObjectStore.stream`` / server downloads.
_STREAM_CHUNK = 64 * 1024


class HasOutputContentHash(Protocol):
    """Structural type for read_materialization_output's argument —
    a runner MatRef both satisfy this without either needing to know
    about the other."""

    output_content_hash: str
    content_type: Optional[str]


@dataclass(frozen=True)
class ObjectInfo:
    """One object from ``ObjectStore.inventory`` (key = content hash)."""

    key: str
    size: int
    etag: Optional[str] = None


@dataclass(frozen=True)
class StoreCapabilities:
    """What callers may assume about a store backend.

    ``local_paths``: ``object_path`` / filesystem ops are valid (LocalStore).
    ``destructive_gc``: retention GC may delete bytes (cloud hard-refuses
    until versioned-bucket gating lands — see ``notes/retention.md``).
    ``sized_inventory``: ``inventory()`` carries sizes/etags without
    per-object HEAD/GET — required for cloud ``du`` / GC dry-run.
    """

    local_paths: bool = False
    destructive_gc: bool = False
    sized_inventory: bool = False


@runtime_checkable
class ObjectStore(Protocol):
    """Provider-neutral content-addressed object store.

    ``write`` is create-if-absent (conditional put). A precondition failure
    (local exists-check / HTTP 412) is success — the object is already there.
    ``read`` / ``stream`` return ``None`` for missing keys (never raise).
    """

    @property
    def capabilities(self) -> StoreCapabilities: ...

    def exists(self, content_hash: str) -> bool: ...

    def read(self, content_hash: str) -> Optional[bytes]: ...

    def write(
        self,
        content_hash: str,
        data: bytes,
        *,
        run_id: str = "",
        coordinate: str = "",
    ) -> None: ...

    def delete(self, content_hash: str) -> None: ...

    def cleanup_staged(self, run_id: str) -> None: ...

    def inventory(self) -> Iterator[ObjectInfo]: ...

    def size_of(self, content_hash: str) -> Optional[int]: ...

    def stream(
        self, content_hash: str, *, chunk_size: int = _STREAM_CHUNK
    ) -> Optional[Iterator[bytes]]: ...

    def serialize_output(
        self, run_id: str, coordinate: str, result: Any
    ) -> Tuple[Any, str]: ...

    def read_output(self, output_value: Any, content_type: Optional[str]) -> Any: ...

    def read_materialization_output(self, materialization: Any) -> Any: ...


def resolve_object_sizes(
    store: ObjectStore, needed: Set[str]
) -> Dict[str, int]:
    """Map content hashes → byte sizes for objects that are present.

    Absent hashes are omitted (callers treat omission as missing). When the
    store advertises ``sized_inventory``, sizes come from one inventory pass
    — zero per-object HEAD/GET. Otherwise falls back to ``size_of`` per hash.
    """
    if not needed:
        return {}
    if store.capabilities.sized_inventory:
        out: Dict[str, int] = {}
        for info in store.inventory():
            if info.key in needed:
                out[info.key] = info.size
                if len(out) == len(needed):
                    break
        return out
    out = {}
    for h in needed:
        size = store.size_of(h)
        if size is not None:
            out[h] = size
    return out


# Subtypes of the `arrow-ipc` content_type — the full string is
# "arrow-ipc:<kind>" so read_materialization_output reconstructs the
# original Python type on cache hit (a polars user gets a polars
# DataFrame back, not a raw pyarrow Table — their step body's .filter()
# calls keep working).  Supported kinds: polars, pandas, table.


def _to_arrow_table(value: Any):
    """Return (pa.Table, kind) for any supported Arrow-compatible value.

    `kind` is the subtype tag persisted in content_type so the round-trip
    can reconstruct the original Python type.  Detection is by isinstance
    in a deliberately fixed order: polars-first (it's the common case in
    tests/examples and its DataFrames wrap Arrow natively), then pandas,
    then a bare pa.Table — returned as-is.
    """
    try:
        import polars as pl
        if isinstance(value, pl.DataFrame):
            return value.to_arrow(), "polars"
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return pa.Table.from_pandas(value), "pandas"
    except ImportError:
        pass
    if isinstance(value, pa.Table):
        return value, "table"
    raise TypeError(
        f"value of type {type(value).__name__} is not Arrow-compatible "
        "(expected polars/pandas DataFrame or pyarrow Table)"
    )


def _from_arrow_table(table: Any, kind: str):
    """Reconstruct the original Python type from a pa.Table on cache hit."""
    if kind == "polars":
        import polars as pl
        return pl.DataFrame(table)
    if kind == "pandas":
        return table.to_pandas()
    return table


def _try_arrow(value: Any) -> bool:
    """Is this value an Arrow-compatible output (DataFrame / pa.Table)?

    Detects via polars/pandas isinstance or a duck-type check on
    .schema/.column.  Used to decide whether to enter the Arrow-IPC
    serialization branch in _serialize.
    """
    try:
        import polars as pl
        if isinstance(value, pl.DataFrame):
            return True
    except ImportError:
        pass
    try:
        import pandas as pd
        if isinstance(value, pd.DataFrame):
            return True
    except ImportError:
        pass
    return hasattr(value, "schema") and hasattr(value, "column")


def _coerce(value: Any) -> Any:
    """Coerce a step output value to a JSON-storable form.

    Checks for a ``.model_dump()`` method (Pydantic v2) or ``.to_dict()``
    method (general protocol) and calls it to produce a dict.  Plain
    JSON-compatible values (dict, list, int, str, etc.) pass through
    unchanged.  Arrow-compatible values (pa.Table, polars/pandas
    DataFrames) also pass through — they have their own serialization
    path (Arrow IPC) and some have a ``.to_dict()`` method that would
    interfere.  Deserialization is one-way: the downstream step receives
    the dict, not the original class — reconstruct in the step function
    if needed (``MyModel(**parent_dict)``).
    """
    if _try_arrow(value):
        return value
    if hasattr(value, "model_dump") and callable(value.model_dump):
        return value.model_dump()
    if hasattr(value, "to_dict") and callable(value.to_dict):
        return value.to_dict()
    return value


def _serialize(result: Any) -> Tuple[bytes, str]:
    """Serialize an output value to bytes and return its content type.

    One format per value kind: `bytes`, `text`, `arrow-ipc:<kind>` (for
    DataFrame / Arrow table outputs — the `:kind` suffix records the
    original Python type so the round-trip reconstructs it), `json`
    (fallback for dicts and anything else JSON can carry).
    """
    value = _coerce(result)

    if isinstance(value, bytes):
        return value, "bytes"
    if isinstance(value, str):
        return value.encode("utf-8"), "text"

    if _try_arrow(value):
        table, kind = _to_arrow_table(value)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes(), f"arrow-ipc:{kind}"

    return (
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        "json",
    )


def _deserialize_bytes(raw_data: bytes, content_type: Optional[str]) -> Any:
    """Deserialize spilled object bytes using ``content_type``."""
    if content_type == "bytes":
        return raw_data
    if content_type == "text":
        return raw_data.decode("utf-8")
    if content_type == "json":
        return json.loads(raw_data.decode("utf-8"))
    if content_type and content_type.startswith("arrow-ipc:"):
        kind = content_type.split(":", 1)[1]
        reader = pa.ipc.open_stream(raw_data)
        table = reader.read_all()
        return _from_arrow_table(table, kind)
    try:
        return json.loads(raw_data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            return raw_data.decode("utf-8")
        except UnicodeDecodeError:
            return raw_data


class _SerializeMixin:
    """Shared spill/read helpers that call protocol ``write`` / ``read``."""

    def prepare_output(self, result: Any) -> Tuple[bool, Any, str, Optional[bytes]]:
        """Decide inline vs spill without writing.

        Returns ``(inline, value, content_type, raw_data)``:
        - inline True → ``value`` is the Arrow column value; ``raw_data`` is None
        - inline False → ``raw_data`` is the bytes to spill; ``value`` is None
        """
        value = _coerce(result)

        if isinstance(value, bytes):
            return False, None, "bytes", value

        if _try_arrow(value):
            table, kind = _to_arrow_table(value)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            raw_data = sink.getvalue().to_pybytes()
            return False, None, f"arrow-ipc:{kind}", raw_data

        try:
            raw_data = json.dumps(
                value, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"Cannot serialize value of type {type(value).__name__}: {e}"
            ) from e

        if len(raw_data) > SPILL_THRESHOLD:
            return False, None, "json", raw_data

        if isinstance(value, str):
            return True, value, "text", None
        return True, value, "json", None

    def serialize_output(
        self, run_id: str, coordinate: str, result: Any
    ) -> Tuple[Any, str]:
        """Serialize a step result for the Arrow ``output`` column.

        Returns ``(output_value, content_type)``:
        - **Inline**: ``output_value`` is the Python object itself (dict,
          int, string, etc.) — stored in the Arrow column as a native type
          (struct, int64, string).  No object-store write.  ``content_type``
          is ``"json"``.
        - **Spilled**: ``output_value`` is a ref string ``"objects:<hash>"``,
          ``content_type`` is ``"bytes"``/``"text"``/``"json"``/
          ``"arrow-ipc:<kind>"``.  The serialized bytes are written to the
          content-addressed object store.

        Spill triggers (any one triggers spill):
        - **Type-based**: ``bytes`` → always spill (can't go in an Arrow column)
        - **Type-based**: Arrow-compatible (DataFrame / pa.Table) → always spill
        - **Size-based**: JSON-serialized form > ``SPILL_THRESHOLD`` → spill
        """
        inline, value, content_type, raw_data = self.prepare_output(result)
        if inline:
            return value, content_type
        assert raw_data is not None
        return self._spill(run_id, coordinate, raw_data, content_type), content_type

    def _spill(
        self, run_id: str, coordinate: str, raw_data: bytes, content_type: str
    ) -> str:
        """Write serialized bytes and return the ref string ``objects:<hash>``."""
        content_hash = hash_bytes(raw_data)
        self.write(  # type: ignore[attr-defined]
            content_hash, raw_data, run_id=run_id, coordinate=coordinate
        )
        return f"objects:{content_hash}"

    def read_output(self, output_value: Any, content_type: Optional[str]) -> Any:
        """Read and deserialize a value from the Arrow ``output`` column.

        The ``output`` column may hold:
        - **Native Arrow values** (struct, int64, string, etc.) — returned
          directly as Python objects (the column was stored natively).
        - **Ref strings** (``"objects:<hash>"``) — read the bytes from the
          object store and deserialize using ``content_type``.
        - **JSON strings** (the fallback when inline/spill are mixed in one
          step file) — parse with ``json.loads``.
        """
        if output_value is None:
            return None

        if not isinstance(output_value, str):
            return output_value

        if output_value.startswith("objects:"):
            content_hash = output_value[len("objects:"):]
            raw_data = self.read(content_hash)  # type: ignore[attr-defined]
            if raw_data is None:
                return None
            return _deserialize_bytes(raw_data, content_type)

        if content_type == "json":
            try:
                return json.loads(output_value)
            except (json.JSONDecodeError, TypeError):
                return output_value
        return output_value

    def read_materialization_output(self, materialization: Any) -> Any:
        """Backward-compatible wrapper: reads from the ``output`` and
        ``content_type`` attributes (MatRef, _ArrowRowRef, or any object with
        those fields)."""
        return self.read_output(
            getattr(materialization, "output", None),
            getattr(materialization, "content_type", None),
        )


class LocalStore(_SerializeMixin):
    """Filesystem object store + staging for one home root."""

    def __init__(self, root: PathLike):
        self.root = os.path.abspath(os.fspath(root))
        self.objects_dir = os.path.join(self.root, "objects")
        self.staging_dir = os.path.join(self.root, "staging")
        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.staging_dir, exist_ok=True)
        _ensure_gitignore(self.root)

    @property
    def capabilities(self) -> StoreCapabilities:
        return StoreCapabilities(
            local_paths=True,
            destructive_gc=True,
            sized_inventory=True,
        )

    def object_path(self, content_hash: str) -> str:
        """Compute the path for a content-hashed object."""
        return os.path.join(
            self.objects_dir, content_hash[:2], content_hash[2:4], content_hash
        )

    def _staging_path(self, run_id: str, coordinate: str, content_hash: str) -> str:
        """Compute the temporary path for staging an object before commit."""
        safe_coord = coordinate.replace("/", "_").replace("\\", "_")
        return os.path.join(
            self.staging_dir, run_id, safe_coord, f"{content_hash}.tmp"
        )

    def exists(self, content_hash: str) -> bool:
        return os.path.exists(self.object_path(content_hash))

    def read(self, content_hash: str) -> Optional[bytes]:
        path = self.object_path(content_hash)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def write(
        self,
        content_hash: str,
        data: bytes,
        *,
        run_id: str = "",
        coordinate: str = "",
    ) -> None:
        """Create-if-absent via staging + atomic replace."""
        final_path = self.object_path(content_hash)
        if os.path.exists(final_path):
            return

        staging_path = self._staging_path(
            run_id or "_", coordinate or "_", content_hash
        )
        os.makedirs(os.path.dirname(staging_path), exist_ok=True)
        with open(staging_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        try:
            os.replace(staging_path, final_path)
        except OSError:
            # Lost a race to another writer — content-addressed, so equal.
            if os.path.exists(final_path):
                try:
                    os.remove(staging_path)
                except OSError:
                    pass
                return
            raise

    def delete(self, content_hash: str) -> None:
        try:
            os.remove(self.object_path(content_hash))
        except OSError:
            pass

    def cleanup_staged(self, run_id: str) -> None:
        """Remove any temporary staged files for the given run."""
        run_staging = os.path.join(self.staging_dir, run_id)
        if os.path.exists(run_staging):
            try:
                shutil.rmtree(run_staging)
            except Exception:
                pass

    def inventory(self) -> Iterator[ObjectInfo]:
        for dirpath, _dirs, files in os.walk(self.objects_dir):
            for name in files:
                path = os.path.join(dirpath, name)
                try:
                    size = os.path.getsize(path)
                except OSError:
                    continue
                yield ObjectInfo(key=name, size=size)

    def size_of(self, content_hash: str) -> Optional[int]:
        try:
            return os.path.getsize(self.object_path(content_hash))
        except OSError:
            return None

    def stream(
        self, content_hash: str, *, chunk_size: int = _STREAM_CHUNK
    ) -> Optional[Iterator[bytes]]:
        path = self.object_path(content_hash)
        if not os.path.exists(path):
            return None

        def _chunks() -> Iterator[bytes]:
            with open(path, "rb") as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

        return _chunks()

    def stage_and_commit(
        self, run_id: str, coordinate: str, result: Any
    ) -> Tuple[str, str, str]:
        """Backward-compatible wrapper around ``serialize_output``.

        Returns ``(ref_or_json, content_hash, content_type)``.  For spilled
        values this is the object store path.  For inline values this is a
        JSON string (the native value canonicalized).  Tests that patch
        ``stage_and_commit`` continue to work.
        """
        output_value, content_type = self.serialize_output(run_id, coordinate, result)
        if isinstance(output_value, str) and output_value.startswith("objects:"):
            obj_hash = output_value[len("objects:"):]
            return self.object_path(obj_hash), obj_hash, content_type
        if isinstance(output_value, str):
            json_str = output_value
        else:
            json_str = json.dumps(output_value, sort_keys=True, separators=(",", ":"))
        return json_str, hash_bytes(json_str.encode("utf-8")), content_type


@dataclass(frozen=True)
class S3StoreConfig:
    """Picklable worker-safe config for reconstructing an ``S3Store``.

    Preserved early so item 13 (direct worker→store access for spilled
    blobs) does not require redesign. Credentials stay out of the config
    — workers use the ambient AWS credential chain / injected factory.
    """

    bucket: str
    prefix: str = ""
    endpoint_url: Optional[str] = None
    region_name: Optional[str] = None


def _normalize_prefix(prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"{prefix}/" if prefix else ""


def parse_store_url(url: str) -> Tuple[str, str, Dict[str, Optional[str]]]:
    """Parse ``s3://bucket/prefix?endpoint_url=…&region=…`` → components.

    Returns ``(bucket, prefix, options)`` where ``prefix`` has no leading
    slash and a trailing slash when non-empty, and ``options`` may carry
    ``endpoint_url`` / ``region_name``.
    """
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        raise ValueError(
            f"Unsupported store URL scheme {parsed.scheme!r}; expected s3://"
        )
    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"Store URL missing bucket: {url!r}")
    path = unquote(parsed.path.lstrip("/"))
    prefix = _normalize_prefix(path)
    qs = parse_qs(parsed.query)
    endpoint_vals = qs.get("endpoint_url")
    region_vals = qs.get("region") or qs.get("region_name")
    options: Dict[str, Optional[str]] = {
        "endpoint_url": endpoint_vals[0] if endpoint_vals else None,
        "region_name": region_vals[0] if region_vals else None,
    }
    return bucket, prefix, options


def open_store(url: str, *, client_factory: Optional[Callable[[], Any]] = None) -> "S3Store":
    """Construct an ``S3Store`` from an ``s3://`` URL."""
    bucket, prefix, options = parse_store_url(url)
    return S3Store(
        bucket=bucket,
        prefix=prefix,
        endpoint_url=options.get("endpoint_url"),
        region_name=options.get("region_name"),
        client_factory=client_factory,
    )


class S3Store(_SerializeMixin):
    """S3-compatible content-addressed object store (AWS S3, R2, B2, MinIO).

    Staging is a local concept — uploads go directly with a conditional put
    (``IfNoneMatch="*"``); a 412 PreconditionFailed is success. ``cleanup_staged``
    is a no-op. Key layout mirrors LocalStore under the configured prefix:
    ``{prefix}objects/{ab}/{cd}/{hash}``.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = "",
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        client_factory: Optional[Callable[[], Any]] = None,
        client: Any = None,
    ):
        if client is not None and client_factory is not None:
            raise ValueError("S3Store: pass client= or client_factory=, not both")
        self.bucket = bucket
        self.prefix = _normalize_prefix(prefix)
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self._client_factory = client_factory
        self._client = client
        self.store_config = S3StoreConfig(
            bucket=bucket,
            prefix=self.prefix,
            endpoint_url=endpoint_url,
            region_name=region_name,
        )

    @classmethod
    def from_config(
        cls,
        config: S3StoreConfig,
        *,
        client_factory: Optional[Callable[[], Any]] = None,
    ) -> "S3Store":
        """Rebuild an ``S3Store`` from picklable worker-safe config."""
        return cls(
            bucket=config.bucket,
            prefix=config.prefix,
            endpoint_url=config.endpoint_url,
            region_name=config.region_name,
            client_factory=client_factory,
        )

    @property
    def client_factory(self) -> Optional[Callable[[], Any]]:
        return self._client_factory

    @property
    def capabilities(self) -> StoreCapabilities:
        return StoreCapabilities(
            local_paths=False,
            destructive_gc=False,
            sized_inventory=True,
        )

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        if self._client_factory is not None:
            self._client = self._client_factory()
            return self._client
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "S3Store requires boto3; install with: pip install 'rubedo[s3]'"
            ) from e
        kwargs: Dict[str, Any] = {}
        if self.endpoint_url:
            kwargs["endpoint_url"] = self.endpoint_url
        if self.region_name:
            kwargs["region_name"] = self.region_name
        self._client = boto3.client("s3", **kwargs)
        return self._client

    def object_key(self, content_hash: str) -> str:
        return (
            f"{self.prefix}objects/"
            f"{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"
        )

    def exists(self, content_hash: str) -> bool:
        client = self._get_client()
        try:
            client.head_object(Bucket=self.bucket, Key=self.object_key(content_hash))
            return True
        except Exception as e:
            if _is_not_found(e):
                return False
            raise

    def read(self, content_hash: str) -> Optional[bytes]:
        client = self._get_client()
        try:
            resp = client.get_object(
                Bucket=self.bucket, Key=self.object_key(content_hash)
            )
            return resp["Body"].read()
        except Exception as e:
            if _is_not_found(e):
                return None
            raise

    def write(
        self,
        content_hash: str,
        data: bytes,
        *,
        run_id: str = "",
        coordinate: str = "",
    ) -> None:
        """Conditional put — PreconditionFailed (412) means already present."""
        if self.exists(content_hash):
            return
        client = self._get_client()
        key = self.object_key(content_hash)
        try:
            client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data,
                IfNoneMatch="*",
            )
        except Exception as e:
            # 412 / precondition: object appeared between exists and put.
            if _is_precondition_failed(e) or self.exists(content_hash):
                return
            # Some S3-compatible servers ignore IfNoneMatch; fall back to
            # overwrite of identical content-addressed bytes.
            if _is_unsupported_if_none_match(e):
                client.put_object(Bucket=self.bucket, Key=key, Body=data)
                return
            raise

    def delete(self, content_hash: str) -> None:
        client = self._get_client()
        try:
            client.delete_object(
                Bucket=self.bucket, Key=self.object_key(content_hash)
            )
        except Exception as e:
            if _is_not_found(e):
                return
            raise

    def cleanup_staged(self, run_id: str) -> None:
        return None

    def inventory(self) -> Iterator[ObjectInfo]:
        client = self._get_client()
        prefix = f"{self.prefix}objects/"
        token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": prefix,
            }
            if token:
                kwargs["ContinuationToken"] = token
            resp = client.list_objects_v2(**kwargs)
            for obj in resp.get("Contents") or []:
                key = obj["Key"]
                content_hash = key.rsplit("/", 1)[-1]
                if len(content_hash) < 8:
                    continue
                yield ObjectInfo(
                    key=content_hash,
                    size=int(obj.get("Size", 0)),
                    etag=(obj.get("ETag") or "").strip('"') or None,
                )
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")

    def size_of(self, content_hash: str) -> Optional[int]:
        client = self._get_client()
        try:
            resp = client.head_object(
                Bucket=self.bucket, Key=self.object_key(content_hash)
            )
            return int(resp["ContentLength"])
        except Exception as e:
            if _is_not_found(e):
                return None
            raise

    def stream(
        self, content_hash: str, *, chunk_size: int = _STREAM_CHUNK
    ) -> Optional[Iterator[bytes]]:
        client = self._get_client()
        try:
            resp = client.get_object(
                Bucket=self.bucket, Key=self.object_key(content_hash)
            )
        except Exception as e:
            if _is_not_found(e):
                return None
            raise
        body = resp["Body"]

        def _chunks() -> Iterator[bytes]:
            try:
                yield from body.iter_chunks(chunk_size=chunk_size)
            finally:
                try:
                    body.close()
                except Exception:
                    pass

        return _chunks()


def _error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None) or {}
    error = response.get("Error") or {}
    return str(error.get("Code") or "")


def _http_status(exc: BaseException) -> Optional[int]:
    response = getattr(exc, "response", None) or {}
    meta = response.get("ResponseMetadata") or {}
    status = meta.get("HTTPStatusCode")
    return int(status) if status is not None else None


def _is_not_found(exc: BaseException) -> bool:
    code = _error_code(exc)
    status = _http_status(exc)
    name = type(exc).__name__
    if name in {"NoSuchKey", "404", "NotFound"}:
        return True
    if code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}:
        return True
    if status == 404:
        return True
    # botocore ClientError often surfaces as 404 with Code=404 for head_object.
    msg = str(exc).lower()
    return "not found" in msg or "nosuchkey" in msg


def _is_precondition_failed(exc: BaseException) -> bool:
    code = _error_code(exc)
    status = _http_status(exc)
    if status == 412:
        return True
    return code in {"PreconditionFailed", "412"}


def _is_unsupported_if_none_match(exc: BaseException) -> bool:
    """Some S3-compatible APIs reject IfNoneMatch as an unknown param."""
    msg = str(exc).lower()
    return "ifnonematch" in msg or "if-none-match" in msg or "unknown parameter" in msg
