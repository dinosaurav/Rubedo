"""
Content-addressed object store operations + inline/spill serialization.

Phase 4: the Arrow ``output`` column is the sole source of truth for output
content.  Small values are stored inline as JSON strings (zero object-store
I/O); large values spill to the content-addressed object store with a ref
string (``"objects:<hash>"``) in the column.  The reader checks: if the
``output`` string starts with ``"objects:"``, read from the store; else
JSON-parse.

Stateful paths live on ``LocalStore`` (owned by a ``Home``). Pure
serialization helpers stay module-level.
"""
from __future__ import annotations

import json
import os
import shutil
from typing import Any, Optional, Protocol, Tuple, Union

import pyarrow as pa

from .hashing import hash_bytes
from .util import _ensure_gitignore

PathLike = Union[str, os.PathLike]

SPILL_THRESHOLD = 4096  # bytes; serialized values larger than this spill


class HasOutputContentHash(Protocol):
    """Structural type for read_materialization_output's argument — a
    a runner MatRef both satisfy this without
    either needing to know about the other."""

    output_content_hash: str
    content_type: Optional[str]


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


class LocalStore:
    """Filesystem object store + staging for one home root."""

    def __init__(self, root: PathLike):
        self.root = os.path.abspath(os.fspath(root))
        self.objects_dir = os.path.join(self.root, "objects")
        self.staging_dir = os.path.join(self.root, "staging")
        os.makedirs(self.objects_dir, exist_ok=True)
        os.makedirs(self.staging_dir, exist_ok=True)
        _ensure_gitignore(self.root)

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
        value = _coerce(result)

        if isinstance(value, bytes):
            return self._spill(run_id, coordinate, value, "bytes"), "bytes"

        if _try_arrow(value):
            table, kind = _to_arrow_table(value)
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            raw_data = sink.getvalue().to_pybytes()
            content_type = f"arrow-ipc:{kind}"
            return self._spill(run_id, coordinate, raw_data, content_type), content_type

        try:
            raw_data = json.dumps(
                value, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"Cannot serialize value of type {type(value).__name__}: {e}"
            ) from e

        if len(raw_data) > SPILL_THRESHOLD:
            return self._spill(run_id, coordinate, raw_data, "json"), "json"

        if isinstance(value, str):
            return value, "text"
        return value, "json"

    def _spill(
        self, run_id: str, coordinate: str, raw_data: bytes, content_type: str
    ) -> str:
        """Write serialized bytes to the content-addressed object store and
        return the ref string ``"objects:<hash>"``."""
        content_hash = hash_bytes(raw_data)
        final_path = self.object_path(content_hash)

        if os.path.exists(final_path):
            return f"objects:{content_hash}"

        staging_path = self._staging_path(run_id, coordinate, content_hash)
        os.makedirs(os.path.dirname(staging_path), exist_ok=True)
        with open(staging_path, "wb") as f:
            f.write(raw_data)
            f.flush()
            os.fsync(f.fileno())

        os.makedirs(os.path.dirname(final_path), exist_ok=True)
        os.replace(staging_path, final_path)

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
            obj_path = self.object_path(content_hash)
            if not os.path.exists(obj_path):
                return None
            with open(obj_path, "rb") as f:
                raw_data = f.read()

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

        if content_type == "json":
            try:
                return json.loads(output_value)
            except (json.JSONDecodeError, TypeError):
                return output_value
        return output_value

    def cleanup_staged(self, run_id: str) -> None:
        """Remove any temporary staged files for the given run."""
        run_staging = os.path.join(self.staging_dir, run_id)
        if os.path.exists(run_staging):
            try:
                shutil.rmtree(run_staging)
            except Exception:
                pass

    def read_materialization_output(self, materialization) -> Any:
        """Backward-compatible wrapper: reads from the ``output`` and
        ``content_type`` attributes (MatRef, _ArrowRowRef, or any object with
        those fields)."""
        return self.read_output(
            getattr(materialization, "output", None),
            getattr(materialization, "content_type", None),
        )

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
