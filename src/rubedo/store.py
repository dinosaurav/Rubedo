"""
Content-addressed object store operations.
"""
import os
from .util import _ensure_gitignore
import json
from typing import Any, Optional, Protocol, Tuple
import pyarrow as pa
from .models import ProcessResult
from .hashing import hash_bytes


class HasOutputContentHash(Protocol):
    """Structural type for read_materialization_output's argument — a
    Materialization row and a runner MatRef both satisfy this without
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

def _default_home() -> str:
    return os.environ.get("RUBEDO_HOME", ".rubedo")


OBJECTS_DIR = os.path.join(_default_home(), "objects")
STAGING_DIR = os.path.join(_default_home(), "staging")


def init_store(home: Optional[str] = None):
    """Ensure the objects and staging directories exist.

    home (optional): an explicit root, overriding OBJECTS_DIR/STAGING_DIR
    for the rest of the process (mirrors db.init_db's db_path precedent).
    With no home given, this just ensures whatever's currently configured
    exists — it never resets an already-set custom home back to the
    RUBEDO_HOME/default, which matters since stage_and_commit() calls this
    with no arguments on every commit.
    """
    global OBJECTS_DIR, STAGING_DIR
    if home is not None:
        OBJECTS_DIR = os.path.join(home, "objects")
        STAGING_DIR = os.path.join(home, "staging")
    os.makedirs(OBJECTS_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)
    _ensure_gitignore(os.path.dirname(OBJECTS_DIR))


def _get_object_path(content_hash: str) -> str:
    """Compute the path for a content-hashed object."""
    return os.path.join(OBJECTS_DIR, content_hash[:2], content_hash[2:4], content_hash)


def _get_staging_path(run_id: str, coordinate: str, content_hash: str) -> str:
    """Compute the temporary path for staging an object before commit."""
    safe_coord = coordinate.replace("/", "_").replace("\\", "_")
    return os.path.join(STAGING_DIR, run_id, safe_coord, f"{content_hash}.tmp")


def _serialize(result: Any) -> Tuple[bytes, str]:
    """Serialize an output value to bytes and return its content type.

    One format per value kind: `bytes`, `text`, `arrow-ipc:<kind>` (for
    DataFrame / Arrow table outputs — the `:kind` suffix records the
    original Python type so the round-trip reconstructs it), `json`
    (fallback for dicts and anything else JSON can carry).
    """
    value = result.value if isinstance(result, ProcessResult) else result

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


def stage_and_commit(run_id: str, coordinate: str, result: Any) -> Tuple[str, str, str]:
    """Persist a step result and return (object_path, content_hash, content_type).

    Objects are stored by content hash, so committing is idempotent: identical
    bytes land at the same path and are never rewritten, and a re-execution
    that produces different bytes gets a new object without disturbing the old
    one (committed outputs stay immutable).
    """
    init_store()

    raw_data, content_type = _serialize(result)
    content_hash = hash_bytes(raw_data)
    final_path = _get_object_path(content_hash)

    if os.path.exists(final_path):
        return final_path, content_hash, content_type

    # 1. worker writes result to staging path
    staging_path = _get_staging_path(run_id, coordinate, content_hash)
    os.makedirs(os.path.dirname(staging_path), exist_ok=True)

    with open(staging_path, "wb") as f:
        f.write(raw_data)
        f.flush()
        os.fsync(f.fileno())

    # 2. commit object atomically
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    os.replace(staging_path, final_path)

    return final_path, content_hash, content_type


def read_materialization_output(materialization: Optional[HasOutputContentHash]) -> Any:
    """Reads and deserializes a materialization output from the store.

    Accepts anything carrying output_content_hash and content_type
    (a Materialization row or a runner MatRef).
    """
    if not materialization or not materialization.output_content_hash:
        return None
    obj_path = _get_object_path(materialization.output_content_hash)
    if not os.path.exists(obj_path):
        return None

    with open(obj_path, "rb") as f:
        raw_data = f.read()

    content_type = getattr(materialization, "content_type", None)
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

    # Legacy rows without a content_type: best-effort guess
    try:
        return json.loads(raw_data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        try:
            return raw_data.decode("utf-8")
        except UnicodeDecodeError:
            return raw_data

def cleanup_staged(run_id: str):
    """Remove any temporary staged files for the given run."""
    import shutil
    run_staging = os.path.join(STAGING_DIR, run_id)
    if os.path.exists(run_staging):
        try:
            shutil.rmtree(run_staging)
        except Exception:
            pass
