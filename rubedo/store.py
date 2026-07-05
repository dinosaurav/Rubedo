"""
Content-addressed object store operations.
"""
import os
import json
from typing import Any, Optional, Protocol, Tuple
from .models import ProcessResult
from .hashing import hash_bytes


class HasOutputContentHash(Protocol):
    """Structural type for read_materialization_output's argument — a
    Materialization row and a runner MatRef both satisfy this without
    either needing to know about the other."""

    output_content_hash: str
    content_type: Optional[str]

def _default_home() -> str:
    return os.environ.get("RUBEDO_HOME", ".rubedo")


OBJECTS_DIR = os.path.join(_default_home(), "objects")
STAGING_DIR = os.path.join(_default_home(), "staging")


def _ensure_gitignore(directory: str):
    """Ensure a directory is gitignored."""
    if not directory:
        return
    gitignore_path = os.path.join(directory, ".gitignore")
    if not os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "w") as f:
                f.write(
                    "# Ignore everything in this directory\n*\n# Except this file\n!.gitignore\n"
                )
        except Exception:
            pass


def init_store(home: str = None):
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
    """Serialize an output value to bytes and return its content type."""
    value = result.value if isinstance(result, ProcessResult) else result

    if isinstance(value, bytes):
        return value, "bytes"
    if isinstance(value, str):
        return value.encode("utf-8"), "text"
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
