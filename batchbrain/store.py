import os
import json
from typing import Any, Tuple
from .models import ProcessResult
from .hashing import hash_bytes

OBJECTS_DIR = ".batchbrain/objects"
STAGING_DIR = ".batchbrain/staging"


def _ensure_gitignore(directory: str):
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


def init_store():
    os.makedirs(OBJECTS_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)
    _ensure_gitignore(os.path.dirname(OBJECTS_DIR))


def _get_object_path(content_hash: str) -> str:
    return os.path.join(OBJECTS_DIR, content_hash[:2], content_hash[2:4], content_hash)


def _get_staging_path(run_id: str, coordinate: str, content_hash: str) -> str:
    safe_coord = coordinate.replace("/", "_").replace("\\", "_")
    return os.path.join(STAGING_DIR, run_id, safe_coord, f"{content_hash}.tmp")


def _serialize(result: Any) -> Tuple[bytes, str]:
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


def read_materialization_output(materialization) -> Any:
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
