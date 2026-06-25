import os
import json
import shutil
from typing import Any, Union, Tuple
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
                f.write("# Ignore everything in this directory\n*\n# Except this file\n!.gitignore\n")
        except Exception:
            pass

def init_store():
    os.makedirs(OBJECTS_DIR, exist_ok=True)
    os.makedirs(STAGING_DIR, exist_ok=True)
    _ensure_gitignore(os.path.dirname(OBJECTS_DIR))

def _get_object_path(output_address: str) -> str:
    return os.path.join(OBJECTS_DIR, output_address[:2], output_address[2:4], output_address)

def _get_staging_path(run_id: str, coordinate: str, output_address: str) -> str:
    safe_coord = coordinate.replace("/", "_").replace("\\", "_")
    return os.path.join(STAGING_DIR, run_id, safe_coord, f"{output_address}.tmp")

def stage_and_commit(run_id: str, coordinate: str, output_address: str, result: Union[str, bytes, ProcessResult, Any]) -> Tuple[str, str]:
    init_store()
    
    # 1. worker writes result to staging path
    staging_path = _get_staging_path(run_id, coordinate, output_address)
    os.makedirs(os.path.dirname(staging_path), exist_ok=True)

    value = result
    if isinstance(result, ProcessResult):
        value = result.value

    # Serialize
    if isinstance(value, bytes):
        raw_data = value
    elif isinstance(value, str):
        raw_data = value.encode('utf-8')
    else:
        raw_data = json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf-8')

    with open(staging_path, 'wb') as f:
        f.write(raw_data)
        f.flush()
        os.fsync(f.fileno())

    # 3. compute output content hash
    output_content_hash = hash_bytes(raw_data)

    # 4. commit object
    final_path = _get_object_path(output_address)
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    
    # Atomic rename
    os.replace(staging_path, final_path)
    
    return final_path, output_content_hash
