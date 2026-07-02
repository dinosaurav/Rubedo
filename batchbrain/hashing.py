import hashlib
import json
from typing import Any


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def hash_json(data: Any) -> str:
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_output_address(
    step: str,
    code_version: str,
    input_hash: str,
    config_hash: str,
    params_hash: str = None,
) -> str:
    """Cache identity: code + data + static config, plus run params for
    steps that consume them (params_hash is None for steps that don't)."""
    combined = f"{step}:{code_version}:{input_hash}:{config_hash}"
    if params_hash is not None:
        combined += f":{params_hash}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()
