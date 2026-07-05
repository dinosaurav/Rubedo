"""
Hashing utilities for content addressing and cache keys.
"""
import hashlib
import json
from typing import Any


def hash_bytes(data: bytes) -> str:
    """Compute the SHA-256 hash of a byte string."""
    return hashlib.sha256(data).hexdigest()


def hash_text(text: str) -> str:
    """Compute the SHA-256 hash of a UTF-8 string."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_file(path: str) -> str:
    """Compute the SHA-256 hash of a file's contents."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def hash_json(data: Any) -> str:
    """Compute the SHA-256 hash of a canonicalized JSON representation."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_output_address(
    step: str,
    code_version: str,
    input_hash: str,
    params_hash: str = None,
    code_hash: str = None,
) -> str:
    """Cache identity: version + data, plus run params for
    steps that consume them and the source hash for steps with code='auto'.
    Optional segments are labeled so their absence/presence can't collide."""
    combined = f"{step}:{code_version}:{input_hash}"
    if params_hash is not None:
        combined += f":params:{params_hash}"
    if code_hash is not None:
        combined += f":code:{code_hash}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()
