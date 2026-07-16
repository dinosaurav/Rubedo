"""
Hashing utilities for content addressing and cache keys.
"""
import hashlib
import json
from typing import Any, Optional


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


def canonicalize_output(v: Any) -> Any:
    """Strip ``None``-valued keys from dicts recursively (including dicts
    inside lists) so identity is stable across the Arrow write/read
    round-trip.

    Arrow's union struct null-fills missing keys: a dict ``{"a": 1}``
    stored alongside ``{"a": 1, "b": 2}`` reads back as
    ``{"a": 1, "b": None}``.  Without canonicalization,
    :func:`_identity_of` (commit time, original dict) and
    :func:`_identity_from_output` (plan time, read-back dict) would
    compute different hashes, causing unnecessary downstream
    recomputation and permanent phantom churn on expand children.

    Arrow cannot distinguish "absent" from "null", so stripping
    ``None``-valued keys loses nothing that storage does not already
    lose.
    """
    if isinstance(v, dict):
        return {
            k: canonicalize_output(val)
            for k, val in v.items()
            if val is not None
        }
    if isinstance(v, list):
        return [canonicalize_output(item) for item in v]
    return v


def compute_output_address(
    step: str,
    code_version: str,
    input_hash: str,
    params_hash: Optional[str] = None,
    code_hash: Optional[str] = None,
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
