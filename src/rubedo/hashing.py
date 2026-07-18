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


def compute_output_address(
    step: str,
    code_version: str,
    input_hash: str,
    pipeline: str,
    params_hash: Optional[str] = None,
    code_hash: Optional[str] = None,
) -> str:
    """Cache identity: version + data, plus run params for
    steps that consume them, the source hash for steps with code='auto',
    and the owning pipeline name — so an identically named/versioned step
    with identical input in a different pipeline never shares a liveness
    row (TODO 33). `pipeline` is required (never optional-with-default):
    every address in the system must be pipeline-scoped, with no call
    site able to silently mint an unscoped one. Optional segments are
    labeled so their absence/presence can't collide; `pipeline` is always
    present and always appended last."""
    combined = f"{step}:{code_version}:{input_hash}"
    if params_hash is not None:
        combined += f":params:{params_hash}"
    if code_hash is not None:
        combined += f":code:{code_hash}"
    combined += f":pipeline:{pipeline}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()
