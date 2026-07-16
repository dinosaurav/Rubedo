"""Per-step Arrow lane store — pure computed-output history.

One Arrow IPC file per step.  Each row is one successful computation:
``(row_id, lane_key, address, input_hash, content_hash, content_type,
output_path, ts, run_id, filtered)``.  The output *bytes* still live in
the content-addressed object store (``objects/``); this module tracks
the *metadata* columnarly.

**No tombstones here.**  Liveness (reuse vs. recompute) is the
``input_hash_usages`` SQLite table's job — ``fulfilled=True`` means a
filled Arrow row exists for this address; ``fulfilled=False`` means
recompute (crash, in-flight claim, or invalidation).  The Arrow file is
pure data: every row has a non-null content_hash and output_path.
Invalidation flips ``fulfilled=False`` in SQLite; the old Arrow row
stays as history but is not reused.

During a run, rows accumulate in an in-memory buffer.  Reads combine the
on-disk history (previous runs) with the in-memory buffer (current run),
so downstream steps see the current run's outputs immediately.  At run
end, ``flush_all()`` writes the buffers to disk (read existing + concat +
write — simple for v1; the stream-append optimisation is Phase 3).

See notes/arrow-storage.md for the design and the guarantees this layer
replaces (partial unique index, lifecycle table, pairing guard, supersede
dance).
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc


_SCHEMA_FIELDS: List[Tuple[str, str, bool]] = [
    # (name, pyarrow type string, nullable)
    ("row_id", "string", False),
    ("lane_key", "string", False),
    ("address", "string", False),  # output_address: hash(step, version, input_hash[, params][, code])
    ("input_hash", "string", False),
    ("content_hash", "string", True),  # nullable for blank tombstones
    ("content_type", "string", True),
    ("output_path", "string", True),
    ("code_hash", "string", True),  # source hash at creation time, for drift detection
    ("ts", "timestamp[us]", False),
    ("run_id", "string", False),
    ("filtered", "bool", False),
]


def _make_row_id(pipeline_id: str, step_name: str, lane_key: str, ts) -> str:
    """Deterministic id for one lane_store row — the edges-FK target (see
    notes/arrow-storage.md §0).  A hash of the identifying tuple, so the
    same row at the same ts always produces the same id, and two rows
    that differ on any component never collide."""
    import hashlib
    from datetime import datetime

    if isinstance(ts, datetime):
        ts_str = ts.isoformat()
    else:
        ts_str = str(ts)
    raw = f"{pipeline_id}|{step_name}|{lane_key}|{ts_str}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _schema(pa):
    """Build the pa.Schema for a per-step Arrow file."""
    fields = []
    for name, type_str, nullable in _SCHEMA_FIELDS:
        if type_str == "string":
            t = pa.string()
        elif type_str == "timestamp[us]":
            t = pa.timestamp("us", tz="UTC")
        elif type_str == "bool":
            t = pa.bool_()
        else:
            raise ValueError(f"unknown column type {type_str}")
        fields.append(pa.field(name, t, nullable=nullable))
    return pa.schema(fields)


def _default_home() -> str:
    return os.environ.get("RUBEDO_HOME", ".rubedo")


TABLES_DIR = os.path.join(_default_home(), "tables")


def init_tables(home: Optional[str] = None):
    """Ensure the tables directory exists.

    Mirrors ``store.init_store``: an explicit home overrides TABLES_DIR
    for the rest of the process; no home just ensures the current dir
    exists (called on every commit, so the no-reset behaviour matters).
    """
    global TABLES_DIR
    if home is not None:
        TABLES_DIR = os.path.join(home, "tables")
    os.makedirs(TABLES_DIR, exist_ok=True)


def _get_step_file(pipeline_id: str, step_name: str) -> str:
    """Path to the per-step Arrow IPC file."""
    safe_pipe = pipeline_id.replace("/", "_")
    safe_step = step_name.replace("/", "_")
    return os.path.join(TABLES_DIR, safe_pipe, f"{safe_step}.arrow")


# ---------------------------------------------------------------------------
# In-memory run buffer
# ---------------------------------------------------------------------------
# Keyed by (pipeline_id, step_name) → list of row dicts.  Populated during a
# run as lanes commit; flushed to disk at run end.  Reads consult both the
# buffer and the on-disk history so downstream steps see current-run outputs
# without waiting for a flush.

_run_buffers: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}


def _buffer(pipeline_id: str, step_name: str) -> List[Dict[str, Any]]:
    return _run_buffers.setdefault((pipeline_id, step_name), [])


def clear_run_buffers():
    """Discard all in-memory buffers (after a flush or on a fresh run)."""
    _run_buffers.clear()


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


def append_filled(
    pipeline_id: str,
    step_name: str,
    lane_key: str,
    address: str,
    input_hash: str,
    content_hash: str,
    content_type: str,
    output_path: str,
    run_id: str,
    filtered: bool = False,
    code_hash: Optional[str] = None,
    ts: Optional[Any] = None,
):
    """Append a filled row (a successful computation) to the step's buffer.

    ``address`` is the comprehensive cache identity = hash(step, version,
    input_hash[, params][, code]) — what today's SQLite reuse check keys on.
    Carrying it as a column lets planning ask "is there a filled row with
    *this* address for *this* lane_key?" — a port of the current
    `Materialization.output_address IN (...) AND is_live` lookup, just on
    a different substrate."""
    from datetime import datetime, timezone

    if ts is None:
        ts = datetime.now(timezone.utc)
    _buffer(pipeline_id, step_name).append(
        {
            "row_id": _make_row_id(pipeline_id, step_name, lane_key, ts),
            "lane_key": lane_key,
            "address": address,
            "input_hash": input_hash,
            "content_hash": content_hash,
            "content_type": content_type,
            "output_path": output_path,
            "code_hash": code_hash,
            "ts": ts,
            "run_id": run_id,
            "filtered": filtered,
        }
    )


# ---------------------------------------------------------------------------
# Read path
# ---------------------------------------------------------------------------


def _read_disk_table(pipeline_id: str, step_name: str):
    """Read the on-disk Arrow file as a pa.Table, or None if absent."""
    path = _get_step_file(pipeline_id, step_name)
    if not os.path.exists(path):
        return None
    
    try:
        with pa.ipc.open_file(path) as reader:
            return reader.read_all()
    except Exception:
        # A corrupt/partially-written file (crash mid-flush) — treat as
        # absent.  The run's input_hash_usages entries will detect the
        # crash and_retry; losing the file is recoverable.
        return None


def _buffer_table(pipeline_id: str, step_name: str):
    """Convert the in-memory buffer to a pa.Table, or None if empty."""
    rows = _buffer(pipeline_id, step_name)
    if not rows:
        return None
    
    return pa.Table.from_pylist(rows, schema=_schema(pa))


def _combined_table(pipeline_id: str, step_name: str):
    """Concatenate on-disk history + in-memory buffer, or None if both empty."""
    
    disk = _read_disk_table(pipeline_id, step_name)
    buf = _buffer_table(pipeline_id, step_name)
    if disk is None and buf is None:
        return None
    if disk is None:
        return buf
    if buf is None:
        return disk
    # Ensure schema compatibility (disk file may predate a schema change —
    # v1 assumes they match; the dev-stage reset covers migrations).
    return pa.concat_tables([disk, buf], promote_options="default")


def _rows_for_lane(
    pipeline_id: str, step_name: str, lane_key: str
) -> List[Dict[str, Any]]:
    """All rows (filled and blank) for one lane_key, as dicts."""
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return []

    mask = pc.equal(table.column("lane_key"), lane_key)
    filtered = table.filter(mask)
    if filtered.num_rows == 0:
        return []
    return filtered.to_pylist()


def _latest_by_ts(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The row with the maximum ts from a list of row dicts."""
    if not rows:
        return None
    return max(rows, key=lambda r: r["ts"])


def find_latest_filled(
    pipeline_id: str,
    step_name: str,
    lane_key: str,
    input_hash: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """The latest filled row for this lane.

    If ``input_hash`` is given, only a row with a matching input_hash
    is returned.  Returns None if no filled row exists.

    NOTE: this filter doesn't account for version / params / code_hash,
    so it isn't the right reuse check for the engine's planning phase —
    use ``find_latest_filled_by_address`` for that.  Kept for tests and
    narrow uses where the caller knows the identity is stable.
    """
    rows = _rows_for_lane(pipeline_id, step_name, lane_key)
    candidates = [r for r in rows if r["content_hash"] is not None]
    if input_hash is not None:
        candidates = [r for r in candidates if r["input_hash"] == input_hash]
    return _latest_by_ts(candidates)


def find_latest_filled_by_address(
    pipeline_id: str,
    step_name: str,
    lane_key: str,
    address: str,
) -> Optional[Dict[str, Any]]:
    """Retrieve the Arrow row for (step, lane_key, address).

    Returns the latest row matching the given address, or None if no row
    with that address exists.  Every row in the Arrow file is a filled
    computation — there are no blank tombstones here.  Liveness (should
    this row be reused?) is the ``input_hash_usages.fulfilled`` column's
    job, not this function's; the caller checks ``fulfilled`` first and
    only calls this to retrieve the content on a confirmed reuse hit.

    ``address`` is the comprehensive cache identity
    (``hash(step, version, input_hash[, params][, code])`` — see
    ``hashing.compute_output_address``).
    """
    rows = _rows_for_lane(pipeline_id, step_name, lane_key)
    candidates = [r for r in rows if r["address"] == address]
    return _latest_by_ts(candidates)


def find_latest(
    pipeline_id: str, step_name: str, lane_key: str
) -> Optional[Dict[str, Any]]:
    """The latest row of any kind (filled or blank) for this lane.

    Used to distinguish invalidated (latest is blank) from never-computed
    (no row at all).  Returns None if no row exists for this lane_key.
    """
    return _latest_by_ts(_rows_for_lane(pipeline_id, step_name, lane_key))


def get_all_lane_keys(
    pipeline_id: str, step_name: str, filled_only: bool = False
) -> List[str]:
    """All lane_keys in the step's history (optionally only filled ones)."""
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return []
    if filled_only:

        table = table.filter(pc.is_valid(table.column("content_hash")))
    return table.column("lane_key").to_pylist()


def get_filled_rows(
    pipeline_id: str, step_name: str
) -> List[Dict[str, Any]]:
    """All filled rows (latest-by-lane_key) for the step.

    Used by ``run_summary.output_for`` and the server's current-outputs
    view — the "what's live for this step right now" snapshot.
    """
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return []

    table = table.filter(pc.is_valid(table.column("content_hash")))
    if table.num_rows == 0:
        return []
    lane_keys = table.column("lane_key").to_pylist()
    ts_vals = table.column("ts").to_pylist()
    latest_idx: Dict[str, int] = {}
    for i, (lk, ts) in enumerate(zip(lane_keys, ts_vals)):
        if lk not in latest_idx or ts > ts_vals[latest_idx[lk]]:
            latest_idx[lk] = i
    return [
        {col: table.column(col)[i].as_py() for col in table.column_names}
        for i in latest_idx.values()
    ]


def find_by_row_id(
    pipeline_id: str, step_name: str, row_id: str
) -> Optional[Dict[str, Any]]:
    """Look up a single row by its row_id — the edges-table join target.

    Used by trace._bfs and the downstream-invalidation blast radius while
    materialization_edges still references lane_store rows by row_id
    (see notes/arrow-storage.md §0).  Returns None if no row matches —
    a pruned/compacted-away row reads as absent, which is correct.
    """
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return None
    mask = pc.equal(table.column("row_id"), row_id)
    filtered = table.filter(mask)
    if filtered.num_rows == 0:
        return None
    return filtered.to_pylist()[0]


def batch_lookup_by_address(
    pipeline_id: str,
    step_name: str,
    addresses: set,
    session,
) -> Dict[str, Dict[str, Any]]:
    """Batch reuse lookup — the port-ready replacement for the planning
    phase's `Materialization.filter(output_address IN (...), is_live=True)`.

    Returns a dict mapping ``address -> row_dict`` for every address that
    has a ``fulfilled=True`` entry in ``input_hash_usages`` AND a filled
    Arrow row in the lane_store.  Addresses not in the dict are misses
    (recompute).  The row_dict carries all fields the planning phase needs
    to build a MatRef: ``row_id``, ``content_hash``, ``content_type``,
    ``output_path``, ``filtered``, ``code_hash``, ``ts``, and ``mat_id``
    (the SQLite Materialization.id for backward compat with join/reduce
    index lookups — transitional, goes away when materialization_index
    is deleted).

    The two-step lookup (SQLite for liveness, Arrow for content) replaces
    today's one-step SQLite query — but the SQLite lookup is a simple
    indexed query on ``input_hash_usages``, and the Arrow lookup only
    fires on confirmed reuse hits.
    """
    from .models import InputHashUsage, Materialization

    if not addresses:
        return {}

    # Step 1: find which addresses are fulfilled (liveness gate)
    fulfilled_addrs = {
        u.address for u in session.query(InputHashUsage)
        .filter(
            InputHashUsage.address.in_(addresses),
            InputHashUsage.fulfilled.is_(True),
        )
        .all()
    }
    if not fulfilled_addrs:
        return {}

    # Step 1b: for backward compat, also fetch the SQLite Materialization
    # integer ids for these addresses (the join/reduce paths still use
    # MaterializationIndexEntry keyed on mat id).  Transitional — deleted
    # when materialization_index is dropped.
    mat_rows = (
        session.query(Materialization)
        .filter(
            Materialization.output_address.in_(fulfilled_addrs),
            Materialization.is_live.is_(True),
        )
        .all()
    )
    mat_meta_by_addr = {
        m.output_address: {
            "mat_id": m.id,
            "created_at": m.created_at,
            "refreshed_at": m.refreshed_at,
        }
        for m in mat_rows
    }

    # Step 2: for each fulfilled address, retrieve the Arrow row by
    # scanning the step's Arrow file on the address column directly.
    result: Dict[str, Dict[str, Any]] = {}
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return result
    rows = table.to_pylist()
    for row in rows:
        addr = row.get("address")
        if addr in fulfilled_addrs:
            meta = mat_meta_by_addr.get(addr, {})
            row["mat_id"] = meta.get("mat_id")
            row["created_at"] = meta.get("created_at")
            row["refreshed_at"] = meta.get("refreshed_at")
            result[addr] = row
    return result


def scan_indexed_field(
    pipeline_id: str,
    step_name: str,
    field: str,
    value: Any,
) -> List[str]:
    """Lane_keys where the indexed field matches the value.

    Phase 2c will make the indexed field a real Arrow column; for now this
    is a placeholder that returns [] (materialization_index still handles
    the query).  The signature is here so callers can be written against
    the future API.
    """
    return []


# ---------------------------------------------------------------------------
# Flush path
# ---------------------------------------------------------------------------


def flush_step(pipeline_id: str, step_name: str):
    """Write the in-memory buffer for one step to disk.

    Reads the existing on-disk file, concatenates with the buffer, and
    writes the combined table back.  O(total history) per flush — simple
    for v1; the stream-append optimisation is Phase 3.
    """
    rows = _buffer(pipeline_id, step_name)
    if not rows:
        return
    init_tables()
    
    buf_table = pa.Table.from_pylist(rows, schema=_schema(pa))
    disk_table = _read_disk_table(pipeline_id, step_name)
    if disk_table is not None:
        combined = pa.concat_tables(
            [disk_table, buf_table], promote_options="default"
        )
    else:
        combined = buf_table
    path = _get_step_file(pipeline_id, step_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write to a temp file then atomically replace — crash mid-write
    # leaves the old file intact.
    tmp_path = f"{path}.tmp"
    with pa.ipc.new_file(tmp_path, combined.schema) as writer:
        writer.write_table(combined)
    os.replace(tmp_path, path)


def flush_all():
    """Flush every step's in-memory buffer to disk (call at run end)."""
    for (pipeline_id, step_name) in list(_run_buffers.keys()):
        flush_step(pipeline_id, step_name)
    clear_run_buffers()


# ---------------------------------------------------------------------------
# GC support
# ---------------------------------------------------------------------------


def compact_step(
    pipeline_id: str,
    step_name: str, keep_lane_keys: set
):
    """Rewrite a step's file, keeping only the latest row per lane_key.

    Used by retention GC to prune old generations.  ``keep_lane_keys`` is
    the set of lane_keys whose history should be preserved (the keep-set);
    lanes not in the set are dropped entirely.  For kept lanes, only the
    latest row survives — old generations are pruned.
    """
    table = _read_disk_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return
    
    lane_keys = table.column("lane_key").to_pylist()
    ts_vals = table.column("ts").to_pylist()
    latest_idx: Dict[str, int] = {}
    for i, (lk, ts) in enumerate(zip(lane_keys, ts_vals)):
        if lk in keep_lane_keys:
            if lk not in latest_idx or ts > ts_vals[latest_idx[lk]]:
                latest_idx[lk] = i
    if not latest_idx:
        # Nothing to keep — remove the file
        path = _get_step_file(pipeline_id, step_name)
        if os.path.exists(path):
            os.remove(path)
        return
    indices = sorted(latest_idx.values())
    pruned = table.take(indices)
    path = _get_step_file(pipeline_id, step_name)
    tmp_path = f"{path}.tmp"
    with pa.ipc.new_file(tmp_path, pruned.schema) as writer:
        writer.write_table(pruned)
    os.replace(tmp_path, path)