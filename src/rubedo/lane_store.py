"""Per-step Arrow lane store — the append-only history of what each step's
lanes produced across all runs.

Replaces the `materializations` SQLite table (one row per lane) with one
Arrow IPC file per step (one row per lane *attempt*, stacked across time).
The output *bytes* still live in the content-addressed object store
(``objects/``); this module tracks the *metadata* — lane_key, input_hash,
content_hash, content_type, output_path, ts, run_id, filtered — columnarly.

Blank rows (content_hash = None, output_path = None) are invalidation
tombstones written by ``invalidate()``.  "What is live" is a query: the
latest row by ``ts`` for a (step, lane_key); filled = live, blank =
invalidated (pending recompute), absent = never computed or crashed.

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
    ("input_hash", "string", False),
    ("content_hash", "string", True),  # nullable for blank tombstones
    ("content_type", "string", True),
    ("output_path", "string", True),
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
            t = pa.timestamp("us")
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
    input_hash: str,
    content_hash: str,
    content_type: str,
    output_path: str,
    run_id: str,
    filtered: bool = False,
    ts: Optional[Any] = None,
):
    """Append a filled row (a successful computation) to the step's buffer."""
    from datetime import datetime, timezone

    if ts is None:
        ts = datetime.now(timezone.utc)
    _buffer(pipeline_id, step_name).append(
        {
            "row_id": _make_row_id(pipeline_id, step_name, lane_key, ts),
            "lane_key": lane_key,
            "input_hash": input_hash,
            "content_hash": content_hash,
            "content_type": content_type,
            "output_path": output_path,
            "ts": ts,
            "run_id": run_id,
            "filtered": filtered,
        }
    )


def append_blank(
    pipeline_id: str,
    step_name: str,
    lane_key: str,
    run_id: str,
    input_hash: str = "",
    ts: Optional[Any] = None,
):
    """Append a blank tombstone row (an invalidation marker).

    content_hash / content_type / output_path are NULL — the latest row
    being blank means "pending, recompute next run."  input_hash is stored
    as the empty string (Arrow doesn't allow null in the non-nullable
    column) to keep the schema simple; readers treat blank as "regardless
    of input_hash, this lane is invalidated."
    """
    from datetime import datetime, timezone

    if ts is None:
        ts = datetime.now(timezone.utc)
    _buffer(pipeline_id, step_name).append(
        {
            "row_id": _make_row_id(pipeline_id, step_name, lane_key, ts),
            "lane_key": lane_key,
            "input_hash": input_hash,
            "content_hash": None,
            "content_type": None,
            "output_path": None,
            "ts": ts,
            "run_id": run_id,
            "filtered": False,
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
    """The latest filled (non-blank) row for this lane — the reuse check.

    If ``input_hash`` is given, only a latest row with a matching
    input_hash counts as a reuse hit; a mismatch means the lane was
    recomputed with a different input since, so the old output is stale.

    Returns None if no filled row exists — meaning the lane must be
    (re)computed.  A blank tombstone is NOT a filled row; if the latest
    row is blank, this returns None, signalling "invalidated, recompute."
    """
    latest = find_latest(pipeline_id, step_name, lane_key)
    if latest is None or latest["content_hash"] is None:
        return None  # blank tombstone wins — lane is invalidated
    if input_hash is not None and latest["input_hash"] != input_hash:
        return None  # latest filled was for a different input
    return latest


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