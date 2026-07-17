"""Per-step Arrow lane store — pure computed-output history.

One Arrow IPC file per step.  Each row is one successful computation:
``(row_id, lane_key, address, input_hash, code_version, output,
output_identity, content_type, code_hash, ts, run_id, filtered)``.  The
``output`` column holds the value itself in a native Arrow type (struct
for dicts, int64 for ints, string) when all lanes in a step are inline;
falls back to ``string`` (JSON-serialized inline + ``"objects:<hash>"``
ref strings) when any value spills to the object store.  ``output_identity``
is the content identity hash (for downstream ``input_hash`` computation),
computed once at commit time from the original output value — plan time
reads it from the column instead of recomputing from the Arrow-read-back
value.

**No tombstones here.**  Liveness (reuse vs. recompute) is the
``input_hash_usages`` SQLite table's job — ``fulfilled=True`` means a
filled Arrow row exists for this address; ``fulfilled=False`` means
recompute (crash, in-flight claim, or invalidation).  The Arrow file is
pure data.  Invalidation flips ``fulfilled=False`` in SQLite; the old
Arrow row stays as history but is not reused.

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
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc


_SCHEMA_FIELDS: List[Tuple[str, str, bool]] = [
    # (name, pyarrow type string, nullable)
    ("row_id", "string", False),
    ("lane_key", "string", False),
    ("address", "string", False),  # output_address: hash(step, version, input_hash[, params][, code])
    ("input_hash", "string", False),
    ("code_version", "string", True),  # step version string, for selection queries
    ("output", "dynamic", True),  # native Arrow type per-step, or string for mixed/spilled
    ("output_identity", "string", True),  # content identity hash, stored at commit time
    ("content_type", "string", True),  # "json" for inline; "bytes"/"text"/"arrow-ipc:..." for spilled
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


def _schema(pa, output_type=None):
    """Build the pa.Schema for a per-step Arrow file.

    ``output_type`` overrides the ``output`` column's type — this is
    dynamic per step file: a step returning dicts gets a ``struct<...>``
    output column, a step returning ints gets ``int64``, etc.  Defaults
    to ``string`` for the mixed/spilled case (inline JSON + ref strings)."""
    fields = []
    for name, type_str, nullable in _SCHEMA_FIELDS:
        if name == "output":
            t = output_type or pa.string()
            fields.append(pa.field(name, t, nullable=True))
            continue
        if type_str == "string":
            t = pa.string()
        elif type_str == "timestamp[us]":
            t = pa.timestamp("us", tz="UTC")
        elif type_str == "bool":
            t = pa.bool_()
        elif type_str == "map<string, list<string>>":
            t = pa.map_(pa.string(), pa.list_(pa.string()))
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
# run as lanes commit; flushed to disk per segment.  Reads consult both the
# buffer and the on-disk history so downstream steps see current-run outputs
# without waiting for a flush.

_run_buffers: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
_arrow_batch_buffers: Dict[Tuple[str, str], List["pa.Table"]] = {}

# Cache of on-disk Arrow tables, keyed by (pipeline_id, step_name).
# Invalidated by flush_step (the file changed).  Bounds the number of
# full-file reads: a parent step's table is read once after flush, then
# served from cache for every subsequent lookup during the run.

_DISK_TABLE_CACHE: OrderedDict = OrderedDict()
_DISK_TABLE_CACHE_MAX = 16


def _buffer(pipeline_id: str, step_name: str) -> List[Dict[str, Any]]:
    return _run_buffers.setdefault((pipeline_id, step_name), [])


def _arrow_batch_buffer(pipeline_id: str, step_name: str) -> List["pa.Table"]:
    return _arrow_batch_buffers.setdefault((pipeline_id, step_name), [])


def clear_run_buffers():
    """Discard all in-memory buffers (after a flush or on a fresh run)."""
    _run_buffers.clear()
    _arrow_batch_buffers.clear()
    _DISK_TABLE_CACHE.clear()


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


def append_arrow_batch(
    pipeline_id: str,
    step_name: str,
    table: "pa.Table",
):
    """Add a pre-built Arrow table to the arrow batch buffer.

    Used by table-return expand: the source table's struct column is
    written directly to the lane store without going through Python dict
    intermediaries.  The table must have the full lane store schema
    (row_id, lane_key, address, input_hash, output, content_type,
    code_hash, code_version, ts, run_id, filtered).

    At flush time, the arrow batch buffer is concatenated with the dict
    buffer and the on-disk history."""
    _arrow_batch_buffer(pipeline_id, step_name).append(table)


def arrow_batch_row_by_address(pipeline_id: str, step_name: str, address: str) -> Optional[Dict[str, Any]]:
    """Look up a single row from the arrow batch buffer by address.
    Returns the row as a dict (with 'output' as a native Python value
    from the struct column), or None if not found."""
    batches = _arrow_batch_buffer(pipeline_id, step_name)
    for batch in batches:
        addr_col = batch.column("address")
        if isinstance(addr_col, pa.ChunkedArray):
            addr_col = pa.concat_arrays(addr_col.chunks)
        mask = pc.equal(addr_col, address)
        filtered = batch.filter(mask)
        if filtered.num_rows > 0:
            row = filtered.to_pylist()[0]
            row["pipeline_id"] = pipeline_id
            row["step_name"] = step_name
            return row
    return None


def append_filled(
    pipeline_id: str,
    step_name: str,
    lane_key: str,
    address: str,
    input_hash: str,
    output: str,
    content_type: str,
    run_id: str,
    filtered: bool = False,
    code_hash: Optional[str] = None,
    code_version: Optional[str] = None,
    output_identity: Optional[str] = None,
    ts: Optional[Any] = None,
):
    """Append a filled row (a successful computation) to the step's buffer.

    ``address`` is the comprehensive cache identity = hash(step, version,
    input_hash[, params][, code]).

    ``output`` is either an inline JSON string (small values — no object
    store I/O) or a ref string ``"objects:<hash>"`` pointing to the
    serialized value bytes in the content-addressed object store (large
    values, bytes, DataFrames).  ``content_type`` tells the reader how to
    deserialize: ``"json"`` for inline, ``"bytes"``/``"text"``
    ``"arrow-ipc:<kind>"`` for spilled.

    ``output_identity`` is the content identity hash (for downstream
    ``input_hash`` computation), computed once at commit time from the
    original output value — stored directly so plan time reads it from
    the column instead of recomputing from the Arrow-read-back value."""
    from datetime import datetime, timezone

    if ts is None:
        ts = datetime.now(timezone.utc)
    _buffer(pipeline_id, step_name).append(
        {
            "row_id": _make_row_id(pipeline_id, step_name, lane_key, ts),
            "lane_key": lane_key,
            "address": address,
            "input_hash": input_hash,
            "code_version": code_version,
            "output": output,
            "output_identity": output_identity,
            "content_type": content_type,
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
    """Read the on-disk Arrow file as a pa.Table, or None if absent.

    Results are cached in _DISK_TABLE_CACHE (LRU, bounded) so repeated
    lookups during a run don't re-read the same file.  The cache is
    invalidated by flush_step when the file changes."""
    key = (pipeline_id, step_name)
    if key in _DISK_TABLE_CACHE:
        _DISK_TABLE_CACHE.move_to_end(key)
        return _DISK_TABLE_CACHE[key]

    path = _get_step_file(pipeline_id, step_name)
    if not os.path.exists(path):
        return None

    try:
        with pa.ipc.open_file(path) as reader:
            table = reader.read_all()
    except Exception:
        # A corrupt/partially-written file (crash mid-flush) — treat as
        # absent.  The run's input_hash_usages entries will detect the
        # crash and_retry; losing the file is recoverable.
        return None

    _DISK_TABLE_CACHE[key] = table
    if len(_DISK_TABLE_CACHE) > _DISK_TABLE_CACHE_MAX:
        _DISK_TABLE_CACHE.popitem(last=False)
    return table


def _infer_output_type(rows: List[Dict[str, Any]]):
    """Determine the Arrow type for the ``output`` column from the buffer's
    rows.  Returns a pa.DataType, or ``None`` to use ``string`` (the
    fallback for mixed inline/spill or all-null).

    Collects all non-ref output values and passes them to ``pa.array()``
    in one shot.  Pyarrow infers the union of dict fields (nullable for
    missing) and handles schema evolution: a step where some lanes return
    ``{"a": 1, "b": 2}`` and others return ``{"a": 1, "b": 2, "c": 3}``
    gets ``struct<a, b, c>`` with ``c = null`` for the first row — no
    fallback to string.  Only falls back when:
    - any value is a ref string (spilled) — can't mix native + string
    - field types genuinely conflict (e.g. field "a" is int in one row,
      string in another)
    - the value type can't be represented in Arrow at all
    """
    values: List[Any] = []
    has_refs = False

    for row in rows:
        output = row.get("output")
        if output is None:
            values.append(None)
            continue
        if isinstance(output, str) and output.startswith("objects:"):
            has_refs = True
            continue
        values.append(output)

    if has_refs:
        return pa.string()  # mixed inline + spill → string

    if not any(v is not None for v in values):
        return None  # all null → caller uses string

    try:
        arr = pa.array(values)
        return arr.type
    except (pa.lib.ArrowInvalid, pa.lib.ArrowTypeError, TypeError):
        return pa.string()  # conflicting types or unrepresentable → string


def _stringify_outputs(rows: List[Dict[str, Any]]) -> None:
    """Convert all ``output`` values in the buffer rows to strings (in
    place).  Inline values are JSON-serialized; ref strings pass through;
    None stays None.  Used when the output column must be ``string``
    (mixed inline/spill or unrepresentable types)."""
    import json

    for row in rows:
        output = row.get("output")
        if output is None:
            continue
        if isinstance(output, str):
            continue  # already a string (ref or plain string return)
        row["output"] = json.dumps(
            output, sort_keys=True, separators=(",", ":")
        )


def _buffer_table(pipeline_id: str, step_name: str):
    """Convert the in-memory buffer to a pa.Table, or None if empty.

    The ``output`` column's Arrow type is inferred from the buffer's
    values: a step returning dicts gets a ``struct<...>`` column, a step
    returning ints gets ``int64``, etc.  If the buffer has mixed types or
    spilled ref strings, the column falls back to ``string`` (inline
    values JSON-serialized, ref strings as-is)."""
    rows = _buffer(pipeline_id, step_name)
    if not rows:
        return None

    output_type = _infer_output_type(rows)
    if output_type is None or output_type == pa.string():
        _stringify_outputs(rows)
        output_type = pa.string()

    schema = _schema(pa, output_type)
    return pa.Table.from_pylist(rows, schema=schema)


def _arrow_batch_table(pipeline_id: str, step_name: str):
    """Concat all arrow batch tables for a step, or None if none."""
    batches = _arrow_batch_buffer(pipeline_id, step_name)
    if not batches:
        return None
    if len(batches) == 1:
        return batches[0]
    return pa.concat_tables(batches, promote_options="default")


def _combined_table(pipeline_id: str, step_name: str):
    """Concatenate on-disk history + arrow batch buffer + dict buffer,
    or None if all empty.  If the output column types are compatible
    (e.g. structs with different fields — schema evolution),
    ``pa.concat_tables`` with ``promote_options='default'`` unions the
    struct fields and fills nulls.  If the types are genuinely
    incompatible (struct vs string, int vs string), all are converted
    to ``string`` before concatenation."""
    import json

    disk = _read_disk_table(pipeline_id, step_name)
    buf = _buffer_table(pipeline_id, step_name)
    arrow = _arrow_batch_table(pipeline_id, step_name)

    tables = [t for t in (disk, arrow, buf) if t is not None]
    if not tables:
        return None
    if len(tables) == 1:
        return tables[0]

    try:
        return pa.concat_tables(tables, promote_options="default")
    except (pa.lib.ArrowInvalid, pa.lib.ArrowTypeError):
        pass

    # Types are genuinely incompatible — convert all to string
    def _to_string_table(table):
        col = table.column("output")
        if col.type == pa.string():
            return table
        pyvals = col.to_pylist()
        str_vals: List[Optional[str]] = []
        for v in pyvals:
            if v is None:
                str_vals.append(None)
            elif isinstance(v, str):
                str_vals.append(v)
            else:
                str_vals.append(
                    json.dumps(v, sort_keys=True, separators=(",", ":"))
                )
        new_col = pa.array(str_vals, type=pa.string())
        return table.set_column(
            table.schema.get_field_index("output"), "output", new_col
        )

    tables = [_to_string_table(t) for t in tables]
    return pa.concat_tables(tables, promote_options="default")


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
    candidates = [r for r in rows if r["output"] is not None]
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

        table = table.filter(pc.is_valid(table.column("output")))
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

    table = table.filter(pc.is_valid(table.column("output")))
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


def output_column_as_table(
    pipeline_id: str, step_name: str, lane_keys: List[str]
) -> Optional["pa.Table"]:
    """Extract the ``output`` struct column for a set of lanes as a
    ``pa.Table`` — the struct fields become columns.  Used by
    ``arrow_reduce`` steps that want the parent's data as a table instead
    of a dict-of-lanes.

    Returns None if the step has no Arrow file or the output column is
    not a struct (string fallback — the caller should use the dict path).
    """
    table = _combined_table(pipeline_id, step_name)
    if table is None or table.num_rows == 0:
        return None

    output_type = table.schema.field("output").type
    if pa.types.is_struct(output_type):
        # Filter to the requested lanes
        lane_set = set(lane_keys)
        mask = pc.is_in(table.column("lane_key"), value_set=pa.array(list(lane_set)))
        filtered = table.filter(mask)
        if filtered.num_rows == 0:
            return None
        # Flatten the struct column into a table
        struct_col = filtered.column("output")
        field_names = [f.name for f in output_type]
        arrays = []
        for name in field_names:
            arrays.append(pc.struct_field(struct_col, name))
        return pa.table(dict(zip(field_names, arrays)))

    return None


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
    to build a MatRef: ``row_id``, ``output_identity``, ``content_type``,
    ``output``, ``filtered``, ``code_hash``, ``ts``.
    """
    from .models import InputHashUsage

    if not addresses:
        return {}

    # Step 1: find which addresses are fulfilled (liveness gate)
    fulfilled_addrs = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(
            InputHashUsage.address.in_(addresses),
            InputHashUsage.fulfilled.is_(True),
        )
        .all()
    }
    if not fulfilled_addrs:
        return {}

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
            result[addr] = row
    return result


def address_row_index() -> Dict[str, Dict[str, Any]]:
    """A ``{address: row_dict}`` index of the latest row per address
    across all Arrow files.  Used by server endpoints that need to
    resolve a single address to its content metadata without querying
    the ``materializations`` SQLite table."""
    index: Dict[str, Dict[str, Any]] = {}
    for row in all_filled_rows():
        addr = row.get("address")
        if not addr:
            continue
        existing = index.get(addr)
        if existing is None or (row.get("ts") and existing.get("ts") and row["ts"] > existing["ts"]):
            index[addr] = row
    return index


def all_filled_rows() -> List[Dict[str, Any]]:
    """Every filled row across all step Arrow files.

    Returns a list of row dicts (one per lane per attempt), each carrying
    ``address``, ``content_hash``, ``pipeline_id``, ``step_name``,
    ``lane_key``, ``run_id``, ``filtered``, ``ts``, etc.  Used by gc and
    du to refcount object bytes and compute storage reports without
    querying the ``materializations`` SQLite table.

    ``pipeline_id`` and ``step_name`` are derived from the file path
    (``tables/<pipeline>/<step>.arrow``), not stored in the row — the
    caller knows which step's file it's reading.
    """
    result: List[Dict[str, Any]] = []
    if not os.path.isdir(TABLES_DIR):
        return result
    for entry in os.listdir(TABLES_DIR):
        pipe_dir = os.path.join(TABLES_DIR, entry)
        if not os.path.isdir(pipe_dir):
            continue
        for fname in os.listdir(pipe_dir):
            if not fname.endswith(".arrow"):
                continue
            step_name = fname[:-len(".arrow")]
            table = _combined_table(entry, step_name)
            if table is None or table.num_rows == 0:
                continue
            for row in table.to_pylist():
                row["pipeline_id"] = entry
                row["step_name"] = step_name
                result.append(row)
    return result


# ---------------------------------------------------------------------------
# Flush path
# ---------------------------------------------------------------------------


def flush_step(pipeline_id: str, step_name: str):
    """Write the in-memory buffers (dict + arrow batch) for one step to disk.

    Reads the existing on-disk file, concatenates with both buffers, and
    writes the combined table back.  After flushing, the combined table
    stays in the disk-table cache so downstream lookups get a cache hit
    — no re-read from disk.  The write buffers are cleared (the data is
    now on disk + in cache).  O(total history) per flush — simple for
    v1; the stream-append optimisation is Phase 3.
    """
    rows = _buffer(pipeline_id, step_name)
    arrow_batches = _arrow_batch_buffer(pipeline_id, step_name)
    if not rows and not arrow_batches:
        return
    init_tables()

    combined = _combined_table(pipeline_id, step_name)
    if combined is None:
        return
    path = _get_step_file(pipeline_id, step_name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write to a temp file then atomically replace — crash mid-write
    # leaves the old file intact.
    tmp_path = f"{path}.tmp"
    with pa.ipc.new_file(tmp_path, combined.schema) as writer:
        writer.write_table(combined)
    os.replace(tmp_path, path)

    # Keep the flushed table in the disk-table cache so downstream
    # lookups get a cache hit — no re-read from disk.  Clear the write
    # buffers (the data is now on disk + in cache).
    key = (pipeline_id, step_name)
    _DISK_TABLE_CACHE[key] = combined
    _DISK_TABLE_CACHE.move_to_end(key)
    if len(_DISK_TABLE_CACHE) > _DISK_TABLE_CACHE_MAX:
        _DISK_TABLE_CACHE.popitem(last=False)
    rows.clear()
    arrow_batches.clear()


def flush_all():
    """Flush every step's in-memory buffers to disk (call at run end)."""
    all_keys = set(_run_buffers.keys()) | set(_arrow_batch_buffers.keys())
    for (pipeline_id, step_name) in all_keys:
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
        _DISK_TABLE_CACHE.pop((pipeline_id, step_name), None)
        return
    indices = sorted(latest_idx.values())
    pruned = table.take(indices)
    path = _get_step_file(pipeline_id, step_name)
    tmp_path = f"{path}.tmp"
    with pa.ipc.new_file(tmp_path, pruned.schema) as writer:
        writer.write_table(pruned)
    os.replace(tmp_path, path)
    _DISK_TABLE_CACHE.pop((pipeline_id, step_name), None)