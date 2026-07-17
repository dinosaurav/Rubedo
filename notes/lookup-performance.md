# Planning Lookup Performance

## The problem

`batch_lookup_by_address` (the planning phase's reuse check) was
O(total rows per step) per call — it called `table.to_pylist()` on
the entire step history, then did a Python-level linear scan. For a
pipeline with N lanes and S steps, a single `.plan()` was
O(S × N) in Python object construction, regardless of how few
addresses were actually looked up.

## The fixes

### Fix 1: Vectorized Arrow filter (pc.is_in)

Replaced `to_pylist()` + Python loop with `pc.is_in` + `table.filter` —
Arrow's C++ compute kernel scans the address column and filters to
only matching rows before materializing to Python. Only the hit rows
get converted to dicts.

### Fix 2: Cached address→row_index

`_ADDRESS_INDEX_CACHE` — a companion to `_DISK_TABLE_CACHE` that maps
`{address: row_index}` for each step's on-disk table. Built once from
the address column (one `to_pylist` of a single string column) when the
table is first loaded or flushed, then amortized across every subsequent
lookup in the run.

`batch_lookup_by_address` now does:
1. SQLite IHU query (liveness gate — which addresses are fulfilled?)
2. Hash-probe the address index → get row indices (O(matches))
3. `table.take(indices)` → only the matching rows (O(matches))
4. `to_pylist()` on the matched rows only

The in-memory write buffers (current run, not yet flushed) are scanned
linearly after the index probe — they're small and buffer rows override
disk rows (newest wins).

### Cache lifecycle

- `flush_step` populates both caches (table + address index) after
  writing to disk
- `compact_step` invalidates both caches (the file changed)
- `clear_read_caches()` at run start clears both caches (stale data
  from a previous run/test)
- `clear_run_buffers()` (end-of-run) clears only write buffers, NOT
  read caches — the caches persist for `pipe.plan()` calls after a run

## Benchmark results

Benchmark: `bench/bench_plan_lookup.py` — N lanes × 4 steps
(1 map root + 1 dependent expand + 3 map chain). Run once to populate,
run again (reuse), then time `.plan()`.

### 5,000 lanes × 4 steps (20,001 lanes total)

| Configuration | .plan() 1st | .plan() 2nd |
|---|---|---|
| Baseline (to_pylist + Python loop) | 0.354s | 0.381s |
| Fix 1 (pc.is_in filter) | 0.330s | 0.350s |
| Fix 1+2 (address index) | 0.352s | 0.374s |

### 20,000 lanes × 4 steps (80,001 lanes total)

| Configuration | .plan() 1st | .plan() 2nd |
|---|---|---|
| Baseline (to_pylist + Python loop) | 1.797s | 1.998s |
| Fix 1 (pc.is_in filter) | 1.696s | 1.864s |
| Fix 1+2 (address index) | similar | similar |

### Arrow lookup in isolation (5,000 lanes)

| Operation | Time |
|---|---|
| `table.take(indices)` + `to_pylist()` (5K rows) | 28ms |
| SQLite `IN (20K addresses)` query | 127ms |

## Analysis

The Arrow lookup is already fast with the index — `table.take` +
`to_pylist` is 28ms for 5K rows. The remaining bottleneck is the
SQLite `input_hash_usages` query: `WHERE address IN (20K values)` takes
~127ms, and `.plan()` makes 4 of these queries (one per step).

The SQLite `IN` clause with 20K values is the dominant cost, not the
Arrow scan. Future optimization options:

1. **Batch all steps into one SQLite query** — currently each step
   queries IHU separately. A single `UNION ALL` or temp table for all
   steps' addresses would halve the SQLite round-trips.

2. **Store `output_identity` and `content_type` in IHU** — the IHU row
   is already written at fulfill time. Adding the identity hash and
   content type would let planning skip the Arrow read entirely when it
   only needs the identity (which is most of the time — the output
   value is only needed for join/group_key field extraction and
   expand-anchor child hash reading).

3. **Bloom filter in front of IHU** — for cold-cache lanes that have
   never been seen, a bloom filter would skip the SQLite lookup
   entirely.

## What shipped

Fix 1 (pc.is_in) and Fix 2 (address index) are both implemented. The
Arrow lookup path is now O(matches) instead of O(total rows). The
remaining `.plan()` time is dominated by SQLite, which is a separate
optimization.
