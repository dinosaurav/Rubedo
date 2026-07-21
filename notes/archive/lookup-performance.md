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
| Fix 1+2+3 (cached fulfilled set) | 0.222s | 0.229s |

### 20,000 lanes × 4 steps (80,001 lanes total)

| Configuration | .plan() 1st | .plan() 2nd |
|---|---|---|
| Baseline (to_pylist + Python loop) | 1.797s | 1.998s |
| Fix 1+2 (address index) | similar | similar |
| Fix 1+2+3 (cached fulfilled set) | 1.235s | 1.181s |

### Overall improvement

| Scale | Baseline | Fix 1+2+3 | Improvement |
|---|---|---|---|
| 5K lanes | 0.354s | 0.222s | 37% |
| 20K lanes | 1.797s | 1.235s | 31% |

### SQLite breakdown (5K lanes)

| Operation | Time |
|---|---|
| Per-step `IN (5K)` query × 4 (old approach) | 128ms total (32ms each) |
| `SELECT all fulfilled` once (new approach) | 112ms |
| Python set intersection (4 steps) | 3.2ms |

### SQLite breakdown (20K lanes)

| Operation | Time |
|---|---|
| Per-step `IN (20K)` query × 4 (old approach) | 816ms total (204ms each) |
| `SELECT all fulfilled` once (new approach) | 606ms |
| Python set intersection (4 steps) | 24ms |

### Arrow lookup in isolation (5,000 lanes)

| Operation | Time |
|---|---|
| `table.take(indices)` + `to_pylist()` (5K rows) | 28ms |

## Analysis

The Arrow lookup is already fast with the index — `table.take` +
`to_pylist` is 28ms for 5K rows. The cached fulfilled set eliminates
3 of 4 SQLite queries (one `SELECT all` replaces 4 per-step `IN`
queries). The remaining `.plan()` time is the single SQLite query +
Arrow lookups + planning logic (address computation, MatRef
construction, etc.).

### Fix 3: cached fulfilled set

`_FULFILLED_CACHE` — a module-level set of all fulfilled addresses,
loaded once per run (via `clear_read_caches()` at run start) and
updated incrementally by `mark_fulfilled()` when new lanes are
committed. `batch_lookup_by_address` does a Python set intersection
(`addresses & all_fulfilled`) instead of a SQLite `IN (...)` query.

The cache is NOT cleared in `plan()` — it persists across plan calls
within the same run, so the 2nd `.plan()` is even faster (warm
cache). Tests that need isolation call `clear_read_caches()` in their
fixtures or in helpers like `backdate_materializations()`.

## Future optimization options

1. **Store `output_identity` and `content_type` in IHU** — the IHU row
   is already written at fulfill time. Adding the identity hash and
   content type would let planning skip the Arrow read entirely when it
   only needs the identity (which is most of the time — the output
   value is only needed for join/group_key field extraction and
   expand-anchor child hash reading).

2. **Bloom filter in front of IHU** — for cold-cache lanes that have
   never been seen, a bloom filter would skip the SQLite lookup
   entirely.

3. **Sort-by-address at flush + binary search** — helps when tables no
   longer fit in memory.

## What shipped

Fix 1 (pc.is_in), Fix 2 (address index), and Fix 3 (cached fulfilled
set) are all implemented. The Arrow lookup path is O(matches) instead
of O(total rows). The SQLite query is done once per run (not per step).
Overall `.plan()` is 31-37% faster.
