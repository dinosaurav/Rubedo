# Benchmarks

Before/after performance comparison for engine changes. Not part of the
pytest suite — run explicitly:

```sh
# On the baseline commit / branch:
uv run python benchmarks/bench.py run --label before

# After your change:
uv run python benchmarks/bench.py run --label after

uv run python benchmarks/bench.py compare before after
uv run python benchmarks/bench.py list
```

Results are JSON files in `benchmarks/results/` (gitignored, so labels
survive branch switches) with the git sha and scale recorded. `compare`
warns on mismatched scales and flags >10% regressions.

## Options

- `--scale small|medium|large` (default `medium`) — sizes the synthetic
  history and file counts. `small` is a fast smoke check; use the same
  scale for both sides of a comparison.
- `--only <substring>` — run a subset, e.g. `--only lookup` or
  `--only micro` / `--only run_`.
- `--repeats N` — override per-scenario repeat counts (medians are
  reported; raise this for noisy machines).

## Scenarios

**micro_*** — drive `lane_store` + the SQLite liveness table directly
with synthetic flushed history, isolating storage hot paths from the
engine:

| scenario | measures |
| --- | --- |
| `micro_batch_lookup_cold` | `batch_lookup_by_address`, cold table cache (includes the Arrow file read — the new-process case) |
| `micro_batch_lookup_hot` | same, table cached — steady-state per-step plan cost |
| `micro_batch_lookup_sparse` | same, ~90% of queried addresses miss — the mostly-recompute plan |
| `micro_find_latest_by_address` | the per-lane single-address path, looped |
| `micro_flush_append` | `flush_step` of a small buffer onto deep history (the O(history) rewrite) |
| `micro_all_filled_rows` | the gc/du full scan across many step files |
| `micro_address_row_index` | the server's address-resolution index build |

**run_* / plan_*** — a real 4-step pipeline (expand source → two maps →
reduce over `n_files` files) through `Pipeline.run()` / `.plan()`:

| scenario | measures |
| --- | --- |
| `run_cold` | first run, everything created |
| `run_warm` | unchanged rerun, everything reused |
| `run_incremental` | one file changed out of n — surgical invalidation |
| `run_history_deep` | warm run after several full-invalidation generations — cost scaling with accumulated history rather than live lanes |

There is deliberately no `.plan()` scenario: on an expand-source
pipeline a dry-run plan can't know the source's lanes without running
user code, so everything downstream reports `pending` and the reuse
lookup never fires. The warm/incremental/deep runs cover the real
plan-phase cost (each segment plans before executing).

Working state uses `.test_bench_data` / `.test_bench_env` at the repo
root (same layout as the test suite, gitignored via `.test_*/`), wiped
per repetition and removed when the run finishes.
