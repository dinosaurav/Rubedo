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

**plan_deep_*** — pure `.plan()` benchmarks on the one shape where a
dry run resolves every lane: map root (params-addressed `@root`) →
dependent expand (`n_lanes` fan-out) → map chain (`chain_depth`). An
expand *source* must re-run to yield its lanes, so the folder pipelines
above report `pending` downstream and never hit the reuse lookup; here
the expand's children reuse via the parent-addressed anchor, making
`.plan()` a real end-to-end reuse-lookup measurement:

| scenario | measures |
| --- | --- |
| `plan_deep_coldcache` | new-process dry run: liveness gate + Arrow retrieval + file reads |
| `plan_deep_hotcache` | read caches primed: planning logic + liveness gate only |

Both report counters, including `sqlite_stmts` (every SQL statement,
engine-wide) — the direct signal for liveness-gate strategy changes
(per-step `IN` queries vs one cached fulfilled-set load).

## Shape comparisons and work counters

**shape_*** scenarios compare pipeline *shapes* rather than code
versions: two pipelines differing in exactly one knob, same workload,
same phase. Timing alone can't prove a shape "isn't doing extra work" —
a fast implementation could still write rows it shouldn't — so these
scenarios also report **work counters**: Arrow rows written (total and
per step), flushes with data, disk-table cache misses, batch/single
reuse lookups, SQL statements executed (`sqlite_stmts`), and
scenario-specific counts like `util_fn_calls`. Counters
land in the JSON and in the `run` output (`work: ...` line); `compare`
prints any counter that changed between two results — the "it got
faster but now does different work" signal.

The built-in quartet covers the skip_cache question:

| scenario | expectation |
| --- | --- |
| `shape_util_cached_cold` / `shape_util_skipcache_cold` | first run; skip_cache writes **zero** Arrow rows for the util step |
| `shape_util_cached_warm` / `shape_util_skipcache_warm` | unchanged rerun; skip_cache has **zero** `util_fn_calls` and no util-step lookups |

Note the shape being tested: `skip_cache` is rejected by spec
validation on `expand` (its lanes are the cache anchors) and on
`reduce` — so "a big expand I never want to cache" is expressed as the
expand's downstream util map being `skip_cache`.

### Writing your own shape scenario

Copy the `make_util_pipeline` + `_bench_util_shape` pattern:

```python
def make_my_pipeline(n, the_knob):
    @step
    def gen():
        for i in range(n):
            yield {"i": i}
    # ... steps differing only in the_knob ...
    return pipeline(name="bench_my", steps=[...], home=ENV_DIR)

@scenario("shape_my_variant", repeats=3)
def bench_my_variant(p, repeats):
    times = []
    for _ in range(repeats):
        fresh_env()                      # or warm-store setup + drop_table_cache()
        pipe = make_my_pipeline(p["n_files"], the_knob=True)
        t, counters = timed_counted(lambda: pipe.run(workers=1))
        times.append(t)
    return times, counters               # counters from the last rep
```

That's the whole contract: return `times` or `(times, counters)`.
Register one scenario per variant so `compare` aligns them by name, and
add closure counters (a list the step fn appends to) for anything the
harness can't see from `lane_store` traffic.

Counters make a shape's work *visible*; they are not assertions. When a
shape guarantee is load-bearing ("skip_cache never materializes"),
pin it in pytest — `tests/test_skip_cache.py` already does this with
the same closure-counting trick.

Working state uses `.test_bench_data` / `.test_bench_env` at the repo
root (same layout as the test suite, gitignored via `.test_*/`), wiped
per repetition and removed when the run finishes.
