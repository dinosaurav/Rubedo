"""Rubedo benchmark suite — before/after comparison for engine changes.

Two layers of scenarios:

- **micro_***: drive ``lane_store`` + the SQLite liveness table directly
  with synthetic history, isolating the storage-layer hot paths
  (``batch_lookup_by_address``, ``find_latest_filled_by_address``,
  ``flush_step``, ``all_filled_rows``) from engine overhead.
- **run_***: drive a real 4-step pipeline (expand source → two maps →
  reduce) end-to-end through ``Pipeline.run()`` — cold, warm,
  incremental, and deep-history variants.  (No ``.plan()`` scenario: on
  an expand-source pipeline a dry run can't know the source's lanes, so
  the reuse lookup never fires — the warm runs cover plan-phase cost.)

Usage:

    uv run python benchmarks/bench.py run --label before
    # ... make changes ...
    uv run python benchmarks/bench.py run --label after
    uv run python benchmarks/bench.py compare before after

Results land in ``benchmarks/results/<label>.json`` (gitignored) with the
git sha recorded, so labels survive branch switches.  ``--scale
small|medium|large`` sizes the synthetic data; ``--only <substring>``
filters scenarios; ``--repeats N`` overrides per-scenario repeat counts.

Working state lives in ``.test_bench_data`` (scanned folder) and
``.test_bench_env`` (rubedo home) at the repo root — same convention as
the test suite (never nest the store inside the scanned folder), wiped
per repetition and removed at exit.
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
import warnings
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Tuple

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
DATA_DIR = os.path.join(REPO_ROOT, ".test_bench_data")
ENV_DIR = os.path.join(REPO_ROOT, ".test_bench_env")

# Redefining structurally-identical steps across repetitions can trip the
# code-drift UserWarning (by design in the engine; noise here).
warnings.filterwarnings("ignore", category=UserWarning, module=r"rubedo\..*")

SCALES: Dict[str, Dict[str, int]] = {
    #                     macro: files in the scanned folder
    #                     |        micro: rows of step history on disk
    #                     |        |         micro: addresses per lookup batch
    #                     |        |         |      micro: buffered rows per flush
    #                     |        |         |      |      micro: step files for gc paths
    #                     |        |         |      |      |   macro: prior runs of history
    "small":  dict(n_files=20,  hist_rows=2_000,   n_addrs=200,   new_rows=200,   n_steps=5,  deep_runs=3),
    "medium": dict(n_files=100, hist_rows=20_000,  n_addrs=1_000, new_rows=1_000, n_steps=10, deep_runs=5),
    "large":  dict(n_files=400, hist_rows=100_000, n_addrs=5_000, new_rows=2_000, n_steps=10, deep_runs=8),
}

MICRO_PIPELINE = "bench_micro"

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------

# name -> (fn(params, repeats) -> times | (times, counters), default_repeats)
SCENARIOS: List[Tuple[str, Callable[..., Any], int]] = []


def scenario(name: str, repeats: int):
    def deco(fn):
        SCENARIOS.append((name, fn, repeats))
        return fn

    return deco


def timed(fn: Callable[[], Any]) -> float:
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


class WorkCounters:
    """Count the *work* the engine does while a block runs — Arrow rows
    written (per step), flushes, disk-table cache misses, reuse lookups.

    Timing tells you a change is faster; counters tell you it did less
    (or, for shape comparisons like skip_cache, that a shape does zero
    extra work).  Implemented by wrapping ``lane_store`` module
    attributes — every engine call site resolves them at call time, so
    the wrappers see all traffic.  Use as a context manager; counts are
    in ``.counts`` afterwards."""

    def __init__(self):
        self.counts: Dict[str, int] = {}

    def _bump(self, key: str, by: int = 1):
        self.counts[key] = self.counts.get(key, 0) + by

    def __enter__(self):
        from rubedo import lane_store

        self._ls = lane_store
        self._orig = {
            name: getattr(lane_store, name)
            for name in (
                "append_filled",
                "flush_step",
                "_read_disk_table",
                "batch_lookup_by_address",
                "find_latest_filled_by_address",
            )
        }
        orig = self._orig

        def append_filled(pipeline_id, step_name, *a, **kw):
            self._bump("rows_written_total")
            self._bump(f"rows_written[{step_name}]")
            return orig["append_filled"](pipeline_id, step_name, *a, **kw)

        def flush_step(pipeline_id, step_name, *a, **kw):
            key = (pipeline_id, step_name)
            if lane_store._run_buffers.get(key) or lane_store._arrow_batch_buffers.get(key):
                self._bump("flushes")  # no-op flushes on empty buffers don't count
            return orig["flush_step"](pipeline_id, step_name, *a, **kw)

        def _read_disk_table(pipeline_id, step_name, *a, **kw):
            if (pipeline_id, step_name) not in lane_store._DISK_TABLE_CACHE:
                self._bump("disk_table_reads")
            return orig["_read_disk_table"](pipeline_id, step_name, *a, **kw)

        def batch_lookup_by_address(pipeline_id, step_name, addresses, *a, **kw):
            self._bump("batch_lookups")
            self._bump("batch_lookup_addrs", len(addresses))
            return orig["batch_lookup_by_address"](
                pipeline_id, step_name, addresses, *a, **kw
            )

        def find_latest_filled_by_address(*a, **kw):
            self._bump("single_lookups")
            return orig["find_latest_filled_by_address"](*a, **kw)

        for name, fn in {
            "append_filled": append_filled,
            "flush_step": flush_step,
            "_read_disk_table": _read_disk_table,
            "batch_lookup_by_address": batch_lookup_by_address,
            "find_latest_filled_by_address": find_latest_filled_by_address,
        }.items():
            setattr(lane_store, name, fn)
        return self

    def __exit__(self, *exc):
        for name, fn in self._orig.items():
            setattr(self._ls, name, fn)
        return False


def timed_counted(fn: Callable[[], Any]) -> Tuple[float, Dict[str, int]]:
    """Time fn and count the engine work it did."""
    with WorkCounters() as wc:
        t0 = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - t0
    return elapsed, wc.counts


def fresh_env():
    """Wipe and re-init the data folder, rubedo home, and all module-level
    engine state (DB engine, object store paths, lane_store buffers/cache)."""
    from rubedo import lane_store
    from rubedo.runner import _init_home

    for d in (DATA_DIR, ENV_DIR):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
    lane_store.clear_run_buffers()
    lane_store.clear_read_caches()
    _init_home(ENV_DIR)


def drop_table_cache():
    """Simulate a new process: drop lane_store's in-memory buffers AND the
    read caches (disk-table + address-index — clear_run_buffers alone no
    longer drops those).  On-disk files and SQLite survive."""
    from rubedo import lane_store

    lane_store.clear_run_buffers()
    lane_store.clear_read_caches()


def make_files(n: int, gen: int):
    """Write n files into the scanned folder.  ``gen`` is baked into the
    content, so a new generation invalidates every lane."""
    for i in range(n):
        path = os.path.join(DATA_DIR, f"{i:05d}.txt")
        with open(path, "w") as f:
            f.write(f"file {i} generation {gen}\n" + ("lorem ipsum dolor sit amet " * 8))


def make_pipeline():
    """The macro benchmark pipeline: expand source → map → map → reduce."""
    from rubedo import pipeline, step

    @step
    def scan():
        for fname in sorted(os.listdir(DATA_DIR)):
            path = os.path.join(DATA_DIR, fname)
            if os.path.isfile(path):
                yield {"path": fname, "text": open(path).read()}

    @step
    def extract(scan: dict):
        text = scan["text"]
        return {"path": scan["path"], "words": len(text.split()), "chars": len(text)}

    @step
    def enrich(extract: dict):
        return {"path": extract["path"], "score": extract["words"] * 2 + extract["chars"]}

    @step(depends_on=["enrich"], shape="reduce")
    def summarize(enrich: dict):
        return {"total": sum(v["score"] for v in enrich.values()), "lanes": len(enrich)}

    return pipeline(name="bench", steps=[scan, extract, enrich, summarize], home=ENV_DIR)


def seed_step_history(step_name: str, n_rows: int, fulfilled: bool = True) -> List[str]:
    """Write n_rows of synthetic filled history for one step (flushed to
    disk) and seed matching input_hash_usages rows.  Returns the addresses."""
    from rubedo import lane_store
    from rubedo.db import get_session
    from rubedo.models import InputHashUsage

    addresses = []
    for i in range(n_rows):
        addr = f"addr-{step_name}-{i:08d}"
        addresses.append(addr)
        lane_store.append_filled(
            MICRO_PIPELINE,
            step_name,
            lane_key=f"row-{i:08d}",
            address=addr,
            input_hash=f"ih-{i:08d}",
            output={"path": f"{i:05d}.txt", "text": "lorem ipsum dolor sit amet " * 4, "n": i},
            content_type="json",
            run_id="bench-seed",
            code_hash="codehash",
            code_version="1.0",
            output_identity=f"oid-{step_name}-{i:08d}",
        )
    lane_store.flush_step(MICRO_PIPELINE, step_name)

    session = get_session()
    try:
        session.add_all(
            InputHashUsage(address=a, last_run_id="bench-seed", fulfilled=fulfilled)
            for a in addresses
        )
        session.commit()
    finally:
        session.close()
    return addresses


def sample_evenly(items: List[str], k: int) -> List[str]:
    if k >= len(items):
        return list(items)
    stride = len(items) / k
    return [items[int(i * stride)] for i in range(k)]


# ---------------------------------------------------------------------------
# Micro scenarios — lane_store + SQLite liveness, synthetic history
# ---------------------------------------------------------------------------


@scenario("micro_batch_lookup_cold", repeats=5)
def bench_batch_lookup_cold(p, repeats):
    """batch_lookup_by_address with a cold table cache (new-process case:
    includes the Arrow file read).  All queried addresses are fulfilled."""
    from rubedo import lane_store
    from rubedo.db import get_session

    fresh_env()
    addrs = seed_step_history("step_a", p["hist_rows"])
    query = set(sample_evenly(addrs, p["n_addrs"]))

    times = []
    session = get_session()
    try:
        for _ in range(repeats):
            drop_table_cache()
            times.append(
                timed(
                    lambda: lane_store.batch_lookup_by_address(
                        MICRO_PIPELINE, "step_a", query, session
                    )
                )
            )
    finally:
        session.close()
    return times


@scenario("micro_batch_lookup_hot", repeats=10)
def bench_batch_lookup_hot(p, repeats):
    """batch_lookup_by_address with the table already cached in memory —
    the steady-state per-step cost during a plan."""
    from rubedo import lane_store
    from rubedo.db import get_session

    fresh_env()
    addrs = seed_step_history("step_a", p["hist_rows"])
    query = set(sample_evenly(addrs, p["n_addrs"]))

    times = []
    session = get_session()
    try:
        lane_store.batch_lookup_by_address(MICRO_PIPELINE, "step_a", query, session)  # prime
        for _ in range(repeats):
            times.append(
                timed(
                    lambda: lane_store.batch_lookup_by_address(
                        MICRO_PIPELINE, "step_a", query, session
                    )
                )
            )
    finally:
        session.close()
    return times


@scenario("micro_batch_lookup_sparse", repeats=10)
def bench_batch_lookup_sparse(p, repeats):
    """batch_lookup_by_address where only ~10% of queried addresses are
    fulfilled — the mostly-recompute plan (hot cache)."""
    from rubedo import lane_store
    from rubedo.db import get_session

    fresh_env()
    addrs = seed_step_history("step_a", p["hist_rows"])
    hits = sample_evenly(addrs, max(1, p["n_addrs"] // 10))
    misses = [f"addr-miss-{i:08d}" for i in range(p["n_addrs"] - len(hits))]
    query = set(hits) | set(misses)

    times = []
    session = get_session()
    try:
        lane_store.batch_lookup_by_address(MICRO_PIPELINE, "step_a", query, session)  # prime
        for _ in range(repeats):
            times.append(
                timed(
                    lambda: lane_store.batch_lookup_by_address(
                        MICRO_PIPELINE, "step_a", query, session
                    )
                )
            )
    finally:
        session.close()
    return times


@scenario("micro_find_latest_by_address", repeats=3)
def bench_find_latest_by_address(p, repeats):
    """The per-lane single-address path, called once per lane (hot cache).
    Times a loop of n_addrs lookups — the way planning's non-batch
    callers hit it."""
    from rubedo import lane_store

    fresh_env()
    addrs = seed_step_history("step_a", p["hist_rows"])
    picks = sample_evenly(addrs, min(p["n_addrs"], 200))  # 200 is plenty to see O(n) per call

    lane_store.find_latest_filled_by_address(
        MICRO_PIPELINE, "step_a", "row-00000000", addrs[0]
    )  # prime the cache

    def lookup_all():
        for a in picks:
            i = int(a.rsplit("-", 1)[1])
            lane_store.find_latest_filled_by_address(
                MICRO_PIPELINE, "step_a", f"row-{i:08d}", a
            )

    return [timed(lookup_all) for _ in range(repeats)]


@scenario("micro_flush_append", repeats=3)
def bench_flush_append(p, repeats):
    """flush_step of new_rows buffered rows onto hist_rows of existing
    on-disk history — the O(history) read+concat+rewrite path."""
    from rubedo import lane_store

    fresh_env()
    seed_step_history("step_a", p["hist_rows"])
    path = lane_store._get_step_file(MICRO_PIPELINE, "step_a")
    backup = path + ".bench-bak"
    shutil.copy(path, backup)

    times = []
    for rep in range(repeats):
        shutil.copy(backup, path)  # restore pristine history
        drop_table_cache()
        for i in range(p["new_rows"]):
            lane_store.append_filled(
                MICRO_PIPELINE,
                "step_a",
                lane_key=f"newrow-{rep}-{i:08d}",
                address=f"addr-new-{rep}-{i:08d}",
                input_hash=f"ih-new-{i:08d}",
                output={"path": f"n{i}.txt", "text": "x" * 40, "n": i},
                content_type="json",
                run_id=f"bench-rep{rep}",
                code_hash="codehash",
                code_version="1.0",
                output_identity=f"oid-new-{rep}-{i:08d}",
            )
        times.append(timed(lambda: lane_store.flush_step(MICRO_PIPELINE, "step_a")))
    os.remove(backup)
    return times


@scenario("micro_all_filled_rows", repeats=3)
def bench_all_filled_rows(p, repeats):
    """all_filled_rows() across n_steps step files (the gc/du refcount
    scan), cold cache each repetition."""
    from rubedo import lane_store

    fresh_env()
    rows_per_step = max(1, p["hist_rows"] // p["n_steps"])
    for s in range(p["n_steps"]):
        seed_step_history(f"step_{s}", rows_per_step)

    times = []
    for _ in range(repeats):
        drop_table_cache()
        times.append(timed(lane_store.all_filled_rows))
    return times


@scenario("micro_address_row_index", repeats=3)
def bench_address_row_index(p, repeats):
    """address_row_index() over the same layout — the server's
    address-resolution path, cold cache each repetition."""
    from rubedo import lane_store

    fresh_env()
    rows_per_step = max(1, p["hist_rows"] // p["n_steps"])
    for s in range(p["n_steps"]):
        seed_step_history(f"step_{s}", rows_per_step)

    times = []
    for _ in range(repeats):
        drop_table_cache()
        times.append(timed(lane_store.address_row_index))
    return times


# ---------------------------------------------------------------------------
# Macro scenarios — real pipelines end-to-end
# ---------------------------------------------------------------------------


@scenario("run_cold", repeats=3)
def bench_run_cold(p, repeats):
    """First run: n_files through expand → map → map → reduce, everything
    created (plan + execute + commit + flush)."""
    times = []
    for _ in range(repeats):
        fresh_env()
        make_files(p["n_files"], gen=0)
        pipe = make_pipeline()
        times.append(timed(lambda: pipe.run(workers=1)))
    return times


@scenario("run_warm", repeats=3)
def bench_run_warm(p, repeats):
    """Second run of an unchanged pipeline: everything reused.  This is
    the pure cache-hit end-to-end cost."""
    fresh_env()
    make_files(p["n_files"], gen=0)
    make_pipeline().run(workers=1)

    times = []
    for _ in range(repeats):
        drop_table_cache()  # each rep models a fresh process on a warm store
        pipe = make_pipeline()
        times.append(timed(lambda: pipe.run(workers=1)))
    return times


@scenario("run_incremental", repeats=3)
def bench_run_incremental(p, repeats):
    """One file changed out of n_files: surgical invalidation — plan must
    reuse n-1 lanes per step and recompute one chain."""
    fresh_env()
    make_files(p["n_files"], gen=0)
    make_pipeline().run(workers=1)

    times = []
    for rep in range(repeats):
        with open(os.path.join(DATA_DIR, "00000.txt"), "w") as f:
            f.write(f"changed content, revision {rep}\n")
        drop_table_cache()
        pipe = make_pipeline()
        times.append(timed(lambda: pipe.run(workers=1)))
    return times


@scenario("run_history_deep", repeats=3)
def bench_run_history_deep(p, repeats):
    """Warm run after deep_runs generations of full invalidation — every
    step's Arrow file holds deep_runs × n_files rows of history.  Shows
    how cost scales with accumulated history, not live lanes."""
    fresh_env()
    for gen in range(p["deep_runs"]):
        make_files(p["n_files"], gen=gen)
        make_pipeline().run(workers=1)

    times = []
    for _ in range(repeats):
        drop_table_cache()
        pipe = make_pipeline()
        times.append(timed(lambda: pipe.run(workers=1)))
    return times


# ---------------------------------------------------------------------------
# Shape scenarios — same workload, different pipeline shapes, with work
# counters.  The pattern for "does shape X do extra work?" questions:
# build two pipelines differing in exactly one shape knob, run the same
# phase (cold or warm), and compare both the times and the counters.
# ---------------------------------------------------------------------------


def make_util_pipeline(n_rows: int, skip_cache: bool, util_calls: List[int]):
    """Big expand source → a util map (the skip_cache knob) → consumer.

    skip_cache is invalid on expand/reduce shapes (spec validation), so
    "a big expand I never want to cache" is expressed as the expand's
    downstream util being skip_cache — the expand's lanes stay the cache
    anchors.  ``util_calls`` observes actual fn executions."""
    from rubedo import pipeline, step

    @step
    def gen():
        for i in range(n_rows):
            yield {"i": i, "text": f"payload {i} " + "lorem ipsum dolor " * 6}

    @step(skip_cache=skip_cache)
    def normalize(gen: dict):
        util_calls.append(1)
        return {"i": gen["i"], "t": gen["text"].strip().upper()}

    @step
    def report(normalize: dict):
        return {"i": normalize["i"], "length": len(normalize["t"])}

    return pipeline(name="bench_util", steps=[gen, normalize, report], home=ENV_DIR)


def _bench_util_shape(p, repeats, skip_cache: bool, warm: bool):
    times = []
    counters: Dict[str, int] = {}
    n = p["n_files"]

    if warm:
        fresh_env()
        make_util_pipeline(n, skip_cache, []).run(workers=1)
    for _ in range(repeats):
        if warm:
            drop_table_cache()  # fresh process on a warm store
        else:
            fresh_env()
        calls: List[int] = []
        pipe = make_util_pipeline(n, skip_cache, calls)
        t, counters = timed_counted(lambda: pipe.run(workers=1))
        counters["util_fn_calls"] = len(calls)
        times.append(t)
    return times, counters


@scenario("shape_util_cached_cold", repeats=3)
def bench_util_cached_cold(p, repeats):
    """Materialized util downstream of a big expand, first run — the
    baseline the skip_cache variant is compared against."""
    return _bench_util_shape(p, repeats, skip_cache=False, warm=False)


@scenario("shape_util_skipcache_cold", repeats=3)
def bench_util_skipcache_cold(p, repeats):
    """skip_cache util, first run — expect zero rows_written for the
    util step (fused into its consumer, never materialized)."""
    return _bench_util_shape(p, repeats, skip_cache=True, warm=False)


@scenario("shape_util_cached_warm", repeats=3)
def bench_util_cached_warm(p, repeats):
    """Materialized util, unchanged rerun — pays a reuse lookup for the
    util step's own lanes."""
    return _bench_util_shape(p, repeats, skip_cache=False, warm=True)


@scenario("shape_util_skipcache_warm", repeats=3)
def bench_util_skipcache_warm(p, repeats):
    """skip_cache util, unchanged rerun — expect zero util_fn_calls (the
    consumer reuses, so the fused util never runs) and no util-step
    lookups or writes."""
    return _bench_util_shape(p, repeats, skip_cache=True, warm=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def fmt_time(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1e6:8.1f}µs"
    if seconds < 1.0:
        return f"{seconds * 1e3:8.1f}ms"
    return f"{seconds:8.2f}s "


def git_info() -> Dict[str, str]:
    def _cmd(args):
        try:
            return subprocess.run(
                ["git"] + args, cwd=REPO_ROOT, capture_output=True, text=True
            ).stdout.strip()
        except OSError:
            return ""

    sha = _cmd(["rev-parse", "--short", "HEAD"])
    dirty = bool(_cmd(["status", "--porcelain"]))
    return {
        "sha": sha + ("-dirty" if dirty else ""),
        "branch": _cmd(["rev-parse", "--abbrev-ref", "HEAD"]),
    }


def cleanup():
    for d in (DATA_DIR, ENV_DIR):
        if os.path.exists(d):
            shutil.rmtree(d)


def cmd_run(args):
    params = SCALES[args.scale]
    info = git_info()
    label = args.label or f"{info['branch']}-{info['sha']}"
    selected = [
        (name, fn, reps)
        for name, fn, reps in SCENARIOS
        if not args.only or args.only in name
    ]
    if not selected:
        sys.exit(f"no scenario matches --only {args.only!r}")

    print(f"label={label}  scale={args.scale}  git={info['branch']}@{info['sha']}")
    results = []
    try:
        for name, fn, default_repeats in selected:
            repeats = args.repeats or default_repeats
            print(f"  {name} (x{repeats}) ...", end="", flush=True)
            out = fn(params, repeats)
            counters: Dict[str, int] = {}
            if isinstance(out, tuple):
                times, counters = out
            else:
                times = out
            med = statistics.median(times)
            print(f"  median {fmt_time(med).strip()}  min {fmt_time(min(times)).strip()}")
            if counters:
                shown = "  ".join(f"{k}={v}" for k, v in sorted(counters.items()))
                print(f"      work: {shown}")
            entry = {
                "name": name,
                "repeats": repeats,
                "times": times,
                "min": min(times),
                "median": med,
                "mean": statistics.fmean(times),
            }
            if counters:
                entry["counters"] = counters
            results.append(entry)
    finally:
        cleanup()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = os.path.join(RESULTS_DIR, f"{label}.json")
    with open(out_path, "w") as f:
        json.dump(
            {
                "label": label,
                "git": info,
                "scale": args.scale,
                "params": params,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "python": sys.version.split()[0],
                "results": results,
            },
            f,
            indent=2,
        )
    print(f"wrote {os.path.relpath(out_path, REPO_ROOT)}")


def load_result(label: str) -> Dict[str, Any]:
    path = os.path.join(RESULTS_DIR, f"{label}.json")
    if not os.path.exists(path):
        sys.exit(f"no result file {os.path.relpath(path, REPO_ROOT)} — run with --label {label} first")
    with open(path) as f:
        return json.load(f)


def cmd_compare(args):
    a, b = load_result(args.before), load_result(args.after)
    if a["scale"] != b["scale"]:
        print(f"WARNING: scales differ ({a['scale']} vs {b['scale']}) — deltas are meaningless\n")
    a_by_name = {r["name"]: r for r in a["results"]}
    b_by_name = {r["name"]: r for r in b["results"]}

    print(f"{a['label']} ({a['git']['sha']})  vs  {b['label']} ({b['git']['sha']})  [scale={b['scale']}]\n")
    header = f"{'scenario':<30} {'before':>10} {'after':>10} {'delta':>8} {'speedup':>8}"
    print(header)
    print("-" * len(header))
    for name in [r["name"] for r in a["results"]]:
        if name not in b_by_name:
            print(f"{name:<30} {fmt_time(a_by_name[name]['median'])} {'(missing)':>10}")
            continue
        am, bm = a_by_name[name]["median"], b_by_name[name]["median"]
        delta = (bm - am) / am * 100 if am else float("inf")
        speedup = am / bm if bm else float("inf")
        marker = "  <-- regression" if delta > 10 else ""
        print(
            f"{name:<30} {fmt_time(am)} {fmt_time(bm)} {delta:+7.1f}% {speedup:7.2f}x{marker}"
        )
        # Work-counter drift is the "it got faster but does more/less
        # work" signal — print only the keys that changed.
        ac = a_by_name[name].get("counters", {})
        bc = b_by_name[name].get("counters", {})
        for key in sorted(set(ac) | set(bc)):
            if ac.get(key, 0) != bc.get(key, 0):
                print(f"{'':<30} {key}: {ac.get(key, 0)} -> {bc.get(key, 0)}")
    for name in b_by_name:
        if name not in a_by_name:
            print(f"{name:<30} {'(missing)':>10} {fmt_time(b_by_name[name]['median'])}")


def cmd_list(_args):
    if not os.path.isdir(RESULTS_DIR):
        print("no results yet")
        return
    for fname in sorted(os.listdir(RESULTS_DIR)):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(RESULTS_DIR, fname)) as f:
            data = json.load(f)
        print(
            f"{data['label']:<30} scale={data['scale']:<7} "
            f"git={data['git']['branch']}@{data['git']['sha']:<16} {data['timestamp']}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run the suite and save results")
    p_run.add_argument("--label", help="result name (default: <branch>-<sha>)")
    p_run.add_argument("--scale", choices=SCALES, default="medium")
    p_run.add_argument("--only", help="only scenarios whose name contains this substring")
    p_run.add_argument("--repeats", type=int, help="override every scenario's repeat count")
    p_run.set_defaults(func=cmd_run)

    p_cmp = sub.add_parser("compare", help="compare two saved results")
    p_cmp.add_argument("before")
    p_cmp.add_argument("after")
    p_cmp.set_defaults(func=cmd_compare)

    p_list = sub.add_parser("list", help="list saved results")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
