"""Benchmark: planning phase reuse lookup performance.

Builds a pipeline with N lanes through a multi-step chain, runs it once
to populate the lane store, then times .plan() (the reuse-check path)
on a second run.  This isolates batch_lookup_by_address — the hot path
that scans Arrow tables for fulfilled addresses.

Usage:
    uv run python bench/bench_plan_lookup.py [--lanes N] [--steps M]

Defaults: 5000 lanes, 4 steps (1 expand root + 3 map).
"""
import argparse
import os
import shutil
import sys
import time

sys.path.insert(0, "src")

from rubedo import step, pipeline
from rubedo.db import init_db
from rubedo.store import init_store


def run_bench(n_lanes: int, n_steps: int):
    shutil.rmtree(".rubedo", ignore_errors=True)
    os.environ["RUBEDO_DB_PATH"] = "sqlite:///.rubedo/bench.sqlite"
    init_db()
    init_store()

    # Use a map root with params to create N lanes — avoids the expand
    # root always-rerun, so the reuse path is pure planning lookup.
    @step
    def source():
        return {"lanes": n_lanes}

    def make_gen(pname, parent_name):
        def fn(parent: dict):
            for i in range(parent["lanes"]):
                yield {"id": i, "text": f"lane_{i}_content"}
        fn.__name__ = pname
        return step(fn=fn, name=pname, shape="expand", depends_on={"parent": parent_name})

    gen = make_gen("gen", "source")
    steps = [source, gen]
    prev = "gen"

    for j in range(n_steps - 1):
        step_name = f"step_{j}"
        parent_name = prev

        def make_fn(pname):
            def fn(parent: dict):
                return {"id": parent["id"], "text": parent.get("text", parent.get("processed", "")) + "_x"}
            fn.__name__ = pname
            return fn

        s = step(fn=make_fn(step_name), name=step_name, depends_on={"parent": parent_name})
        steps.append(s)
        prev = step_name

    pipe = pipeline(name="bench", steps=steps)

    print(f"Building: {n_lanes} lanes x {n_steps} steps...")
    t0 = time.perf_counter()
    summary = pipe.run(workers=1)
    t1 = time.perf_counter()
    print(f"  Run 1 (populate): {t1 - t0:.3f}s  created={summary.created_count}")

    # Run 2: the reuse check path.  The root expand re-runs (source),
    # but its children are content-addressed so downstream steps should
    # reuse.  .plan() on a cached run is the pure planning path.
    print(f"Run 2 (reuse)...")
    t0 = time.perf_counter()
    summary2 = pipe.run(workers=1)
    t1 = time.perf_counter()
    run2_time = t1 - t0
    print(f"  Run 2: {run2_time:.3f}s  created={summary2.created_count} reused={summary2.reused_count}")

    print(f"Planning (reuse check)...")
    t0 = time.perf_counter()
    plan = pipe.plan()
    t1 = time.perf_counter()
    plan_time = t1 - t0
    print(f"  .plan(): {plan_time:.3f}s  reuse={plan.counts.get('reuse', 0)} execute={plan.counts.get('execute', 0)}")

    t0 = time.perf_counter()
    plan2 = pipe.plan()
    t1 = time.perf_counter()
    plan_time_2 = t1 - t0
    print(f"  .plan() (2nd): {plan_time_2:.3f}s  reuse={plan2.counts.get('reuse', 0)} execute={plan2.counts.get('execute', 0)}")

    print(f"\nResult: run2={run2_time:.3f}s  plan_time={plan_time:.3f}s  plan_time_2={plan_time_2:.3f}s")
    return run2_time, plan_time, plan_time_2


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lanes", type=int, default=5000)
    parser.add_argument("--steps", type=int, default=4)
    args = parser.parse_args()

    run_bench(args.lanes, args.steps)
