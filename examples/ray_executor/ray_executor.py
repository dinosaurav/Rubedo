"""Heavy multi-step Ray workload — expand → burn → polish → verify → report.

Three Ray-executed map steps do real CPU work (SHA-256 stretching + a
pure-Python mixer). Defaults are meant to take on the order of a minute
on a few cores; scale up or down with env vars:

    RUBEDO_RAY_SHARDS=64 \\
    RUBEDO_RAY_SHA_ROUNDS=8000000 \\
    RUBEDO_RAY_MIX_ITERS=1000000 \\
      uv run python examples/ray_executor/ray_executor.py

Ray is in the repo's ``dev`` dependency group (Rubedo never imports it):

    uv run python examples/ray_executor/ray_executor.py

The first run pays the Ray cost. The second run fully reuses Rubedo's
cache and finishes almost instantly — the factory is only invoked when
there is work to execute.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
import time

from rubedo import Home, pipeline, step


_ray_started = False

# Defaults aim for roughly a minute on a few cores. Raise the env vars to
# make Ray sweat; lower them for a quick smoke.
SHARDS = int(os.environ.get("RUBEDO_RAY_SHARDS", "48"))
SHA_ROUNDS = int(os.environ.get("RUBEDO_RAY_SHA_ROUNDS", "5000000"))
MIX_ITERS = int(os.environ.get("RUBEDO_RAY_MIX_ITERS", "600000"))
RAY_CPUS = int(os.environ.get("RUBEDO_RAY_CPUS", str(os.cpu_count() or 4)))


class RayPool:
    """Thin ``submit``/``shutdown`` adapter around Ray remote tasks."""

    def submit(self, fn, *args, **kwargs):
        import ray

        return ray.remote(fn).remote(*args, **kwargs).future()

    def shutdown(self, wait: bool = True) -> None:
        # Rubedo shuts the pool down at the end of the step segment.
        # Keep the Ray runtime up until ``main`` exits so later
        # execute-shaped segments in the same process can reuse it.
        del wait


def make_ray_pool():
    """Zero-argument factory returning a Future-shaped Ray pool."""
    global _ray_started
    import ray

    if not _ray_started:
        ray.init(num_cpus=RAY_CPUS, ignore_reinit_error=True)
        _ray_started = True
    return RayPool()


def _stretch(seed: bytes, rounds: int) -> bytes:
    """CPU-bound SHA-256 chain — releases the GIL, parallelizes on Ray."""
    digest = hashlib.sha256(seed).digest()
    for i in range(rounds):
        digest = hashlib.sha256(digest + i.to_bytes(4, "little")).digest()
    return digest


def _mix(start: int, rounds: int) -> int:
    """Pure-Python follow-up so the step isn't *only* C crypto."""
    x = start & ((1 << 64) - 1)
    score = 0
    for _ in range(rounds):
        x = (x * 6364136223846793005 + 1) & ((1 << 64) - 1)
        # Fold in a short Collatz-ish walk so the loop isn't a single mul.
        y = x
        steps = 0
        while y > 1 and steps < 24:
            y = y // 2 if y % 2 == 0 else 3 * y + 1
            steps += 1
        score += steps
    return score


def _stage(tag: str, job_id: int, material: bytes, salt: int) -> dict:
    digest = _stretch(material + f"|{tag}:{job_id}".encode(), SHA_ROUNDS)
    score = _mix(int.from_bytes(digest[:8], "little") ^ salt, MIX_ITERS)
    return {
        "job_id": job_id,
        "tag": tag,
        "digest": digest.hex(),
        "score": score,
        "sha_rounds": SHA_ROUNDS,
        "mix_iters": MIX_ITERS,
    }


@step
def jobs():
    """Fan out one lane per shard — each Ray hop processes one."""
    for job_id in range(SHARDS):
        seed = 1_000_003 * (job_id + 1)
        yield {
            "job_id": job_id,
            "seed": seed,
            "material": f"job:{job_id}:{seed}".encode().hex(),
        }


@step(executor=make_ray_pool)
def burn(jobs: dict):
    """Ray hop 1: stretch + mix the raw shard material."""
    out = _stage("burn", jobs["job_id"], bytes.fromhex(jobs["material"]), jobs["seed"])
    out["seed"] = jobs["seed"]
    return out


@step(executor=make_ray_pool)
def polish(burn: dict):
    """Ray hop 2: re-stretch the burn digest and remix."""
    out = _stage(
        "polish",
        burn["job_id"],
        bytes.fromhex(burn["digest"]),
        burn["score"] ^ burn["seed"],
    )
    out["burn_score"] = burn["score"]
    out["seed"] = burn["seed"]
    return out


@step(executor=make_ray_pool)
def verify(polish: dict):
    """Ray hop 3: final stretch/mix; carries a combined score forward."""
    out = _stage(
        "verify",
        polish["job_id"],
        bytes.fromhex(polish["digest"]),
        polish["score"] ^ polish["burn_score"],
    )
    out["burn_score"] = polish["burn_score"]
    out["polish_score"] = polish["score"]
    out["combined"] = polish["burn_score"] + polish["score"] + out["score"]
    return out


@step(shape="reduce", depends_on=["verify"])
def report(verify: dict):
    rows = sorted(verify.values(), key=lambda row: row["job_id"])
    total = sum(row["combined"] for row in rows)
    top = max(rows, key=lambda row: row["combined"])
    return {
        "shards": len(rows),
        "total_combined": total,
        "top_job": top["job_id"],
        "top_combined": top["combined"],
        "sha_rounds": SHA_ROUNDS,
        "mix_iters": MIX_ITERS,
        "ray_hops": 3,
    }


def main() -> None:
    expected = 4 * SHARDS + 1  # jobs + burn + polish + verify + report
    print(
        f"Ray workload: {SHARDS} shards × 3 hops × "
        f"(sha={SHA_ROUNDS:,} + mix={MIX_ITERS:,}), "
        f"{RAY_CPUS} CPUs → expect {expected} created lanes"
    )
    try:
        # Fresh temporary Home so a warm repo cache can't skip the Ray work.
        with tempfile.TemporaryDirectory(prefix="rubedo-ray-") as root:
            pipe = pipeline(
                name="ray_executor",
                steps=[jobs, burn, polish, verify, report],
                home=Home.ephemeral(root),
            )
            print(pipe.describe())
            print()

            t0 = time.perf_counter()
            first = pipe.run(workers=RAY_CPUS)
            t1 = time.perf_counter()
            second = pipe.run(workers=RAY_CPUS)
            t2 = time.perf_counter()

            print(
                f"First:  Created {first.created_count}, "
                f"Reused {first.reused_count}  ({t1 - t0:.1f}s)"
            )
            print(
                f"Second: Created {second.created_count}, "
                f"Reused {second.reused_count}  ({t2 - t1:.1f}s)"
            )
            assert first.created_count == expected, (
                f"expected {expected} created, got {first.created_count}"
            )
            assert second.created_count == 0
            assert second.reused_count == first.created_count
    finally:
        if _ray_started:
            import ray

            ray.shutdown()


if __name__ == "__main__":
    main()
