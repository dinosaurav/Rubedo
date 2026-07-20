"""Bring your own Future-shaped pool with a local Ray cluster.

Optional dependency (Rubedo never imports Ray):

    uv run --with ray python examples/ray_executor/ray_executor.py

The first run executes ``square`` on Ray workers. The second run fully
reuses the plain local Rubedo cache and does not need another cluster —
the factory is only invoked when there is work to execute.
"""
from __future__ import annotations

import tempfile
from typing import Any

from rubedo import Home, pipeline, step


_ray_started = False


class RayPool:
    """Thin ``submit``/``shutdown`` adapter around Ray remote tasks."""

    def submit(self, fn, *args, **kwargs):
        import ray

        return ray.remote(fn).remote(*args, **kwargs).future()

    def shutdown(self, wait: bool = True) -> None:
        # Rubedo shuts the pool down at the end of the step segment.
        # Keep the Ray runtime up until ``main`` exits so a second
        # execute-shaped segment in the same process can reuse it.
        del wait


def make_ray_pool():
    """Zero-argument factory returning a Future-shaped Ray pool."""
    global _ray_started
    import ray

    if not _ray_started:
        ray.init(num_cpus=2, ignore_reinit_error=True)
        _ray_started = True
    return RayPool()


@step
def numbers():
    for number in range(8):
        yield {"number": number}


@step(executor=make_ray_pool)
def square(numbers: dict):
    number = numbers["number"]
    return {"number": number, "square": number * number}


@step(shape="reduce", depends_on=["square"])
def total(square: dict):
    return sum(row["square"] for row in square.values())


def main() -> None:
    try:
        # A fresh temporary Home guarantees this manual acceptance example
        # actually exercises Ray even when the repo's normal cache is warm.
        with tempfile.TemporaryDirectory(prefix="rubedo-ray-") as root:
            pipe = pipeline(
                name="ray_executor",
                steps=[numbers, square, total],
                home=Home.ephemeral(root),
            )
            first = pipe.run(workers=4)
            second = pipe.run(workers=4)
            print(
                f"First: Created {first.created_count}, "
                f"Reused {first.reused_count}"
            )
            print(
                f"Second: Created {second.created_count}, "
                f"Reused {second.reused_count}"
            )
            assert first.created_count == 17
            assert second.created_count == 0
            assert second.reused_count == first.created_count
    finally:
        if _ray_started:
            import ray

            ray.shutdown()


if __name__ == "__main__":
    main()
