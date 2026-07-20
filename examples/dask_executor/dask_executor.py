"""Bring your own Future-shaped pool with a local Dask cluster.

Optional dependency (Rubedo never imports Dask):

    uv run --with "dask[distributed]" \
      python examples/dask_executor/dask_executor.py

The first run executes ``square`` on Dask workers. The second run fully
reuses the plain local Rubedo cache and does not create another cluster.
"""
from __future__ import annotations

from typing import Any

from rubedo import pipeline, step


_cluster: Any = None
_client: Any = None


def make_dask_pool():
    """Zero-argument factory returning Dask's Future-shaped executor."""
    global _cluster, _client
    from dask.distributed import Client, LocalCluster

    _cluster = LocalCluster(n_workers=2, threads_per_worker=1)
    _client = Client(_cluster)
    return _client.get_executor()


@step
def numbers():
    for number in range(8):
        yield {"number": number}


@step(executor=make_dask_pool)
def square(numbers: dict):
    number = numbers["number"]
    return {"number": number, "square": number * number}


@step(shape="reduce", depends_on=["square"])
def total(square: dict):
    return sum(row["square"] for row in square.values())


pipe = pipeline(
    name="dask_executor",
    steps=[numbers, square, total],
)


def main() -> None:
    try:
        first = pipe.run(workers=4)
        second = pipe.run(workers=4)
        print(
            f"First: Created {first.created_count}, Reused {first.reused_count}"
        )
        print(
            f"Second: Created {second.created_count}, Reused {second.reused_count}"
        )
        assert first.created_count == 17  # 8 source + 8 square + 1 aggregate
        assert second.created_count == 0
        assert second.reused_count == first.created_count
    finally:
        if _client is not None:
            _client.close()
        if _cluster is not None:
            _cluster.close()


if __name__ == "__main__":
    main()
