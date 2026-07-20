"""Future-shaped external executor factories (TODO 8)."""
from __future__ import annotations

import concurrent.futures
import json
from dataclasses import dataclass
from typing import Any

import pytest

from conftest import make_home
from rubedo import pipeline, step
from rubedo.models import RunCoordinateStatus


@dataclass
class FakePool:
    submit_count: int = 0
    shutdown_called: bool = False
    shutdown_wait: bool | None = None

    def submit(self, fn, /, *args, **kwargs):
        self.submit_count += 1
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except BaseException as exc:
            future.set_exception(exc)
        return future

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_called = True
        self.shutdown_wait = wait


POOLS: list[FakePool] = []


def make_fake_pool() -> FakePool:
    pool = FakePool()
    POOLS.append(pool)
    return pool


def _work():
    return {"value": 2}


def _double(work: dict):
    return {"value": work["value"] * 2}


def _numbers():
    for value in range(3):
        yield {"value": value}


def _square(numbers: dict):
    return {"value": numbers["value"] ** 2}


def _tag(square: dict):
    return {"value": square["value"], "tagged": True}


def _snapshot(home, run_id: str) -> list[tuple[str, str, str]]:
    with home.session() as session:
        rows = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=run_id)
            .order_by(RunCoordinateStatus.step_name)
            .all()
        )
        return [
            (str(row.coordinate), str(row.status), str(row.output_address))
            for row in rows
        ]


def test_factory_executor_matches_thread_addresses_and_statuses(tmp_path):
    thread_home = make_home(str(tmp_path / "thread"))
    external_home = make_home(str(tmp_path / "external"))
    thread_step = step(name="work", executor="thread")(_work)
    external_step = step(name="work", executor=make_fake_pool)(_work)

    thread_pipe = pipeline(
        name="executor-parity", steps=[thread_step], home=thread_home
    )
    external_pipe = pipeline(
        name="executor-parity", steps=[external_step], home=external_home
    )
    thread_first = thread_pipe.run(workers=1)
    external_first = external_pipe.run(workers=1)

    assert thread_first.created_count == external_first.created_count == 1
    assert _snapshot(thread_home, thread_first.run_id) == _snapshot(
        external_home, external_first.run_id
    )
    assert thread_pipe.run(workers=1).reused_count == 1
    assert external_pipe.run(workers=1).reused_count == 1


def test_factory_pool_used_once_and_shutdown(tmp_path):
    POOLS.clear()
    home = make_home(str(tmp_path / "home"))
    external_step = step(name="work", executor=make_fake_pool)(_work)
    pipe = pipeline(name="executor-lifecycle", steps=[external_step], home=home)

    summary = pipe.run(workers=1)

    assert summary.created_count == 1
    assert len(POOLS) == 1
    assert POOLS[0].submit_count == 1
    assert POOLS[0].shutdown_called
    assert POOLS[0].shutdown_wait is True


def test_mixed_factory_and_thread_steps_reuse(tmp_path):
    POOLS.clear()
    home = make_home(str(tmp_path / "home"))
    external_step = step(name="work", executor=make_fake_pool)(_work)
    thread_step = step(name="double")(_double)
    pipe = pipeline(
        name="executor-mixed",
        steps=[external_step, thread_step],
        home=home,
    )

    first = pipe.run(workers=2)
    second = pipe.run(workers=2)

    assert first.created_count == 2
    assert second.reused_count == 2
    assert POOLS[0].submit_count == 1


def test_multi_lane_factory_runs_once_per_deep_segment(tmp_path):
    POOLS.clear()
    home = make_home(str(tmp_path / "home"))
    source = step(name="numbers")(_numbers)
    external_step = step(name="square", executor=make_fake_pool)(_square)
    thread_step = step(name="tag")(_tag)
    pipe = pipeline(
        name="executor-deep",
        steps=[source, external_step, thread_step],
        home=home,
        schedule="deep",
    )

    summary = pipe.run(workers=3)

    assert summary.created_count == 9
    assert len(POOLS) == 1
    assert POOLS[0].submit_count == 3
    assert POOLS[0].shutdown_called


def test_external_pool_retries_and_shuts_down(tmp_path):
    POOLS.clear()
    attempts: list[int] = []

    def flaky():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("transient")
        return {"ok": True}

    home = make_home(str(tmp_path / "home"))
    external_step = step(
        name="flaky",
        executor=make_fake_pool,
        retries=1,
        retry_on=RuntimeError,
    )(flaky)
    pipe = pipeline(name="executor-retry", steps=[external_step], home=home)

    summary = pipe.run(workers=1)

    assert summary.created_count == 1
    assert len(attempts) == 2
    assert POOLS[0].submit_count == 2
    assert POOLS[0].shutdown_called


def test_definition_serializes_factory_marker(tmp_path):
    home = make_home(str(tmp_path / "home"))
    external_step = step(name="work", executor=make_fake_pool)(_work)
    pipe = pipeline(name="executor-definition", steps=[external_step], home=home)

    snapshot = pipe.definition()
    marker = snapshot["steps"][0]["executor"]
    assert marker.startswith("external:")
    assert marker.endswith(".make_fake_pool")
    json.dumps(snapshot)


def test_invalid_executor_values_reject():
    with pytest.raises(ValueError, match="zero-argument pool factory"):
        step(name="bad", executor="dask")(_work)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="zero arguments"):
        step(name="bad-factory", executor=lambda required: FakePool())(_work)
    with pytest.raises(ValueError, match="zero-argument pool factory"):
        step(name="bad-value", executor=42)(_work)  # type: ignore[arg-type]


def test_factory_must_return_submit_pool(tmp_path):
    class InvalidPool:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    invalid = InvalidPool()
    home = make_home(str(tmp_path / "home"))
    invalid_step = step(name="work", executor=lambda: invalid)(_work)
    pipe = pipeline(name="executor-invalid-pool", steps=[invalid_step], home=home)

    with pytest.raises(TypeError, match="expected an object with submit"):
        pipe.run(workers=1)
    assert invalid.closed
