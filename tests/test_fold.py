"""Fold: deterministic, cached collective accumulation."""

import os
import shutil

import pytest

from rubedo import pipeline, step
from rubedo.hashing import hash_json
from conftest import make_home


ENV_FOLDER = ".test_fold_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    if os.path.exists(abs_env_folder):
        shutil.rmtree(abs_env_folder)
    os.makedirs(abs_env_folder, exist_ok=True)

    TEST_HOME = make_home(ENV_FOLDER)
    yield
    shutil.rmtree(abs_env_folder, ignore_errors=True)


def test_fold_accumulates_lanes_in_coordinate_order_and_reuses():
    calls = []

    @step
    def source():
        # Child coordinates derive from these values, not yielding order.
        yield {"value": "z"}
        yield {"value": "a"}
        yield {"value": "m"}

    @step(in_shape="fold", fold_init="", depends_on=["source"])
    def combine(acc, source):
        calls.append(source["value"])
        return acc + source["value"]

    pipe = pipeline(name="fold-basic", steps=[source, combine], home=TEST_HOME)
    first = pipe.run(workers=1)
    assert first.created_count == 4
    expected_order = [
        value["value"]
        for _, value in sorted(
            (f"row-{hash_json(value)[:12]}", value)
            for value in ({"value": "z"}, {"value": "a"}, {"value": "m"})
        )
    ]
    assert calls == expected_order
    assert first.output_for("combine") == {"@all": "".join(expected_order)}

    second = pipe.run(workers=1)
    assert second.created_count == 0
    assert second.reused_count == 4
    assert len(calls) == 3


def test_fold_groups_and_resets_its_accumulator():
    @step
    def source():
        yield {"group": "east", "amount": 2}
        yield {"group": "west", "amount": 3}
        yield {"group": "east", "amount": 5}

    @step(in_shape="fold", fold_init=0, group_key="group", depends_on=["source"])
    def total(acc, source):
        return acc + source["amount"]

    pipe = pipeline(name="fold-groups", steps=[source, total], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.created_count == 5
    assert summary.output_for("total") == {"east": 7, "west": 3}

    assert pipe.run(workers=1).reused_count == 5


def test_fold_check_cache_false_reexecutes():
    calls = []

    @step
    def source():
        yield {"value": 1}

    @step(
        in_shape="fold", fold_init=0, depends_on=["source"], check_cache=False
    )
    def total(acc, source):
        calls.append(source["value"])
        return acc + source["value"]

    pipe = pipeline(name="fold-no-cache", steps=[source, total], home=TEST_HOME)
    pipe.run(workers=1)
    pipe.run(workers=1)
    assert calls == [1, 1]


def test_fold_copies_mutable_initial_values_per_group():
    @step
    def source():
        yield {"group": "east", "value": "a"}
        yield {"group": "west", "value": "b"}

    @step(in_shape="fold", fold_init=[], group_key="group", depends_on=["source"])
    def collect(acc, source):
        acc.append(source["value"])
        return acc

    pipe = pipeline(name="fold-mutable-init", steps=[source, collect], home=TEST_HOME)
    assert pipe.run(workers=1).output_for("collect") == {
        "east": ["a"],
        "west": ["b"],
    }


def test_fold_requires_json_serializable_initial_value():
    with pytest.raises(ValueError, match="requires fold_init"):

        @step(in_shape="fold", depends_on=["source"])
        def missing(acc, source):
            return acc

    with pytest.raises(ValueError, match="JSON-serializable"):

        @step(in_shape="fold", fold_init={object()}, depends_on=["source"])
        def invalid(acc, source):
            return acc


def test_fold_rejects_arrow_aggregate():
    with pytest.raises(ValueError, match="arrow_aggregate=True requires"):

        @step(
            in_shape="fold", fold_init=0, depends_on=["source"], arrow_aggregate=True
        )
        def total(acc, source):
            return acc + source


def test_fold_rejects_multiple_parents():
    with pytest.raises(ValueError, match="takes exactly one parent"):

        @step(in_shape="fold", fold_init=0, depends_on=["left", "right"])
        def total(acc, value):
            return acc + value
