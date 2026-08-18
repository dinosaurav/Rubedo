"""pipeline(cache_default=...): maps inherit use_cache unless set explicitly."""

import pytest

from conftest import isolated_test_env
from rubedo import pipeline, step
from rubedo.spec import definition

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("cache_default") as env:
        TEST_HOME = env.home
        yield


def test_cache_default_false_fuses_unset_maps():
    calls = []

    @step(use_cache=True)
    def src():
        return {"text": "  Hi  "}

    @step
    def trim(src: dict):
        calls.append("trim")
        return src["text"].strip()

    @step(use_cache=True)
    def out(trim: str):
        return trim.lower()

    pipe = pipeline(
        name="cd_fuse",
        steps=[src, trim, out],
        home=TEST_HOME,
        cache_default=False,
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    assert summary.created_count == 2  # src + out; trim fused
    assert calls == ["trim"]

    names = {r.get("step_name") for r in TEST_HOME.lanes.all_filled_rows()}
    assert names == {"src", "out"}

    summary2 = pipe.run(workers=1)
    assert summary2.reused_count == 2
    assert calls == ["trim"], "fused util must not run on a fully cached consumer"


def test_cache_default_false_leaf_without_use_cache_true_raises():
    @step
    def src():
        return {"x": 1}

    @step
    def leaf(src: dict):
        return src

    with pytest.raises(ValueError, match="use_cache=False but no consumer"):
        pipeline(
            name="cd_leaf",
            steps=[src, leaf],
            home=TEST_HOME,
            cache_default=False,
        ).spec


def test_cache_default_false_does_not_fuse_expand():
    @step
    def src():
        for i in range(2):
            yield {"i": i}

    @step(use_cache=True)
    def twice(src: dict):
        return {"i": src["i"] * 2}

    pipe = pipeline(
        name="cd_exp",
        steps=[src, twice],
        home=TEST_HOME,
        cache_default=False,
    )
    summary = pipe.run(workers=1)
    assert summary.failed_count == 0
    names = {r.get("step_name") for r in TEST_HOME.lanes.all_filled_rows()}
    assert "src" in names
    assert "twice" in names
    assert summary.created_count == 4  # 2 expand lanes + 2 maps


def test_explicit_use_cache_true_overrides_false_default():
    @step(use_cache=True)
    def src():
        return {"x": 1}

    @step(use_cache=True)
    def child(src: dict):
        return src

    pipe = pipeline(
        name="cd_over",
        steps=[src, child],
        home=TEST_HOME,
        cache_default=False,
    )
    snap = definition(pipe.spec)
    assert snap.get("cache_default") is False
    by_name = {s["name"]: s for s in snap["steps"]}
    assert "use_cache" not in by_name["src"]  # resolved True omitted
    assert "use_cache" not in by_name["child"]


def test_definition_snapshots_resolved_use_cache_false():
    @step
    def src():
        return {"text": "a"}

    @step
    def trim(src: dict):
        return src["text"]

    @step(use_cache=True)
    def out(trim: str):
        return trim

    pipe = pipeline(
        name="cd_snap",
        steps=[src, trim, out],
        home=TEST_HOME,
        cache_default=False,
    )
    snap = definition(pipe.spec)
    assert snap["cache_default"] is False
    by_name = {s["name"]: s for s in snap["steps"]}
    assert by_name["src"]["use_cache"] is False
    assert by_name["trim"]["use_cache"] is False
    assert "use_cache" not in by_name["out"]


def test_inherited_false_plus_force_raises_at_build():
    @step(force=True)
    def src():
        return {"x": 1}

    @step(use_cache=True)
    def child(src: dict):
        return src

    with pytest.raises(ValueError, match="contradictory with use_cache=False"):
        pipeline(
            name="cd_force",
            steps=[src, child],
            home=TEST_HOME,
            cache_default=False,
        ).spec


def test_same_step_object_resolves_per_pipeline():
    """Decorator-returned StepSpec is not mutated; two pipelines can
    disagree on cache_default."""

    @step
    def src():
        return {"x": 1}

    @step
    def mid(src: dict):
        return src["x"]

    @step(use_cache=True)
    def out(mid):
        return mid

    fused = pipeline(
        name="cd_a",
        steps=[src, mid, out],
        home=TEST_HOME,
        cache_default=False,
    )
    stored = pipeline(
        name="cd_b",
        steps=[src, mid, out],
        home=TEST_HOME,
        cache_default=True,
    )
    assert fused.spec.steps[1].use_cache is False
    assert stored.spec.steps[1].use_cache is True
    assert src.use_cache is None
    assert mid.use_cache is None


def test_cache_default_must_be_bool():
    with pytest.raises(ValueError, match="cache_default must be True or False"):
        pipeline(name="cd_bad", home=TEST_HOME, cache_default=1)  # type: ignore[arg-type]
