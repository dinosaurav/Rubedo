import os

import pytest

from rubedo import step, pipeline
from rubedo.models import RunEvent
from conftest import isolated_test_env

TEST_FOLDER = ".test_groupkey_data"
ENV_FOLDER = ".test_groupkey_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("groupkey") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def assert_run(pipe):
    summary = pipe.run(workers=1)
    if summary.failed_count > 0:
        with TEST_HOME.session() as session:
            for e in (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            ):
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


def _outputs(step_name):
    """coordinate -> output value, for a step's live materializations."""
    return {
        cell.coordinate: cell.output
        for cell in TEST_HOME.select(f"step:{step_name}", resolve_output=True)
        if cell.output_address
    }


def test_group_key_partitions_by_indexed_field():
    create_file("a.txt", "tech")
    create_file("b.txt", "tech")
    create_file("c.txt", "biz")

    @step
    def classify(scan):
        return {"category": scan["text"].strip()}

    @step(depends_on=["classify"], group_key="category")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(name="g", steps=[scan, classify, rollup], home=TEST_HOME)
    assert_run(pipe)

    outs = _outputs("rollup")
    assert set(outs) == {"tech", "biz"}
    assert outs["tech"]["n"] == 2
    assert outs["biz"]["n"] == 1


def test_group_key_none_is_one_all_group():
    create_file("a.txt", "tech")
    create_file("b.txt", "biz")

    @step
    def classify(scan):
        return {"category": scan["text"].strip()}

    @step(depends_on=["classify"], in_shape="aggregate")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(name="g", steps=[scan, classify, rollup], home=TEST_HOME)
    assert_run(pipe)
    outs = _outputs("rollup")
    assert set(outs) == {"@all"}
    assert outs["@all"]["n"] == 2


def test_group_key_multivalue_joins_multiple_groups():
    create_file("a.txt", "solo")

    @step
    def classify(scan):
        return {"tag": ["tech", "ai"]}

    @step(depends_on=["classify"], group_key="tag")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(name="g", steps=[scan, classify, rollup], home=TEST_HOME)
    assert_run(pipe)
    outs = _outputs("rollup")
    assert set(outs) == {"tech", "ai"}
    assert outs["tech"]["n"] == 1
    assert outs["ai"]["n"] == 1


def test_group_key_reduce_after_expand():
    create_file("feed.txt", "tech\nbiz\ntech")

    @step
    def read(scan):
        return scan["text"].splitlines()

    @step
    def articles(read):
        for i, cat in enumerate(read):
            yield {"category": cat, "i": i}  # distinct payloads (i) so both "tech" survive

    @step(depends_on=["articles"], group_key="category")
    def rollup(articles):
        return {"n": len(articles)}

    pipe = pipeline(name="g", steps=[scan, read, articles, rollup], home=TEST_HOME)
    assert_run(pipe)
    outs = _outputs("rollup")
    # reduce gathers the minted expand lanes and groups them
    assert set(outs) == {"tech", "biz"}
    assert outs["tech"]["n"] == 2
    assert outs["biz"]["n"] == 1


def test_group_key_missing_field_raises():
    create_file("a.txt", "hello")

    @step  # no "category" field in the output
    def classify(scan):
        return {"type": "tech"}

    @step(depends_on=["classify"], group_key="category")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(name="g", steps=[scan, classify, rollup], home=TEST_HOME)
    with pytest.raises(ValueError, match="no value"):
        pipe.run(workers=1)


def test_group_key_infers_aggregate_shape_but_an_explicit_conflict_still_raises():
    # group_key= alone (no shape=) infers in_shape="aggregate" (TODO 22) — no
    # error. An explicit, conflicting shape still raises.
    inferred = step(name="ok", version="1", depends_on=["x"], group_key="category")(
        lambda x: None
    )
    assert inferred.in_shape == "aggregate"

    with pytest.raises(ValueError, match="group_key requires in_shape='aggregate' or 'fold'"):
        step(name="bad", version="1", shape="map", group_key="category")(lambda: None)
