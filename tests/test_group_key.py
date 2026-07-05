import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import FolderSource, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, RunCoordinateStatus, RunEvent
from rubedo.store import init_store, read_materialization_output

TEST_FOLDER = ".test_groupkey_data"
ENV_FOLDER = ".test_groupkey_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def assert_run(pipe):
    summary = run(pipe, workers=1)
    if summary.failed_count > 0:
        with get_session() as session:
            for e in (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            ):
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


def _outputs(step_name):
    """coordinate -> output value, for a step's live materializations."""
    result = {}
    with get_session() as session:
        statuses = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name=step_name)
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        )
        for st in statuses:
            mat = session.get(Materialization, st.materialization_id)
            if mat and mat.is_live:
                result[st.coordinate] = read_materialization_output(mat)
    return result


def test_group_key_partitions_by_indexed_field():
    create_file("a.txt", "tech")
    create_file("b.txt", "tech")
    create_file("c.txt", "biz")

    @step(name="classify", version="1", index=["category"])
    def classify(path):
        return {"category": open(path).read().strip()}

    @step(
        name="rollup", version="1", depends_on=["classify"],
        shape="reduce", group_key="category",
    )
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(
        id="g", name="g", source=FolderSource(TEST_FOLDER), steps=[classify, rollup]
    )
    assert_run(pipe)

    outs = _outputs("rollup")
    assert set(outs) == {"tech", "biz"}
    assert outs["tech"]["n"] == 2
    assert outs["biz"]["n"] == 1


def test_group_key_none_is_one_all_group():
    create_file("a.txt", "tech")
    create_file("b.txt", "biz")

    @step(name="classify", version="1", index=["category"])
    def classify(path):
        return {"category": open(path).read().strip()}

    @step(name="rollup", version="1", depends_on=["classify"], shape="reduce")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(
        id="g", name="g", source=FolderSource(TEST_FOLDER), steps=[classify, rollup]
    )
    assert_run(pipe)
    outs = _outputs("rollup")
    assert set(outs) == {"@all"}
    assert outs["@all"]["n"] == 2


def test_group_key_multivalue_joins_multiple_groups():
    create_file("a.txt", "solo")

    @step(name="classify", version="1", index=["tag"])
    def classify(path):
        return {"tag": ["tech", "ai"]}

    @step(
        name="rollup", version="1", depends_on=["classify"],
        shape="reduce", group_key="tag",
    )
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(
        id="g", name="g", source=FolderSource(TEST_FOLDER), steps=[classify, rollup]
    )
    assert_run(pipe)
    outs = _outputs("rollup")
    # the single lane is a member of both groups
    assert set(outs) == {"tech", "ai"}
    assert outs["tech"]["n"] == 1
    assert outs["ai"]["n"] == 1


def test_group_key_reduce_after_expand():
    create_file("feed.txt", "tech\nbiz\ntech")

    @step(name="read", version="1")
    def read(path):
        return open(path).read().splitlines()

    @step(
        name="articles", version="1", depends_on=["read"],
        shape="expand", index=["category"],
    )
    def articles(read):
        for i, cat in enumerate(read):
            yield str(i), {"category": cat}

    @step(
        name="rollup", version="1", depends_on=["articles"],
        shape="reduce", group_key="category",
    )
    def rollup(articles):
        return {"n": len(articles)}

    pipe = pipeline(
        id="g", name="g", source=FolderSource(TEST_FOLDER),
        steps=[read, articles, rollup],
    )
    assert_run(pipe)
    outs = _outputs("rollup")
    # reduce gathers the minted expand lanes and groups them
    assert set(outs) == {"tech", "biz"}
    assert outs["tech"]["n"] == 2
    assert outs["biz"]["n"] == 1


def test_group_key_unindexed_field_raises():
    create_file("a.txt", "hello")

    @step(name="classify", version="1")  # category is NOT indexed
    def classify(path):
        return {"category": "tech"}

    @step(
        name="rollup", version="1", depends_on=["classify"],
        shape="reduce", group_key="category",
    )
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(
        id="g", name="g", source=FolderSource(TEST_FOLDER), steps=[classify, rollup]
    )
    with pytest.raises(ValueError, match="no indexed value"):
        run(pipe, workers=1)


def test_group_key_requires_reduce_shape():
    with pytest.raises(ValueError, match="group_key requires shape='reduce'"):
        step(name="bad", version="1", group_key="category")(lambda: None)
