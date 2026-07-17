import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import RunCoordinateStatus, RunEvent, InputHashUsage
from rubedo import lane_store
from rubedo.planning import _ArrowRowRef
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
            .filter(RunCoordinateStatus.output_address.isnot(None))
            .all()
        )
        for st in statuses:
            if st.output_address:
                row = lane_store.address_row_index().get(str(st.output_address))
                if row:
                    usage = session.query(InputHashUsage).filter_by(address=str(st.output_address)).first()
                    if usage and usage.fulfilled:
                        result[st.coordinate] = read_materialization_output(_ArrowRowRef(row))
    return result


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

    pipe = pipeline(name="g", steps=[scan, classify, rollup])
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

    @step(depends_on=["classify"], shape="reduce")
    def rollup(classify):
        return {"n": len(classify)}

    pipe = pipeline(name="g", steps=[scan, classify, rollup])
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

    pipe = pipeline(name="g", steps=[scan, classify, rollup])
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

    pipe = pipeline(name="g", steps=[scan, read, articles, rollup])
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

    pipe = pipeline(name="g", steps=[scan, classify, rollup])
    with pytest.raises(ValueError, match="no value"):
        pipe.run(workers=1)


def test_group_key_infers_reduce_shape_but_an_explicit_conflict_still_raises():
    # group_key= alone (no shape=) infers shape="reduce" (TODO 22) — no
    # error. An explicit, conflicting shape still raises.
    inferred = step(name="ok", version="1", depends_on=["x"], group_key="category")(
        lambda x: None
    )
    assert inferred.shape == "reduce"

    with pytest.raises(ValueError, match="group_key requires shape='reduce'"):
        step(name="bad", version="1", shape="map", group_key="category")(lambda: None)
