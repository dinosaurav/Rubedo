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

DIR_A = ".test_ms_a"
DIR_B = ".test_ms_b"
ENV_FOLDER = ".test_ms_env"


@pytest.fixture(autouse=True)
def isolated_env():
    dirs = [os.path.abspath(d) for d in (DIR_A, DIR_B, ENV_FOLDER)]
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{os.path.abspath(ENV_FOLDER)}/store/objects"
    rubedo.store.STAGING_DIR = f"{os.path.abspath(ENV_FOLDER)}/store/staging"

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

    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def write(folder, name, content):
    with open(os.path.join(folder, name), "w") as f:
        f.write(content)


def assert_run(pipe, **kw):
    summary = run(pipe, workers=1, **kw)
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
    result = {}
    with get_session() as session:
        for st in (
            session.query(RunCoordinateStatus)
            .filter_by(step_name=step_name)
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        ):
            mat = session.get(Materialization, st.materialization_id)
            if mat and mat.is_live:
                result[st.coordinate] = read_materialization_output(mat)
    return result


def test_two_named_sources_parallel_chains():
    write(DIR_A, "one.txt", "abc")
    write(DIR_B, "two.txt", "xyz")

    @step(name="load_a", version="1", source="a")
    def load_a(path):
        return open(path).read().upper()

    @step(name="load_b", version="1", source="b")
    def load_b(path):
        return open(path).read()[::-1]

    pipe = pipeline(
        id="ms", name="ms",
        sources={"a": FolderSource(DIR_A), "b": FolderSource(DIR_B)},
        steps=[load_a, load_b],
    )
    s = assert_run(pipe)
    assert s.created_count == 2

    # each root read its own source
    assert _outputs("load_a") == {"one.txt": "ABC"}
    assert _outputs("load_b") == {"two.txt": "zyx"}

    # re-run: both reused (each source cached independently)
    s2 = assert_run(pipe)
    assert (s2.created_count, s2.reused_count) == (0, 2)


def test_single_source_unaffected_by_multisource_machinery():
    write(DIR_A, "f.txt", "hi")

    @step(name="up", version="1")
    def up(path):
        return open(path).read().upper()

    # plain single source= still works, no source= on the root step
    pipe = pipeline(id="s", name="s", source=FolderSource(DIR_A), steps=[up])
    assert_run(pipe)
    assert _outputs("up") == {"f.txt": "HI"}


def test_root_step_unknown_source_raises():
    @step(name="load_a", version="1", source="nope")
    def load_a(path):
        return path

    with pytest.raises(ValueError, match="not in sources"):
        pipeline(
            id="ms", name="ms",
            sources={"a": FolderSource(DIR_A)},
            steps=[load_a],
        )


def test_multisource_root_without_source_raises():
    @step(name="load", version="1")  # no source= but 2 sources exist
    def load(path):
        return path

    with pytest.raises(ValueError, match="must declare source="):
        pipeline(
            id="ms", name="ms",
            sources={"a": FolderSource(DIR_A), "b": FolderSource(DIR_B)},
            steps=[load],
        )


def test_source_override_rejected_for_multisource():
    write(DIR_A, "one.txt", "a")
    write(DIR_B, "two.txt", "b")

    @step(name="load_a", version="1", source="a")
    def load_a(path):
        return open(path).read()

    @step(name="load_b", version="1", source="b")
    def load_b(path):
        return open(path).read()

    pipe = pipeline(
        id="ms", name="ms",
        sources={"a": FolderSource(DIR_A), "b": FolderSource(DIR_B)},
        steps=[load_a, load_b],
    )
    with pytest.raises(ValueError, match="single-source"):
        run(pipe, source=FolderSource(DIR_A), workers=1)
