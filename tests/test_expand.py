import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import FolderSource, run, step, pipeline
from rubedo.runner import plan
from rubedo.db import init_db, get_session
from rubedo.models import MaterializationEdge, RunCoordinateStatus, RunEvent
from rubedo.store import init_store

TEST_FOLDER = ".test_expand_data"
ENV_FOLDER = ".test_expand_env"


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
            events = (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            )
            for e in events:
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


# read (map) -> split (expand, one lane per line) -> shout (map)
def _read():
    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    return read


def _split():
    @step(name="split", version="1", depends_on=["read"], shape="expand")
    def split(read):
        for i, line in enumerate(read.splitlines()):
            yield str(i), {"line": line}

    return split


def _shout():
    @step(name="shout", version="1", depends_on=["split"])
    def shout(split):
        return split["line"].upper()

    return shout


def make_pipe():
    return pipeline(
        id="x",
        name="x",
        source=FolderSource(TEST_FOLDER),
        steps=[_read(), _split(), _shout()],
    )


def test_expand_mints_child_lanes_and_chains_downstream():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    s1 = assert_run(pipe)
    # read: 2 files, split: 3 minted lines, shout: 3 lines
    assert (s1.created_count, s1.reused_count) == (8, 0)

    with get_session() as session:
        shout_coords = sorted(
            r.coordinate
            for r in session.query(RunCoordinateStatus)
            .filter_by(step_name="shout", status="created")
            .all()
        )
        assert shout_coords == ["a.txt/0", "a.txt/1", "b.txt/0"]

        # Lineage: 3 read->split edges + 3 split->shout edges
        assert session.query(MaterializationEdge).count() == 6


def test_expand_reruns_but_reuses_identical_children():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    assert_run(pipe)

    # Nothing changed: expand re-executes (no MVP caching) but yields identical
    # bytes, so every materialization is reused.
    s2 = assert_run(pipe)
    assert (s2.created_count, s2.reused_count) == (0, 8)


def test_expand_caches_anchor_and_skips_fn_on_rerun():
    create_file("a.txt", "alpha\nbeta")
    calls = []

    @step(name="read", version="1")
    def read(path):
        return open(path).read()

    @step(name="split", version="1", depends_on=["read"], shape="expand")
    def split(read):
        calls.append(1)  # side effect proves whether the fn re-runs
        for i, line in enumerate(read.splitlines()):
            yield str(i), {"line": line}

    @step(name="shout", version="1", depends_on=["split"])
    def shout(split):
        return split["line"].upper()

    pipe = pipeline(
        id="c", name="c", source=FolderSource(TEST_FOLDER), steps=[read, split, shout]
    )
    assert_run(pipe)
    assert len(calls) == 1  # scraped once

    # Unchanged parent: the anchor cache-hits, children replay, fn NOT re-run.
    s2 = assert_run(pipe)
    assert len(calls) == 1
    # read(1) + 2 split children + 2 shout children, all reused
    assert (s2.created_count, s2.reused_count) == (0, 5)

    # Change the parent: anchor address moves, so the expand re-runs.
    create_file("a.txt", "alpha\nGAMMA")
    assert_run(pipe)
    assert len(calls) == 2


def test_expand_reacts_to_a_changed_source_lane():
    create_file("a.txt", "alpha\nbeta")
    create_file("b.txt", "gamma")

    pipe = make_pipe()
    assert_run(pipe)

    # Edit b.txt: only b's lane recomputes (read + its one minted line + shout);
    # a's two lanes stay reused.
    create_file("b.txt", "GAMMA")
    s = assert_run(pipe)
    assert s.created_count == 3  # read(b), split child b/0, shout b/0
    assert s.reused_count == 5  # read(a), a/0, a/1 for split and shout


def test_expand_plan_marks_downstream_pending():
    create_file("a.txt", "alpha\nbeta")
    pipe = make_pipe()
    rp = plan(pipe)
    # Downstream of an unexecuted expand can't be enumerated: it's pending.
    actions = {it.step_name: it.action for it in rp.items}
    assert actions.get("shout") == "pending"


def test_expand_duplicate_subkey_fails():
    create_file("a.txt", "x\ny")

    @step(name="dup", version="1", depends_on=["read"], shape="expand")
    def dup(read):
        yield "same", {"v": 1}
        yield "same", {"v": 2}

    pipe = pipeline(
        id="d", name="d", source=FolderSource(TEST_FOLDER), steps=[_read(), dup]
    )
    s = run(pipe, workers=1)
    assert s.failed_count == 1


def test_expand_requires_exactly_one_parent():
    with pytest.raises(ValueError, match="exactly one parent"):
        step(name="bad", version="1", shape="expand")(lambda: None)

    with pytest.raises(ValueError, match="exactly one parent"):
        step(name="bad", version="1", depends_on=["a", "b"], shape="expand")(
            lambda a, b: None
        )


def test_expand_rejects_skip_cache():
    with pytest.raises(ValueError, match="skip_cache is not supported"):
        step(name="bad", version="1", depends_on=["a"], shape="expand", skip_cache=True)(
            lambda a: None
        )
