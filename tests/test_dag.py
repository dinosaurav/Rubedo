import pytest
import os
import shutil
from batchbrain.db import init_db, get_session
from batchbrain.store import init_store
from batchbrain.registry import step, pipeline, clear_registry
from batchbrain.runner import topological_sort
from batchbrain.processor_runner import run_processor
from batchbrain.models import RunCoordinateStatus, Materialization, MaterializationEdge
import uuid
from sqlalchemy import create_engine

TEST_FOLDER = ".test_dag_data"


@pytest.fixture(autouse=True)
def setup_teardown():
    # Setup
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    if os.path.exists(abs_test_folder):
        shutil.rmtree(abs_test_folder)
    os.makedirs(abs_test_folder, exist_ok=True)

    DB_FOLDER = os.path.abspath(".test_dag_env")
    if os.path.exists(DB_FOLDER):
        shutil.rmtree(DB_FOLDER)
    os.makedirs(DB_FOLDER, exist_ok=True)

    os.environ["BATCHBRAIN_STORE_DIR"] = f"{DB_FOLDER}/store"
    # patch store dirs
    import batchbrain.store

    batchbrain.store.OBJECTS_DIR = f"{DB_FOLDER}/store/objects"
    batchbrain.store.STAGING_DIR = f"{DB_FOLDER}/store/staging"

    os.environ["BATCHBRAIN_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    # Need to make sure the engine is created with StaticPool for in-memory shared
    from sqlalchemy.pool import StaticPool
    import batchbrain.db

    if batchbrain.db.engine is not None:
        batchbrain.db.engine.dispose()

    batchbrain.db.engine = create_engine(
        os.environ["BATCHBRAIN_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    from batchbrain.models import Base
    from sqlalchemy.orm import sessionmaker

    Base.metadata.create_all(bind=batchbrain.db.engine)
    batchbrain.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=batchbrain.db.engine
    )

    init_store()
    clear_registry()

    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)
    os.makedirs(TEST_FOLDER, exist_ok=True)

    yield

    # Teardown
    clear_registry()
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)


def create_file(name, content):
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


def test_topological_sort():
    @step(name="a", version="1")
    def a(path):
        pass

    @step(name="b", version="1", depends_on=["a"])
    def b(a):
        pass

    @step(name="c", version="1", depends_on=["b"])
    def c(b):
        pass

    p = pipeline(id="p1", name="p1", folder=TEST_FOLDER, steps=[a, b, c])
    topo = topological_sort(p)
    assert [s.name for s in topo] == ["a", "b", "c"]


def test_linear_dag():
    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipeline(id="p1", name="p1", folder=TEST_FOLDER, steps=[read, upper])

    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")

    run_processor("p1", workers=1)

    with get_session() as session:
        # Check coordinates created
        statuses = session.query(RunCoordinateStatus).all()
        # Since it's a fresh DB per test, it should be exactly 4
        assert len(statuses) == 4  # 2 files * 2 steps

        # Check outputs
        rc_read = (
            session.query(RunCoordinateStatus)
            .filter_by(coordinate="f1.txt", step_name="read")
            .first()
        )
        read_f1 = session.get(Materialization, rc_read.materialization_id)
        assert read_f1 is not None

        rc_upper = (
            session.query(RunCoordinateStatus)
            .filter_by(coordinate="f1.txt", step_name="upper")
            .first()
        )
        upper_f1 = session.get(Materialization, rc_upper.materialization_id)
        assert upper_f1 is not None

        # Check edges
        edge = (
            session.query(MaterializationEdge)
            .filter_by(parent_id=read_f1.id, child_id=upper_f1.id)
            .first()
        )
        assert edge is not None


def test_cache_hit():
    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    pipeline(id="p2", name="p2", folder=TEST_FOLDER, steps=[read])

    create_file("f1.txt", "hello")
    run_processor("p2", workers=1)

    with get_session() as session:
        statuses = session.query(RunCoordinateStatus).all()
        assert len(statuses) == 1
        assert statuses[0].status == "created"

    # Run again, should be reused
    run_processor("p2", workers=1)

    with get_session() as session:
        # 1 from first run, 1 from second run
        statuses = (
            session.query(RunCoordinateStatus)
            .order_by(RunCoordinateStatus.id.desc())
            .limit(1)
            .all()
        )
        assert statuses[0].status == "reused"


def test_invalidate_downstream_then_rerun():
    from batchbrain import Selection
    from batchbrain.invalidation import invalidate

    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipeline(id="p4", name="p4", folder=TEST_FOLDER, steps=[read, upper])

    create_file("f1.txt", "hello")
    run_processor("p4", workers=1)

    res = invalidate(Selection(step="upper"), reason="bad output")
    assert res["invalidated_count"] == 1

    # Recompute resurrects the tombstoned materialization; its lineage
    # edges already exist and must not be inserted twice
    summary = run_processor("p4", workers=1)
    assert summary.created_count == 1
    assert summary.failed_count == 0

    with get_session() as session:
        mat = (
            session.query(Materialization)
            .filter_by(step_name="upper")
            .one()
        )
        assert mat.is_live
        edges = (
            session.query(MaterializationEdge).filter_by(child_id=mat.id).all()
        )
        assert len(edges) == 1


def test_duplicate_content_files_share_materialization():
    @step(name="read", version="1")
    def read(path):
        return open(path).read().strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipeline(id="p5", name="p5", folder=TEST_FOLDER, steps=[read, upper])

    # Same content -> same output addresses; both coordinates resolve to one
    # materialization and one lineage edge, without a unique-constraint crash
    create_file("f1.txt", "hello")
    create_file("f2.txt", "hello")

    summary = run_processor("p5", workers=1)
    assert summary.failed_count == 0

    with get_session() as session:
        mats = session.query(Materialization).filter_by(step_name="upper").all()
        assert len(mats) == 1
        edges = (
            session.query(MaterializationEdge).filter_by(child_id=mats[0].id).all()
        )
        assert len(edges) == 1


def test_dag_blocked_on_failure():
    @step(name="read", version="1")
    def read(path):
        raise ValueError("Boom")

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipeline(id="p3", name="p3", folder=TEST_FOLDER, steps=[read, upper])

    create_file("f1.txt", "hello")

    run_processor("p3", workers=1)

    with get_session() as session:
        rc_read = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="read")
            .order_by(RunCoordinateStatus.id.desc())
            .first()
        )
        assert rc_read.status == "failed"

        rc_upper = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="upper")
            .order_by(RunCoordinateStatus.id.desc())
            .first()
        )
        assert rc_upper.status == "blocked"
