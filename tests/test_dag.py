import pytest
import os
import shutil
from rubedo.db import init_db, get_session
from rubedo.store import init_store, read_materialization_output
from rubedo.spec import step, pipeline
from rubedo.runner import run, topological_sort
from rubedo.models import RunCoordinateStatus, Materialization, MaterializationEdge
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

    os.environ["RUBEDO_STORE_DIR"] = f"{DB_FOLDER}/store"
    # patch store dirs
    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{DB_FOLDER}/store/objects"
    rubedo.store.STAGING_DIR = f"{DB_FOLDER}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    # Need to make sure the engine is created with StaticPool for in-memory shared
    from sqlalchemy.pool import StaticPool
    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    init_store()

    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)
    os.makedirs(TEST_FOLDER, exist_ok=True)

    yield

    # Teardown
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)


def create_file(name, content):
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


@step(name="scan", version="1", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content — the
    replacement for the old folder=TEST_FOLDER source sugar (TODO 14)."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def coordinate_for_path(step_name, path_value):
    """The migrated coordinate is a content hash (row-<hash>), not the
    literal filename (TODO 14). Recover it by scanning that step's live
    materializations for the one whose payload carries this path."""
    with get_session() as session:
        for rc in session.query(RunCoordinateStatus).filter_by(step_name=step_name).all():
            mat = session.get(Materialization, rc.materialization_id)
            if mat is not None and read_materialization_output(mat).get("path") == path_value:
                return rc.coordinate
    return None


def test_topological_sort():
    @step(name="a", version="1", depends_on=["scan"])
    def a(scan):
        pass

    @step(name="b", version="1", depends_on=["a"])
    def b(a):
        pass

    @step(name="c", version="1", depends_on=["b"])
    def c(b):
        pass

    p = pipeline(id="p1", name="p1", steps=[scan, a, b, c])
    topo = topological_sort(p)
    assert [s.name for s in topo] == ["scan", "a", "b", "c"]


def test_linear_dag():
    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"].strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipe = pipeline(id="p1", name="p1", steps=[scan, read, upper])

    create_file("f1.txt", "hello")
    create_file("f2.txt", "world")

    run(pipe, workers=1)

    with get_session() as session:
        # Check coordinates created
        statuses = session.query(RunCoordinateStatus).all()
        # 2 files * 3 steps (scan, read, upper) — scan's own lanes now count
        assert len(statuses) == 6

        # Check outputs. Coordinates are content hashes, not "f1.txt"
        # (TODO 14) — recover f1.txt's coordinate via its scan payload, then
        # reuse it: a simple map chain propagates the parent's coordinate
        # unchanged (see planning.py's `_plan_step`).
        coord_f1 = coordinate_for_path("scan", "f1.txt")
        assert coord_f1 is not None

        rc_read = (
            session.query(RunCoordinateStatus)
            .filter_by(coordinate=coord_f1, step_name="read")
            .first()
        )
        read_f1 = session.get(Materialization, rc_read.materialization_id)
        assert read_f1 is not None

        rc_upper = (
            session.query(RunCoordinateStatus)
            .filter_by(coordinate=coord_f1, step_name="upper")
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
    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"].strip()

    pipe = pipeline(id="p2", name="p2", steps=[scan, read])

    create_file("f1.txt", "hello")
    run(pipe, workers=1)

    with get_session() as session:
        statuses = session.query(RunCoordinateStatus).all()
        assert len(statuses) == 2  # scan's lane + read's lane
        assert {s.status for s in statuses} == {"created"}

    # Run again, should be reused
    run(pipe, workers=1)

    with get_session() as session:
        # 2 from first run, 2 from second run
        statuses = (
            session.query(RunCoordinateStatus)
            .order_by(RunCoordinateStatus.id.desc())
            .limit(1)
            .all()
        )
        assert statuses[0].status == "reused"


def test_invalidate_downstream_then_rerun():
    from rubedo import Selection
    from rubedo.invalidation import invalidate

    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        return scan["text"].strip()

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipe = pipeline(id="p4", name="p4", steps=[scan, read, upper])

    create_file("f1.txt", "hello")
    run(pipe, workers=1)

    res = invalidate(Selection(step="upper"), reason="bad output")
    assert res["invalidated_count"] == 1

    # Recompute resurrects the tombstoned materialization; its lineage
    # edges already exist and must not be inserted twice
    summary = run(pipe, workers=1)
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
    # This scan recipe yields only the file's text — no "path" field — so
    # two files with identical bytes yield byte-identical payloads and
    # collapse into a single content-addressed lane (row-<hash>), per TODO
    # 14 ("identical rows collapse"). The module-level `scan` above folds
    # "path" into the payload precisely so lanes stay distinguishable; this
    # test wants the opposite to exercise collapse.
    @step(name="scan_nopath", version="1", shape="expand")
    def scan_nopath():
        for name in sorted(os.listdir(TEST_FOLDER)):
            path = os.path.join(TEST_FOLDER, name)
            if os.path.isfile(path):
                yield {"text": open(path).read()}

    @step(name="upper", version="1", depends_on=["scan_nopath"])
    def upper(scan_nopath):
        return scan_nopath["text"].strip().upper()

    pipe = pipeline(id="p5", name="p5", steps=[scan_nopath, upper])

    # Same content -> same lane -> one materialization and one lineage edge,
    # without a unique-constraint crash, even though the generator yields it
    # twice.
    create_file("f1.txt", "hello")
    create_file("f2.txt", "hello")

    summary = run(pipe, workers=1)
    assert summary.failed_count == 0

    with get_session() as session:
        mats = session.query(Materialization).filter_by(step_name="upper").all()
        assert len(mats) == 1
        edges = (
            session.query(MaterializationEdge).filter_by(child_id=mats[0].id).all()
        )
        assert len(edges) == 1


def test_dag_blocked_on_failure():
    @step(name="read", version="1", depends_on=["scan"])
    def read(scan):
        raise ValueError("Boom")

    @step(name="upper", version="1", depends_on=["read"])
    def upper(read):
        return read.upper()

    pipe = pipeline(id="p3", name="p3", steps=[scan, read, upper])

    create_file("f1.txt", "hello")

    run(pipe, workers=1)

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
