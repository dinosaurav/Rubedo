import os
import tempfile
import pytest
from rubedo import step, pipeline, run
from rubedo.db import get_session, init_db
import rubedo.db as db
from rubedo.models import (
    Base,
    Run,
    RunCoordinateStatus,
    Materialization,
    Manifest,
    ManifestEntry,
)
from rubedo.selection import Selection
from rubedo.invalidation import invalidate
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


# A simple processor function for tests
@step(name="count-lines", version="v1")
def count_lines(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    text = open(path, "r", encoding="utf-8").read()
    lines = text.split("\n")
    return {"text": text, "line_count": len(lines), "empty": len(text) == 0}


test_pipeline = pipeline(
    id="p-test", name="Test Pipeline", folder="test_input", steps=[count_lines]
)


@pytest.fixture(autouse=True)
def setup_teardown():
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)

    os.makedirs(".rubedo/objects", exist_ok=True)

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    if db.engine is not None:
        db.engine.dispose()

    db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=db.engine)
    from sqlalchemy.orm import sessionmaker

    db.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db.engine)

    # Create input dir
    os.makedirs("test_input", exist_ok=True)
    with open("test_input/a.txt", "w") as f:
        f.write("one\ntwo")
    with open("test_input/b.txt", "w") as f:
        f.write("one")

    yield

    # Teardown
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()
    os.chdir(orig_dir)


def test_first_run_creates_all():
    res = run(test_pipeline, "test_input", workers=1)
    assert res.run_id is not None

    with get_session() as session:
        run_row = session.query(Run).filter_by(id=res.run_id).first()
        assert run_row.status == "completed"

        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 2
        for c in coords:
            assert c.status == "created"

        mats = session.query(Materialization).all()
        assert len(mats) == 2

        manifest = session.query(Manifest).filter_by(run_id=res.run_id).first()
        cur = session.query(ManifestEntry).filter_by(manifest_id=manifest.id).all()
        assert len(cur) == 2


def test_second_run_reuses_all():
    run(test_pipeline, "test_input", workers=1)
    res2 = run(test_pipeline, "test_input", workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 2
        for c in coords:
            assert c.status == "reused"


def test_edit_one_file_recreates_one():
    run(test_pipeline, "test_input", workers=1)

    with open("test_input/a.txt", "w") as f:
        f.write("one\ntwo\nthree")

    res2 = run(test_pipeline, "test_input", workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id)
            .all()
        }
        assert coords["a.txt"].status == "created"
        assert coords["b.txt"].status == "reused"


def test_change_code_version_recreates_all():
    run(test_pipeline, "test_input", workers=1)

    @step(name="count-lines", version="v2")
    def count_lines_v2(path: str) -> dict:
        return {"ok": True}

    p_v2 = pipeline(
        id="p-test", name="Test Pipeline", folder="test_input", steps=[count_lines_v2]
    )

    res2 = run(p_v2, "test_input", workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        for c in coords:
            assert c.status == "created"


def test_failure_creates_no_materialization():
    # Make a file unreadable or raise an error
    @step(name="count-lines", version="v1")
    def failing_processor(path: str) -> dict:
        if "a.txt" in path:
            raise Exception("Failure in a.txt")
        return {"ok": True}

    p_fail = pipeline(
        id="p-fail", name="Test", folder="test_input", steps=[failing_processor]
    )
    res = run(p_fail, "test_input", workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res.run_id)
            .all()
        }
        assert coords["a.txt"].status == "failed"
        assert coords["b.txt"].status == "created"

        # Ensure no materialization was created for a.txt
        mats = session.query(Materialization).all()
        assert len(mats) == 1


def test_select_by_coordinate_glob():
    run(test_pipeline, "test_input", workers=1)

    sel = Selection(source_id="folder:test_input", coordinate_glob="*b.txt")
    from rubedo.selection import get_selection_materialization_ids

    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, sel)
        mats = (
            session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        )
        assert len(mats) == 1
        assert mats[0].input_hash is not None  # belongs to b.txt



def test_invalidate_selected():
    run(test_pipeline, "test_input", workers=1)

    sel = Selection(source_id="folder:test_input", coordinate_glob="*b.txt")
    res = invalidate(sel, "test invalidation")

    assert res["invalidated_count"] == 1

    with get_session() as session:
        # Check materialization is invalidated
        mat = (
            session.query(Materialization)
            .filter(Materialization.id == res["materialization_ids"][0])
            .first()
        )
        assert mat.is_live is False


def test_invalidated_result_not_reused():
    run(test_pipeline, "test_input", workers=1)

    sel = Selection(source_id="folder:test_input", coordinate_glob="*b.txt")
    invalidate(sel, "test")

    res2 = run(test_pipeline, "test_input", workers=1)
    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id)
            .all()
        }
        assert coords["a.txt"].status == "reused"
        assert (
            coords["b.txt"].status == "created"
        )  # Recomputed because it was invalidated


def test_logical_deletion():
    # 1. First run, create files
    res1 = run(test_pipeline, "test_input", workers=1)
    assert res1.created_count == 2
    assert res1.reused_count == 0
    assert res1.removed_count == 0

    # Verify current outputs has 2 items
    with db.get_session() as session:
        manifest = session.query(Manifest).filter_by(run_id=res1.run_id).first()
        currents = session.query(ManifestEntry).filter_by(manifest_id=manifest.id).all()
        assert len(currents) == 2

        # Verify materializations
        mats = session.query(Materialization).all()
        assert len(mats) == 2

    # 2. Delete one file
    os.remove("test_input/a.txt")

    # 3. Second run
    res2 = run(test_pipeline, "test_input", workers=1)
    assert res2.created_count == 0
    assert res2.reused_count == 1
    assert res2.removed_count == 1

    with db.get_session() as session:
        # Current outputs should drop to 1
        manifest = session.query(Manifest).filter_by(run_id=res2.run_id).first()
        currents = session.query(ManifestEntry).filter_by(manifest_id=manifest.id).all()
        assert len(currents) == 1
        assert currents[0].coordinate == "b.txt"

        # Run coordinates should have 1 reused and 1 removed
        run_coords = (
            session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        )
        assert len(run_coords) == 2
        statuses = {rc.coordinate: rc.status for rc in run_coords}
        assert statuses["a.txt"] == "removed"
        assert statuses["b.txt"] == "reused"

        # Materialization for a.txt is STILL there and valid
        mats = session.query(Materialization).all()
        assert len(mats) == 2
        for m in mats:
            assert m.is_live


def test_restore_deleted_reuses_cache():
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    run(test_pipeline, "test_input", workers=1)
    os.remove("test_input/a.txt")
    run(test_pipeline, "test_input", workers=1)

    # Restore file with exact same content
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    # Third run should REUSE, not create
    res3 = run(test_pipeline, "test_input", workers=1)
    assert res3.created_count == 0
    assert res3.reused_count == 2  # a.txt and b.txt
    assert res3.removed_count == 0
