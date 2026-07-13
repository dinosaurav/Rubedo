import os
import tempfile
import pytest
from rubedo import step, pipeline
from rubedo.db import get_session, init_db
import rubedo.db as db
from rubedo.models import (
    Base,
    Run,
    RunCoordinateStatus,
    Materialization,
    MaterializationIndexEntry,
)
from rubedo.selection import Selection
from rubedo.invalidation import invalidate
import uuid
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


# Folder recipe (TODO 14): a root expand step that walks "test_input" and
# yields each file's content — the replacement for the old
# folder="test_input" source sugar. Indexed on `path` so tests can still
# find "the lane for a.txt" without the coordinate being that literal
# string (coordinates are content-addressed: row-<hash>).
@step(name="scan", version="1", shape="expand", index=["path"])
def scan():
    for name in sorted(os.listdir("test_input")):
        path = os.path.join("test_input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path, encoding="utf-8").read()}


# A simple processor function for tests
@step(name="count-lines", version="v1", depends_on=["scan"])
def count_lines(scan: dict) -> dict:
    text = scan["text"]
    lines = text.split("\n")
    return {"text": text, "line_count": len(lines), "empty": len(text) == 0}


test_pipeline = pipeline(name="p-test", steps=[scan, count_lines])


def _coord_for_path(session, run_id, step_name, filename):
    """The coordinate a given run minted for `filename`, found via the
    `scan` step's indexed `path` field — coordinates are row-<hash>, not
    the filename itself, and a dependent 1:1 map step shares its parent's
    coordinate, so this resolves either "scan" or "count-lines" lanes."""
    scan_rows = (
        session.query(RunCoordinateStatus)
        .filter_by(run_id=run_id, step_name="scan")
        .filter(RunCoordinateStatus.materialization_id.isnot(None))
        .all()
    )
    for rc in scan_rows:
        hit = (
            session.query(MaterializationIndexEntry)
            .filter_by(
                materialization_id=rc.materialization_id,
                field="path",
                value=filename,
            )
            .first()
        )
        if hit:
            return rc.coordinate
    return None


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
    res = test_pipeline.run(workers=1)
    assert res.run_id is not None

    with get_session() as session:
        run_row = session.query(Run).filter_by(id=res.run_id).first()
        assert run_row.status == "completed"

        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 4  # 2 files x (scan lane + count-lines lane)
        for c in coords:
            assert c.status == "created"

        mats = session.query(Materialization).all()
        assert len(mats) == 4


def test_second_run_reuses_all():
    test_pipeline.run(workers=1)
    res2 = test_pipeline.run(workers=1)

    with get_session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 4
        for c in coords:
            assert c.status == "reused"


def test_edit_one_file_recreates_one():
    test_pipeline.run(workers=1)

    with open("test_input/a.txt", "w") as f:
        f.write("one\ntwo\nthree")

    res2 = test_pipeline.run(workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res2.run_id, "count-lines", "a.txt")
        b_coord = _coord_for_path(session, res2.run_id, "count-lines", "b.txt")
        assert coords[a_coord].status == "created"
        assert coords[b_coord].status == "reused"


def test_change_code_version_recreates_all():
    test_pipeline.run(workers=1)

    @step(name="count-lines", version="v2", depends_on=["scan"])
    def count_lines_v2(scan: dict) -> dict:
        return {"ok": True}

    p_v2 = pipeline(name="p-test", steps=[scan, count_lines_v2])

    res2 = p_v2.run(workers=1)

    with get_session() as session:
        coords = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        )
        assert len(coords) == 2
        for c in coords:
            assert c.status == "created"


def test_failure_creates_no_materialization():
    # Make a file unreadable or raise an error
    @step(name="count-lines", version="v1", depends_on=["scan"])
    def failing_processor(scan: dict) -> dict:
        if scan["path"] == "a.txt":
            raise Exception("Failure in a.txt")
        return {"ok": True}

    p_fail = pipeline(name="p-fail", steps=[scan, failing_processor])
    res = p_fail.run(workers=1)

    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res.run_id, "count-lines", "a.txt")
        b_coord = _coord_for_path(session, res.run_id, "count-lines", "b.txt")
        assert coords[a_coord].status == "failed"
        assert coords[b_coord].status == "created"

        # Ensure no materialization was created for a.txt (only b.txt's
        # count-lines output — the 2 scan lanes always materialize)
        mats = (
            session.query(Materialization)
            .filter_by(step_name="count-lines")
            .all()
        )
        assert len(mats) == 1


def test_select_by_coordinate_glob():
    test_pipeline.run(workers=1)

    sel = Selection(source_id="scan", coordinate_glob="row-*")
    from rubedo.selection import get_selection_materialization_ids

    with get_session() as session:
        mat_ids = get_selection_materialization_ids(session, sel)
        mats = (
            session.query(Materialization).filter(Materialization.id.in_(mat_ids)).all()
        )
        # every live materialization's latest coordinate matches row-* —
        # both scan's own lanes and count-lines' (which shares its
        # parent's coordinate, a 1:1 dependent map)
        assert len(mats) == 4
        assert all(m.input_hash is not None for m in mats)


def test_invalidate_selected():
    res1 = test_pipeline.run(workers=1)

    with get_session() as session:
        b_coord = _coord_for_path(session, res1.run_id, "count-lines", "b.txt")
    sel = Selection(coordinate_glob=b_coord, step="count-lines")
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
    res1 = test_pipeline.run(workers=1)

    with get_session() as session:
        b_coord = _coord_for_path(session, res1.run_id, "count-lines", "b.txt")
    sel = Selection(coordinate_glob=b_coord, step="count-lines")
    invalidate(sel, "test")

    res2 = test_pipeline.run(workers=1)
    with get_session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res2.run_id, "count-lines", "a.txt")
        assert coords[a_coord].status == "reused"
        assert (
            coords[b_coord].status == "created"
        )  # Recomputed because it was invalidated


def test_logical_deletion():
    # 1. First run, create files
    res1 = test_pipeline.run(workers=1)
    assert res1.created_count == 4  # 2 files x (scan lane + count-lines lane)
    assert res1.reused_count == 0

    with db.get_session() as session:
        mats = session.query(Materialization).all()
        assert len(mats) == 4

    # 2. Delete one file
    os.remove("test_input/a.txt")

    # 3. Second run: a.txt simply isn't scanned — there is no "removed"
    #    bookkeeping. "Current" is just the latest run's lanes.
    res2 = test_pipeline.run(workers=1)
    assert res2.created_count == 0
    assert res2.reused_count == 2  # only b.txt's scan + count-lines lanes

    with db.get_session() as session:
        # This run touched only b.txt — no status row for the vanished a.txt.
        run_coords = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        )
        assert len(run_coords) == 1
        assert run_coords[0].status == "reused"

        # a.txt's materialization is untouched and still live — a re-add reuses it.
        mats = session.query(Materialization).all()
        assert len(mats) == 4
        for m in mats:
            assert m.is_live


def test_restore_deleted_reuses_cache():
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    test_pipeline.run(workers=1)
    os.remove("test_input/a.txt")
    test_pipeline.run(workers=1)

    # Restore file with exact same content
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    # Third run should REUSE, not create
    res3 = test_pipeline.run(workers=1)
    assert res3.created_count == 0
    assert res3.reused_count == 4  # a.txt and b.txt, scan + count-lines each
