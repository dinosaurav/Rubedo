import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import CsvSource, FolderSource, step, pipeline, run
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, RunCoordinateStatus
from rubedo.store import init_store, read_materialization_output

TEST_FOLDER = ".test_sources_data"
ENV_FOLDER = ".test_sources_env"


# ---------- FolderSource ----------


def test_folder_source_scan_and_load(tmp_path):
    (tmp_path / "sub").mkdir()
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "sub" / "b.txt").write_text("world")

    src = FolderSource(str(tmp_path))
    items = {it.coordinate: it for it in src.scan()}

    assert set(items) == {"a.txt", "sub/b.txt"}
    assert items["a.txt"].content_hash != items["sub/b.txt"].content_hash
    assert items["a.txt"].metadata["size_bytes"] == 5

    payload = src.load(items["a.txt"])
    assert open(payload).read() == "hello"


def test_folder_source_id():
    assert FolderSource("examples/input").id == "folder:examples/input"


# ---------- CsvSource ----------


def write_csv(path, text):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def test_csv_source_scan_content_addressed(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n2,bob\n")

    src = CsvSource(csv_path)
    items = src.scan()

    # Every lane is content-addressed; the payload is the whole row.
    assert len(items) == 2
    assert all(it.coordinate == f"row-{it.content_hash[:12]}" for it in items)
    assert {src.load(it)["name"] for it in items} == {"alice", "bob"}
    assert src.id == f"csv:{csv_path}"


def test_csv_source_identical_rows_collapse(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n1,alice\n")

    # Same content: one indistinguishable unit of work → one lane.
    items = CsvSource(csv_path).scan()
    assert len(items) == 1


# ---------- End-to-end: caching over CSV rows ----------


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


def make_row_pipeline(csv_path):
    @step(name="parse", version="1")
    def parse(row: dict):
        return {"name": row["name"], "score": int(row["score"])}

    @step(name="grade", version="1", depends_on=["parse"])
    def grade(parse: dict):
        return {"name": parse["name"], "passed": parse["score"] >= 50}

    return pipeline(
        id="rows",
        name="Rows",
        source=CsvSource(csv_path),
        steps=[parse, grade],
    )


def test_csv_pipeline_row_level_caching():
    csv_path = os.path.join(TEST_FOLDER, "scores.csv")
    write_csv(csv_path, "id,name,score\n1,alice,80\n2,bob,40\n3,carol,60\n")
    p = make_row_pipeline(csv_path)

    # First run: 3 rows x 2 steps
    s1 = run(p, workers=1)
    assert (s1.created_count, s1.reused_count) == (6, 0)

    # Same content: everything reused
    s2 = run(p, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 6)

    # Edit one row: its content changes → its lane recomputes (both steps),
    # the others reuse. (Content-addressed: reads as the old lane removed + a
    # new one created, but only 2 outputs are computed.)
    write_csv(csv_path, "id,name,score\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s3 = run(p, workers=1)
    assert (s3.created_count, s3.reused_count) == (2, 4)

    # Insert a row at the top: content-addressed, so existing lanes don't shift
    write_csv(csv_path, "id,name,score\n4,dave,10\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s4 = run(p, workers=1)
    assert (s4.created_count, s4.reused_count) == (2, 6)

    # Dependent step saw the row payload (a dict), not a path
    with get_session() as session:
        rc = (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="grade")
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .first()
        )
        out = read_materialization_output(
            session.get(Materialization, rc.materialization_id)
        )
        assert set(out.keys()) == {"name", "passed"}

    # Remove a row: its lane simply isn't scanned this run (no removed-tracking);
    # the remaining 3 rows x 2 steps all reuse.
    write_csv(csv_path, "id,name,score\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s5 = run(p, workers=1)
    assert (s5.created_count, s5.reused_count) == (0, 6)
