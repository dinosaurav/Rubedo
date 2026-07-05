import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import CsvSource, FolderSource, step, pipeline, run
from rubedo.db import init_db, get_session
from rubedo.models import RunCoordinateStatus
from rubedo.store import init_store

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


def test_csv_source_scan_with_key(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n2,bob\n")

    src = CsvSource(csv_path, key="id")
    items = {it.coordinate: it for it in src.scan()}

    assert set(items) == {"1", "2"}
    assert src.load(items["1"]) == {"id": "1", "name": "alice"}
    assert src.id == f"csv:{csv_path}#key=id"


def test_csv_source_composite_key(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "region,name,v\neast,alice,1\nwest,alice,2\n")

    src = CsvSource(csv_path, key=["region", "name"])
    coords = {it.coordinate for it in src.scan()}
    assert coords == {"east|alice", "west|alice"}


def test_csv_source_duplicate_keys_disambiguate_lanes(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n1,bob\n2,carol\n")

    items = CsvSource(csv_path, key="id").scan()
    coords = sorted(it.coordinate for it in items)

    # Colliding key -> content-suffixed lanes; unique key stays clean
    assert coords[2] == "2"
    assert all(c.startswith("1#") for c in coords[:2])
    assert len(set(coords)) == 3
    collided = [it for it in items if it.coordinate != "2"]
    assert all(it.metadata.get("key_collision") for it in collided)


def test_csv_source_identical_duplicate_rows_collapse(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n1,alice\n")

    items = CsvSource(csv_path, key="id").scan()
    # Same key, same content: indistinguishable work, one lane, no suffix
    assert [it.coordinate for it in items] == ["1"]


def test_csv_source_missing_key_column_raises(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n")

    with pytest.raises(ValueError, match="key column"):
        CsvSource(csv_path, key="email").scan()


def test_csv_source_content_hash_mode(tmp_path):
    csv_path = str(tmp_path / "rows.csv")
    write_csv(csv_path, "id,name\n1,alice\n2,bob\n")

    src = CsvSource(csv_path, key=None)
    items = src.scan()
    assert all(it.coordinate == f"row-{it.content_hash[:12]}" for it in items)
    assert src.id == f"csv:{csv_path}#key=@content"


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
        source=CsvSource(csv_path, key="id"),
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

    # Edit one row: only that coordinate recomputes (both steps)
    write_csv(csv_path, "id,name,score\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s3 = run(p, workers=1)
    assert (s3.created_count, s3.reused_count) == (2, 4)

    # Insert a row at the top: coordinates are key-based, nothing shifts
    write_csv(csv_path, "id,name,score\n4,dave,10\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s4 = run(p, workers=1)
    assert (s4.created_count, s4.reused_count) == (2, 6)

    # Dependent step saw the row payload, not a path
    with get_session() as session:
        rc = (
            session.query(RunCoordinateStatus)
            .filter_by(coordinate="2", step_name="grade", status="created")
            .order_by(RunCoordinateStatus.id.desc())
            .first()
        )
        assert rc is not None

    # Remove a row: marked removed for both steps
    write_csv(csv_path, "id,name,score\n1,alice,80\n2,bob,90\n3,carol,60\n")
    s5 = run(p, workers=1)
    assert s5.removed_count == 1
    assert (s5.created_count, s5.reused_count) == (0, 6)
