import os
import shutil
import uuid
import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import declarative_base

from rubedo import run, step, pipeline
from rubedo.db import init_db
from rubedo.sources import TableSource

TEST_FOLDER = ".test_table_data"
ENV_FOLDER = ".test_table_env"
REMOTE_DB_PATH = f"sqlite:///{os.path.abspath(ENV_FOLDER)}/remote.db"

RemoteBase = declarative_base()

class Lead(RemoteBase):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    score = Column(Float)

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

    # Init the "remote" db
    remote_engine = create_engine(REMOTE_DB_PATH)
    RemoteBase.metadata.create_all(remote_engine)
    
    # Insert some initial data
    with remote_engine.begin() as conn:
        conn.execute(
            text("INSERT INTO leads (id, name, score) VALUES (1, 'Alice', 9.5), (2, 'Bob', 8.0)")
        )
    remote_engine.dispose()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)

def execute_remote(stmt):
    engine = create_engine(REMOTE_DB_PATH)
    with engine.begin() as conn:
        conn.execute(text(stmt))
    engine.dispose()

def test_table_source_basic():
    source = TableSource(REMOTE_DB_PATH, table="leads", key="id")
    
    @step(name="process_lead", version="1")
    def process_lead(row):
        return f"Processed {row['name']} with score {row['score']}"
        
    pipe = pipeline(id="t1", name="t1", steps=[process_lead], source=source)
    summary = run(pipe, workers=1)
    
    assert summary.created_count == 2
    assert summary.reused_count == 0

def test_table_source_partial_update():
    source = TableSource(REMOTE_DB_PATH, table="leads", key="id")
    
    @step(name="process_lead", version="1")
    def process_lead(row):
        return row
        
    pipe = pipeline(id="t2", name="t2", steps=[process_lead], source=source)
    run(pipe, workers=1)
    
    # Update one row
    execute_remote("UPDATE leads SET score = 10.0 WHERE id = 1")
    
    summary2 = run(pipe, workers=1)
    assert summary2.created_count == 1  # Alice recomputed
    assert summary2.reused_count == 1   # Bob reused
    
def test_table_source_content_addressed():
    # No key: content-addressed lanes. A duplicate name with different content
    # is simply two distinct rows, never an error.
    execute_remote("INSERT INTO leads (id, name, score) VALUES (3, 'Bob', 7.5)")

    src = TableSource(REMOTE_DB_PATH, table="leads")
    items = src.scan()
    assert len(items) == 3  # Alice, Bob(8.0), Bob(7.5) — all distinct rows
    assert all(it.coordinate == f"row-{it.content_hash[:12]}" for it in items)
    assert src.id.endswith("/leads")


def test_table_source_identical_rows_collapse():
    # Under a column projection, two rows with the exact same projected content
    # are one indistinguishable unit of work → one lane.
    execute_remote("INSERT INTO leads (id, name, score) VALUES (4, 'Alice', 9.5)")

    items = TableSource(
        REMOTE_DB_PATH, table="leads", columns=["name", "score"]
    ).scan()
    # Alice (id 1 & 4 → same {name, score}) collapses; Bob distinct → 2 lanes
    assert len(items) == 2


def test_credential_stripping():
    url = "postgresql://user:secretpass@localhost:5432/mydb"
    source = TableSource(url, table="leads")

    assert "user" not in source.id
    assert "secretpass" not in source.id
    assert source.id == "table:postgresql://localhost:5432/mydb/leads"
    
def test_batch_size_must_be_positive():
    with pytest.raises(ValueError, match="batch_size must be a positive int"):
        TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=0)


def test_streaming_is_cache_equivalent_to_eager():
    # batch_size is operational, not identity: an eager run then a streaming
    # run of the same pipeline is a full cache hit.
    @step(name="p", version="1")
    def p(row):
        return {"name": row["name"], "score": row["score"]}

    eager = TableSource(REMOTE_DB_PATH, table="leads", key="id")
    s1 = run(pipeline(id="ts", name="ts", steps=[p], source=eager), workers=1)
    assert s1.created_count == 2

    streaming = TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=1)
    s2 = run(pipeline(id="ts", name="ts", steps=[p], source=streaming), workers=1)
    assert s2.created_count == 0
    assert s2.reused_count == 2


def test_streaming_executes_via_lazy_load():
    captured = []

    @step(name="p", version="1")
    def p(row):
        captured.append(row["name"])
        return dict(row)

    streaming = TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=1)
    s = run(pipeline(id="tstream", name="tstream", steps=[p], source=streaming), workers=1)
    assert s.created_count == 2
    # load() re-fetched the right rows for each lane
    assert sorted(captured) == ["Alice", "Bob"]


def test_streaming_partial_update():
    @step(name="p", version="1")
    def p(row):
        return row

    src = TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=1)
    run(pipeline(id="tsu", name="tsu", steps=[p], source=src), workers=1)

    execute_remote("UPDATE leads SET score = 10.0 WHERE id = 1")
    s2 = run(pipeline(id="tsu", name="tsu", steps=[p], source=src), workers=1)
    assert s2.created_count == 1  # Alice recomputed
    assert s2.reused_count == 1   # Bob reused


def test_streaming_load_picks_planned_content_on_later_duplicate():
    # Streaming keys are unique at scan. If a duplicate row appears *after* the
    # scan, load() still returns the row whose content matches what was
    # planned, not an arbitrary match.
    src = TableSource(
        REMOTE_DB_PATH, table="leads", key="name", columns=["name", "score"], batch_size=1
    )
    items = src.scan()  # Alice, Bob — unique names (the re-fetch key)
    bob = next(it for it in items if it.ref["name"] == "Bob")

    # A second Bob with a different score appears only now
    execute_remote("INSERT INTO leads (id, name, score) VALUES (3, 'Bob', 7.5)")

    loaded = src.load(bob)
    assert loaded["score"] == 8.0  # the planned Bob, not the newcomer


def test_streaming_load_after_delete_raises():
    src = TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=1)
    items = src.scan()
    execute_remote("DELETE FROM leads WHERE id = 1")
    alice = next(it for it in items if it.ref["id"] == 1)
    with pytest.raises(ValueError, match="gone since the scan"):
        src.load(alice)


def test_jsonable_types():
    from rubedo.sources import _jsonable
    
    row = {
        "dec": Decimal("10.5"),
        "dt": datetime.datetime(2023, 1, 1, 12, 0, 0),
        "d": datetime.date(2023, 1, 1),
        "b": b"hello",
        "i": 42,
        "s": "test"
    }
    
    j = _jsonable(row)
    assert j["dec"] == "10.5"
    assert j["dt"] == "2023-01-01T12:00:00"
    assert j["d"] == "2023-01-01"
    assert j["b"] == "68656c6c6f"
    assert j["i"] == 42
    assert j["s"] == "test"
