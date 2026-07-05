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
    
def test_table_source_disambiguate_duplicates():
    # We use key="name" so we can insert duplicate names without violating the PK on id.
    
    # Insert a duplicate key with DIFFERENT content
    execute_remote("INSERT INTO leads (id, name, score) VALUES (3, 'Bob', 7.5)")
    
    source = TableSource(REMOTE_DB_PATH, table="leads", key="name")
    items = source.scan()
    
    # 3 items: Alice, Bob#hash, Bob#hash2
    assert len(items) == 3
    
    bobs = [it for it in items if it.coordinate.startswith("Bob#")]
    assert len(bobs) == 2
    assert bobs[0].metadata.get("key_collision") is True
    assert bobs[1].metadata.get("key_collision") is True
    
    # Insert a duplicate key with EXACT SAME content
    # (Notice we have a different id=4, but if key="name" and we only hash some columns?
    # Wait, the whole row is hashed! Since id is in the row, the content hash will be DIFFERENT!
    # So to test exact same content, we must use columns=["name", "score"] in TableSource!)
    
    source_cols = TableSource(REMOTE_DB_PATH, table="leads", key="name", columns=["name", "score"])
    
    # Insert a row that has exactly the same name and score as Alice
    execute_remote("INSERT INTO leads (id, name, score) VALUES (4, 'Alice', 9.5)")
    
    items2 = source_cols.scan()
    # 3 items: Alice (collapsed), Bob#hash, Bob#hash2
    assert len(items2) == 3
    alices = [it for it in items2 if it.coordinate == "Alice"]
    assert len(alices) == 1
    
def test_credential_stripping():
    # Provide a URL with credentials
    url = "postgresql://user:secretpass@localhost:5432/mydb"
    source = TableSource(url, table="leads", key="id")
    
    assert "user" not in source.id
    assert "secretpass" not in source.id
    assert source.id == "table:postgresql://localhost:5432/mydb/leads#key=id"
    
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


def test_streaming_duplicate_key_load_picks_by_content():
    execute_remote("INSERT INTO leads (id, name, score) VALUES (3, 'Bob', 7.5)")
    src = TableSource(
        REMOTE_DB_PATH, table="leads", key="name", columns=["name", "score"], batch_size=1
    )
    items = src.scan()
    bobs = [it for it in items if it.coordinate.startswith("Bob#")]
    assert len(bobs) == 2

    loaded = [src.load(it) for it in bobs]
    assert sorted(r["score"] for r in loaded) == [7.5, 8.0]


def test_streaming_load_after_delete_raises():
    src = TableSource(REMOTE_DB_PATH, table="leads", key="id", batch_size=1)
    items = src.scan()
    execute_remote("DELETE FROM leads WHERE id = 1")
    alice = next(it for it in items if it.coordinate == "1")
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
