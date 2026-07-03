import os
import shutil
import uuid
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from batchbrain import run, step, pipeline, Filtered
from batchbrain.runner import plan
from batchbrain.db import init_db, get_session
from batchbrain.models import Materialization, MaterializationEdge, RunCoordinateStatus, RunEvent
from batchbrain.store import init_store

TEST_FOLDER = ".test_reduce_data"
ENV_FOLDER = ".test_reduce_env"

@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import batchbrain.store

    batchbrain.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    batchbrain.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["BATCHBRAIN_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import batchbrain.db

    if batchbrain.db.engine is not None:
        batchbrain.db.engine.dispose()

    from batchbrain.models import Base
    from sqlalchemy.orm import sessionmaker

    batchbrain.db.engine = create_engine(
        os.environ["BATCHBRAIN_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=batchbrain.db.engine)
    batchbrain.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=batchbrain.db.engine
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
            events = session.query(RunEvent).filter_by(run_id=summary.run_id, level="error").all()
            for e in events:
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary

def test_reduce_basic_and_lineage():
    create_file("a.txt", "10")
    create_file("b.txt", "20")
    create_file("c.txt", "30")

    @step(name="parse", version="1")
    def parse(path):
        return int(open(path).read().strip())

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(id="reduce1", name="reduce1", folder=TEST_FOLDER, steps=[parse, sum_values])
    summary = assert_run(pipe)
    
    assert summary.created_count == 4  # 3 map + 1 reduce
    
    with get_session() as session:
        mat = session.query(Materialization).filter_by(step_name="sum", is_live=True).order_by(Materialization.id.desc()).first()
        assert mat.output_address is not None
        
        edges = session.query(MaterializationEdge).filter_by(child_id=mat.id).all()
        assert len(edges) == 3

def test_reduce_caching():
    create_file("a.txt", "10")
    create_file("b.txt", "20")

    @step(name="parse", version="1")
    def parse(path):
        return int(open(path).read().strip())

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(id="reduce2", name="reduce2", folder=TEST_FOLDER, steps=[parse, sum_values])
    
    # Run 1: Create
    s1 = assert_run(pipe)
    assert s1.created_count == 3
    
    # Run 2: Reused
    s2 = assert_run(pipe)
    assert s2.reused_count == 3
    assert s2.created_count == 0
    
    # Change one file -> parse recomputes, sum recomputes
    create_file("a.txt", "15")
    s3 = assert_run(pipe)
    assert s3.reused_count == 1  # parse for b.txt
    assert s3.created_count == 2 # parse for a.txt, sum
    
    # Add a file -> parse computes, sum recomputes
    create_file("c.txt", "30")
    s4 = assert_run(pipe)
    assert s4.reused_count == 2
    assert s4.created_count == 2

def test_reduce_filtered_lane():
    create_file("a.txt", "keep:10")
    create_file("b.txt", "drop:20")
    
    @step(name="parse", version="1")
    def parse(path):
        text = open(path).read().strip()
        if text.startswith("drop"):
            return Filtered("dropped")
        return int(text.split(":")[1])

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        # parse dict should only contain a.txt
        assert "a.txt" in parse
        # b.txt is only missing when it starts with "drop"
        if len(parse) == 2:
            assert parse["b.txt"] == 20
        return sum(parse.values())
        
    pipe = pipeline(id="reduce3", name="reduce3", folder=TEST_FOLDER, steps=[parse, sum_values])
    assert_run(pipe)
    
    with get_session() as session:
        mat = session.query(Materialization).filter_by(step_name="sum", is_live=True).order_by(Materialization.id.desc()).first()
        edges = session.query(MaterializationEdge).filter_by(child_id=mat.id).all()
        # Edge only from the survived lane
        assert len(edges) == 1
        
    # Un-filter b.txt
    create_file("b.txt", "keep:20")
    s2 = assert_run(pipe)
    # b.txt parse recomputes, sum recomputes
    assert s2.created_count == 2
    assert s2.reused_count == 1
    
    with get_session() as session:
        mat2 = session.query(Materialization).filter_by(step_name="sum", is_live=True).order_by(Materialization.id.desc()).first()
        edges2 = session.query(MaterializationEdge).filter_by(child_id=mat2.id).all()
        assert len(edges2) == 2

def test_reduce_failed_parent_lane():
    create_file("a.txt", "10")
    create_file("b.txt", "fail")
    
    @step(name="parse", version="1")
    def parse(path):
        text = open(path).read().strip()
        if text == "fail":
            raise ValueError("bad data")
        return int(text)

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(id="reduce4", name="reduce4", folder=TEST_FOLDER, steps=[parse, sum_values])
    s1 = run(pipe, workers=1)
    
    assert s1.failed_count == 1
    assert s1.blocked_count == 1
    
    with get_session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="sum").one()
        assert status.status == "blocked"
        meta = json.loads(status.metadata_json)
        assert "parse:b.txt" in meta["failed_parents"]

def test_reduce_downstream_map():
    create_file("a.txt", "10")
    
    @step(name="parse", version="1")
    def parse(path):
        return int(open(path).read().strip())

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    @step(name="format", version="1", depends_on=["sum"])
    def format_val(sum):
        return f"Total: {sum}"

    pipe = pipeline(id="reduce5", name="reduce5", folder=TEST_FOLDER, steps=[parse, sum_values, format_val])
    s1 = assert_run(pipe)
    
    assert s1.created_count == 3
    
    with get_session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="format").one()
        assert status.coordinate == "@all"
        assert status.status == "created"

def test_reduce_plan():
    create_file("a.txt", "10")
    
    @step(name="parse", version="1")
    def parse(path):
        return int(open(path).read().strip())

    @step(name="sum", version="1", depends_on=["parse"], shape="reduce")
    def sum_values(parse):
        return sum(parse.values())

    pipe = pipeline(id="reduce6", name="reduce6", folder=TEST_FOLDER, steps=[parse, sum_values])
    
    p1 = plan(pipe)
    # Both steps should show as "execute" or "pending" depending on parent state
    # plan() evaluates topologically, but wait, reduce step depends on parse step, 
    # which is "execute", so reduce should be "pending"
    sum_items = [i for i in p1.items if i.step_name == "sum"]
    assert any(i.action == "pending" for i in sum_items)
    
    run(pipe, workers=1)
    
    p2 = plan(pipe)
    sum_items2 = [i for i in p2.items if i.step_name == "sum"]
    assert any(i.action == "reuse" for i in sum_items2)

def test_registration_errors():
    with pytest.raises(ValueError, match="skip_cache is meaningless with shape='reduce'"):
        @step(name="sum", version="1", depends_on=["x"], shape="reduce", skip_cache=True)
        def sum_v1(x):
            pass

    with pytest.raises(ValueError, match="shape must be 'map' or 'reduce'"):
        @step(name="sum", version="1", shape="banana")
        def sum_v2(x):
            pass
            
    with pytest.raises(ValueError, match="requires at least one parent"):
        @step(name="sum", version="1", shape="reduce")
        def sum_v3():
            pass
