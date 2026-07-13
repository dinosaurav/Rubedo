import os
import shutil
import pytest
import uuid

from rubedo import step, pipeline, Selection
from rubedo.db import get_session, init_db
import rubedo.db as db
from rubedo.models import Base, Materialization
from rubedo.invalidation import invalidate
from sqlalchemy.pool import StaticPool
from sqlalchemy import create_engine


TEST_FOLDER = ".test_selection_data"

@pytest.fixture(autouse=True)
def setup_teardown():
    os.getcwd()
    
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)
    os.makedirs(TEST_FOLDER, exist_ok=True)

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

    with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
        f.write("a")
    with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
        f.write("b")
    with open(os.path.join(TEST_FOLDER, "c.txt"), "w") as f:
        f.write("c")

    yield

    # Teardown
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)


@step(name="scan", version="9", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content — the
    replacement for the old folder=TEST_FOLDER source sugar (TODO 14)."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def test_selection_version_range():
    # 1. Run with version 1.0.0
    @step(name="dummy", version="1.0.0", depends_on=["scan"])
    def step_v1(scan): return scan["text"]

    p1 = pipeline(name="p-test", steps=[scan, step_v1])
    p1.run(workers=1)

    # 2. Run with version 2.1.0 on a modified file
    with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
        f.write("b-mod")

    @step(name="dummy", version="2.1.0", depends_on=["scan"])
    def step_v2(scan): return scan["text"]

    p2 = pipeline(name="p-test", steps=[scan, step_v2])
    p2.run(workers=1)

    # 3. Run with unparseable version on another file
    with open(os.path.join(TEST_FOLDER, "c.txt"), "w") as f:
        f.write("c-mod")

    @step(name="dummy", version="legacy-v1", depends_on=["scan"])
    def step_legacy(scan): return scan["text"]

    p3 = pipeline(name="p-test", steps=[scan, step_legacy])
    p3.run(workers=1)

    # We now have materializations with versions: 1.0.0, 2.1.0, legacy-v1
    # (plus scan's own child lanes, version "1" throughout — untouched by
    # the version:<2.0 selection below).
    # Let's invalidate version:<2.0
    sel = Selection.parse("version:<2.0")
    res = invalidate(sel, "invalidate old")

    # It should invalidate the 1.0.0 materializations (which are for a.txt, and the old b.txt and c.txt)
    # Wait, the first run created 3 materializations for a, b, c.
    # Second run created 1 for b (since only b changed).
    # Third run created 1 for c.
    # So there are 3 mats with version 1.0.0, 1 with 2.1.0, 1 with legacy-v1.

    assert res["invalidated_count"] == 3

    with get_session() as session:
        mats = session.query(Materialization).filter(Materialization.id.in_(res["materialization_ids"])).all()
        for m in mats:
            assert m.code_version == "1.0.0"
            assert not m.is_live
