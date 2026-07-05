"""@step(index=[...]): labels are data someone chose to index."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, MaterializationIndexEntry
from rubedo.store import init_store

TEST_FOLDER = ".test_index_data"
ENV_FOLDER = ".test_index_env"


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


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def make_pipeline():
    @step(name="extract", version="1", index=["company", "contacts", "meta.region"])
    def extract(path):
        text = open(path).read().strip()
        company, region, *contacts = text.split(",")
        return {
            "company": company,
            "contacts": contacts,
            "meta": {"region": region},
            "body": text,
        }

    return pipeline(id="ix", name="ix", folder=TEST_FOLDER, steps=[extract])


def entries():
    with get_session() as session:
        return [
            (e.field, e.value)
            for e in session.query(MaterializationIndexEntry)
            .order_by(MaterializationIndexEntry.id)
            .all()
        ]


def test_declared_fields_are_extracted():
    create_file("a.txt", "acme,east,bob@x.com,ann@x.com")
    pipe = make_pipeline()
    run(pipe, workers=1)

    assert sorted(entries()) == [
        ("company", "acme"),
        ("contacts", "ann@x.com"),  # list field: one entry per element
        ("contacts", "bob@x.com"),
        ("meta.region", "east"),  # dotted path into nested dicts
    ]


def test_selection_by_indexed_field():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    run(pipe, workers=1)

    res = invalidate(Selection(index={"company": "acme"}), reason="redo acme")
    assert res["invalidated_count"] == 1

    with get_session() as session:
        dead = session.query(Materialization).filter_by(is_live=False).one()
        assert ("company", "acme") in [
            (e.field, e.value)
            for e in session.query(MaterializationIndexEntry)
            .filter_by(materialization_id=dead.id)
            .all()
        ]


def test_selection_index_pairs_are_anded():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "acme,west")
    pipe = make_pipeline()
    run(pipe, workers=1)

    res = invalidate(
        Selection(index={"company": "acme", "meta.region": "west"}), reason="one"
    )
    assert res["invalidated_count"] == 1


def test_reuse_does_not_duplicate_entries():
    create_file("a.txt", "acme,east")
    pipe = make_pipeline()
    run(pipe, workers=1)
    run(pipe, workers=1)  # full cache hit

    assert len(entries()) == 2  # company + meta.region, once


def test_missing_fields_are_skipped():
    create_file("a.txt", "acme,east")

    @step(name="extract", version="1", index=["company", "nonexistent", "meta.nope"])
    def extract(path):
        return {"company": "acme", "meta": {}}

    pipe = pipeline(id="ix2", name="ix2", folder=TEST_FOLDER, steps=[extract])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 0
    assert entries() == [("company", "acme")]


@pytest.mark.filterwarnings("ignore:Step 'extract' source code changed")
def test_index_declaration_is_not_cache_identity():
    create_file("a.txt", "acme,east")

    @step(name="extract", version="1")
    def extract_v1(path):
        return {"company": "acme"}

    pipe = pipeline(id="ix3", name="ix3", folder=TEST_FOLDER, steps=[extract_v1])
    run(pipe, workers=1)

    # Same step, index added: purely operational, so still a cache hit —
    # and (documented) the existing materialization gains no entries
    @step(name="extract", version="1", index=["company"])
    def extract_v2(path):
        return {"company": "acme"}

    pipe = pipeline(id="ix3", name="ix3", folder=TEST_FOLDER, steps=[extract_v2])
    summary = run(pipe, workers=1)
    assert summary.reused_count == 1
    assert entries() == []


def test_skip_cache_rejects_index():
    with pytest.raises(ValueError, match="index is meaningless"):

        @step(name="u", version="1", skip_cache=True, index=["x"])
        def u(path):
            pass


def test_selection_language_end_to_end():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    run(pipe, workers=1)

    res = invalidate(
        Selection.parse("step:extract company:acme live:true"), reason="via query"
    )
    assert res["invalidated_count"] == 1
