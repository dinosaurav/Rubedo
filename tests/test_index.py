"""Selection by output struct fields: field:value in the query language."""

import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import InputHashUsage
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


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def make_pipeline():
    @step
    def extract(scan: dict):
        text = scan["text"].strip()
        company, region, *contacts = text.split(",")
        return {
            "company": company,
            "contacts": contacts,
            "meta": {"region": region},
            "body": text,
        }

    return pipeline(name="ix", steps=[scan, extract])


def test_struct_fields_are_searchable_without_declaration():
    create_file("a.txt", "acme,east,bob@x.com,ann@x.com")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(Selection(index={"company": "acme"}), reason="find acme")
    assert res["invalidated_count"] == 1


def test_selection_by_indexed_field():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(Selection(index={"company": "acme"}), reason="redo acme")
    assert res["invalidated_count"] == 1

    with get_session() as session:
        unfulfilled = (
            session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(False))
            .all()
        )
        assert len(unfulfilled) == 1


def test_selection_index_pairs_are_anded():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "acme,west")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(
        Selection(index={"company": "acme", "meta.region": "west"}), reason="one"
    )
    assert res["invalidated_count"] == 1


def test_selection_language_end_to_end():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(
        Selection.parse("step:extract company:acme live:true"), reason="via query"
    )
    assert res["invalidated_count"] == 1
