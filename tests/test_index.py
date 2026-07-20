"""Selection by output struct fields: field:value in the query language."""

import os
import shutil

import pytest

from rubedo import Selection, invalidate, step, pipeline
from rubedo.models import InputHashUsage
from conftest import make_home

TEST_FOLDER = ".test_index_data"
ENV_FOLDER = ".test_index_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    TEST_HOME = make_home(ENV_FOLDER)
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

    return pipeline(name="ix", steps=[scan, extract], home=TEST_HOME)


def test_struct_fields_are_searchable_without_declaration():
    create_file("a.txt", "acme,east,bob@x.com,ann@x.com")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(Selection(index={"company": "acme"}), reason="find acme", home=TEST_HOME)
    assert res["invalidated_count"] == 1


def test_selection_by_indexed_field():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(Selection(index={"company": "acme"}), reason="redo acme", home=TEST_HOME)
    assert res["invalidated_count"] == 1

    with TEST_HOME.session() as session:
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
    ,
        home=TEST_HOME,
    )
    assert res["invalidated_count"] == 1


def test_selection_language_end_to_end():
    create_file("a.txt", "acme,east")
    create_file("b.txt", "globex,west")
    pipe = make_pipeline()
    pipe.run(workers=1)

    res = invalidate(
        Selection.parse("step:extract company:acme live:true"), reason="via query"
    ,
        home=TEST_HOME,
    )
    assert res["invalidated_count"] == 1
