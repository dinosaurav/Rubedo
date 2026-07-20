import os
import shutil
import pytest

from rubedo import step, pipeline, Selection
from rubedo.invalidation import invalidate
from conftest import make_home


TEST_FOLDER = ".test_selection_data"

TEST_HOME = None

@pytest.fixture(autouse=True)
def setup_teardown():
    global TEST_HOME
    os.getcwd()
    
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)
    os.makedirs(TEST_FOLDER, exist_ok=True)

    with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
        f.write("a")
    with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
        f.write("b")
    with open(os.path.join(TEST_FOLDER, "c.txt"), "w") as f:
        f.write("c")

    TEST_HOME = make_home(".test_home_env")
    yield

    # Teardown
    if os.path.exists(TEST_FOLDER):
        shutil.rmtree(TEST_FOLDER)


@step(name="scan", version="9", shape="expand")
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def test_selection_version_range():
    # 1. Run with version 1.0.0
    @step(name="dummy", version="1.0.0", depends_on=["scan"])
    def step_v1(scan): return scan["text"]

    p1 = pipeline(name="p-test", steps=[scan, step_v1], home=TEST_HOME)
    p1.run(workers=1)

    # 2. Run with version 2.1.0 on a modified file
    with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
        f.write("b-mod")

    @step(name="dummy", version="2.1.0", depends_on=["scan"])
    def step_v2(scan): return scan["text"]

    p2 = pipeline(name="p-test", steps=[scan, step_v2], home=TEST_HOME)
    p2.run(workers=1)

    # 3. Run with unparseable version on another file
    with open(os.path.join(TEST_FOLDER, "c.txt"), "w") as f:
        f.write("c-mod")

    @step(name="dummy", version="legacy-v1", depends_on=["scan"])
    def step_legacy(scan): return scan["text"]

    p3 = pipeline(name="p-test", steps=[scan, step_legacy], home=TEST_HOME)
    p3.run(workers=1)

    # We now have materializations with versions: 1.0.0, 2.1.0, legacy-v1
    # (plus scan's own child lanes, version "1" throughout — untouched by
    # the version:<2.0 selection below).
    # Let's invalidate version:<2.0
    sel = Selection.parse("version:<2.0")
    res = invalidate(sel, "invalidate old", home=TEST_HOME)

    # It should invalidate the 1.0.0 materializations (which are for a.txt, and the old b.txt and c.txt)
    # Wait, the first run created 3 materializations for a, b, c.
    # Second run created 1 for b (since only b changed).
    # Third run created 1 for c.
    # So there are 3 mats with version 1.0.0, 1 with 2.1.0, 1 with legacy-v1.

    assert res["invalidated_count"] == 3

    # All invalidated addresses should have fulfilled=False in IHU
    from rubedo.models import InputHashUsage

    with TEST_HOME.session() as session:
        for addr in res["addresses"]:
            usage = session.query(InputHashUsage).filter_by(address=addr).first()
            assert usage is not None
            assert usage.fulfilled is False
