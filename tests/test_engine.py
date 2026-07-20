import os
import tempfile
import pytest
from rubedo import step, pipeline
from rubedo.models import (
    Run,
    RunCoordinateStatus,
    InputHashUsage,
)
from rubedo.selection import Selection
from rubedo.invalidation import invalidate
from conftest import make_home

TEST_HOME = None


# Folder recipe: a root expand step that walks "test_input" and yields each
# file's content. The `path` field in the output lets tests find "the lane
# for a.txt" without the coordinate being that literal string (coordinates
# are content-addressed: row-<hash>).
@step(check_cache=False)
def scan():
    for name in sorted(os.listdir("test_input")):
        path = os.path.join("test_input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path, encoding="utf-8").read()}


# A simple processor function for tests
@step(name="count-lines", version="v1")
def count_lines(scan: dict) -> dict:
    text = scan["text"]
    lines = text.split("\n")
    return {"text": text, "line_count": len(lines), "empty": len(text) == 0}


def make_test_pipeline():
    return pipeline(name="p-test", steps=[scan, count_lines], home=TEST_HOME)


def _coord_for_path(session, run_id, step_name, filename):
    """The coordinate a given run minted for `filename`, found via the
    `scan` step's `path` output field — coordinates are row-<hash>, not
    the filename itself, and a dependent 1:1 map step shares its parent's
    coordinate, so this resolves either "scan" or "count-lines" lanes."""
    rows = (
        session.query(RunCoordinateStatus)
        .filter(RunCoordinateStatus.run_id == run_id, RunCoordinateStatus.step_name == "scan")
        .filter(RunCoordinateStatus.output_address.isnot(None))
        .all()
    )
    addr_index = TEST_HOME.lanes.address_row_index()
    for rc in rows:
        row = addr_index.get(str(rc.output_address))
        if row is None:
            continue
        output = row.get("output")
        if isinstance(output, dict) and output.get("path") == filename:
            return rc.coordinate
    return None


@pytest.fixture(autouse=True)
def setup_teardown():
    global TEST_HOME
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)

    # Create input dir
    os.makedirs("test_input", exist_ok=True)
    with open("test_input/a.txt", "w") as f:
        f.write("one\ntwo")
    with open("test_input/b.txt", "w") as f:
        f.write("one")

    TEST_HOME = make_home(os.path.join(temp_dir, ".rubedo"))
    yield

    # Teardown
    os.chdir(orig_dir)


def test_first_run_creates_all():
    res = make_test_pipeline().run(workers=1)
    assert res.run_id is not None

    with TEST_HOME.session() as session:
        run_row = session.query(Run).filter_by(id=res.run_id).first()
        assert run_row.status == "completed"

        coords = session.query(RunCoordinateStatus).filter_by(run_id=res.run_id).all()
        assert len(coords) == 4  # 2 files x (scan lane + count-lines lane)
        for c in coords:
            assert c.status == "created"

        assert len(TEST_HOME.lanes.all_filled_rows()) == 5  # 4 lanes + 1 root-anchor


def test_second_run_reuses_all():
    make_test_pipeline().run(workers=1)
    res2 = make_test_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        coords = session.query(RunCoordinateStatus).filter_by(run_id=res2.run_id).all()
        assert len(coords) == 4
        for c in coords:
            assert c.status == "reused"


def test_edit_one_file_recreates_one():
    make_test_pipeline().run(workers=1)

    with open("test_input/a.txt", "w") as f:
        f.write("one\ntwo\nthree")

    res2 = make_test_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res2.run_id, "count-lines", "a.txt")
        b_coord = _coord_for_path(session, res2.run_id, "count-lines", "b.txt")
        assert coords[a_coord].status == "created"
        assert coords[b_coord].status == "reused"


def test_change_code_version_recreates_all():
    make_test_pipeline().run(workers=1)

    @step(name="count-lines", version="v2")
    def count_lines_v2(scan: dict) -> dict:
        return {"ok": True}

    p_v2 = pipeline(name="p-test", steps=[scan, count_lines_v2], home=TEST_HOME)

    res2 = p_v2.run(workers=1)

    with TEST_HOME.session() as session:
        coords = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        )
        assert len(coords) == 2
        for c in coords:
            assert c.status == "created"


def test_failure_creates_no_materialization():
    # Make a file unreadable or raise an error
    @step(name="count-lines")
    def failing_processor(scan: dict) -> dict:
        if scan["path"] == "a.txt":
            raise Exception("Failure in a.txt")
        return {"ok": True}

    p_fail = pipeline(name="p-fail", steps=[scan, failing_processor], home=TEST_HOME)
    res = p_fail.run(workers=1)

    with TEST_HOME.session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res.run_id, "count-lines", "a.txt")
        b_coord = _coord_for_path(session, res.run_id, "count-lines", "b.txt")
        assert coords[a_coord].status == "failed"
        assert coords[b_coord].status == "created"

        # Ensure no materialization was created for a.txt (only b.txt's
        # count-lines output — the 2 scan lanes always materialize)
        count_lines_rows = [
            r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "count-lines"
        ]
        assert len(count_lines_rows) == 1


def test_select_by_coordinate_glob():
    make_test_pipeline().run(workers=1)

    sel = Selection(source_id="scan", coordinate_glob="row-*")
    from rubedo.selection import get_selection_addresses

    with TEST_HOME.session() as session:
        addrs = get_selection_addresses(session, sel, home=TEST_HOME)
        idx = TEST_HOME.lanes.address_row_index()
        rows = [idx[a] for a in addrs if a in idx]
        # every live materialization's latest coordinate matches row-* —
        # both scan's own lanes and count-lines' (which shares its
        # parent's coordinate, a 1:1 dependent map)
        assert len(rows) == 4
        assert all(r.get("input_hash") is not None for r in rows)


def test_invalidate_selected():
    res1 = make_test_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        b_coord = _coord_for_path(session, res1.run_id, "count-lines", "b.txt")
    sel = Selection(coordinate_glob=b_coord, step="count-lines")
    res = invalidate(sel, "test invalidation", home=TEST_HOME)

    assert res["invalidated_count"] == 1

    with TEST_HOME.session() as session:
        # Check materialization is invalidated (by address via IHU)
        from rubedo.models import InputHashUsage

        addr = res["addresses"][0]
        usage = session.query(InputHashUsage).filter_by(address=addr).first()
        assert usage is not None
        assert usage.fulfilled is False


def test_invalidated_result_not_reused():
    res1 = make_test_pipeline().run(workers=1)

    with TEST_HOME.session() as session:
        b_coord = _coord_for_path(session, res1.run_id, "count-lines", "b.txt")
    sel = Selection(coordinate_glob=b_coord, step="count-lines")
    invalidate(sel, "test", home=TEST_HOME)

    res2 = make_test_pipeline().run(workers=1)
    with TEST_HOME.session() as session:
        coords = {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        }
        a_coord = _coord_for_path(session, res2.run_id, "count-lines", "a.txt")
        assert coords[a_coord].status == "reused"
        assert (
            coords[b_coord].status == "created"
        )  # Recomputed because it was invalidated


def test_logical_deletion():
    # 1. First run, create files
    res1 = make_test_pipeline().run(workers=1)
    assert res1.created_count == 4  # 2 files x (scan lane + count-lines lane)
    assert res1.reused_count == 0

    assert len(TEST_HOME.lanes.all_filled_rows()) == 5  # 4 lanes + 1 root-anchor

    # 2. Delete one file
    os.remove("test_input/a.txt")

    # 3. Second run: a.txt simply isn't scanned — there is no "removed"
    #    bookkeeping. "Current" is just the latest run's lanes.
    res2 = make_test_pipeline().run(workers=1)
    assert res2.created_count == 0
    assert res2.reused_count == 2  # only b.txt's scan + count-lines lanes

    with TEST_HOME.session() as session:
        # This run touched only b.txt — no status row for the vanished a.txt.
        run_coords = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=res2.run_id, step_name="count-lines")
            .all()
        )
        assert len(run_coords) == 1
        assert run_coords[0].status == "reused"

        # a.txt's materialization is untouched and still live — a re-add reuses it.
        rows = TEST_HOME.lanes.all_filled_rows()
        assert len(rows) == 6  # 4 original lanes + old anchor + new anchor (b.txt only)
        assert (
            session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .count()
            == 5  # 4 live lanes + 1 live anchor (old anchor demoted by IHU flip)
        )


def test_restore_deleted_reuses_cache():
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    make_test_pipeline().run(workers=1)
    os.remove("test_input/a.txt")
    make_test_pipeline().run(workers=1)

    # Restore file with exact same content
    with open("test_input/a.txt", "w") as f:
        f.write("a")

    # Third run should REUSE, not create
    res3 = make_test_pipeline().run(workers=1)
    assert res3.created_count == 0
    assert res3.reused_count == 4  # a.txt and b.txt, scan + count-lines each
