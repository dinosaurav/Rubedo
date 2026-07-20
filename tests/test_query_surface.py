import os
import uuid

from sqlalchemy.pool import StaticPool

from conftest import make_home
from rubedo import Home, Selection, pipeline, step
from rubedo.invalidation import invalidate
from rubedo.models import InputHashUsage, Run
from rubedo.util import utcnow_iso


def _write_files(root):
    data = root / "data"
    data.mkdir()
    (data / "a.txt").write_text("one\ntwo", encoding="utf-8")
    (data / "b.txt").write_text("one", encoding="utf-8")
    return data


def _pipeline(home, data, name="query-surface"):
    @step(check_cache=False)
    def scan():
        for filename in sorted(os.listdir(data)):
            path = data / filename
            if path.is_file():
                yield {"path": filename, "text": path.read_text(encoding="utf-8")}

    @step(name="count")
    def count(scan: dict):
        return {
            "path": scan["path"],
            "line_count": len(scan["text"].splitlines()),
        }

    return pipeline(name=name, steps=[scan, count], home=home)


def test_summary_cells_and_output_for_parity(tmp_path):
    home = make_home(str(tmp_path / ".rubedo"))
    data = _write_files(tmp_path)
    summary = _pipeline(home, data).run(workers=1)

    cells = summary.cells("count", resolve_output=True)
    assert {cell.status for cell in cells} == {"created"}
    assert {cell.output["path"] for cell in cells} == {"a.txt", "b.txt"}

    assert summary.output_for("count") == {
        cell.coordinate: cell.output for cell in cells
    }


def test_home_current_only_returns_fulfilled_latest_cells(tmp_path):
    home = make_home(str(tmp_path / ".rubedo"))
    data = _write_files(tmp_path)
    _pipeline(home, data, name="query-current").run(workers=1)

    invalidate(
        Selection(step="count", index={"path": "a.txt"}),
        "query surface test",
        home=home,
    )

    current = home.current(pipeline="query-current", resolve_output=True)
    assert {
        (cell.step_name, cell.output["path"])
        for cell in current
        if isinstance(cell.output, dict)
    } == {
        ("scan", "a.txt"),
        ("scan", "b.txt"),
        ("count", "b.txt"),
    }

    with home.session() as session:
        fulfilled = {
            str(row.address)
            for row in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .all()
        }
    assert {cell.output_address for cell in current} <= fulfilled


def test_home_select_finds_lane_by_output_field(tmp_path):
    home = make_home(str(tmp_path / ".rubedo"))
    data = _write_files(tmp_path)
    summary = _pipeline(home, data).run(workers=1)

    scan_cells = home.select("step:scan path:a.txt", resolve_output=True)
    assert len(scan_cells) == 1
    assert scan_cells[0].output["path"] == "a.txt"

    count_for_a = [
        cell
        for cell in summary.cells("count", resolve_output=True)
        if cell.output["path"] == "a.txt"
    ]
    assert len(count_for_a) == 1
    assert scan_cells[0].coordinate == count_for_a[0].coordinate


def test_ephemeral_homes_with_same_path_are_unshared(tmp_path):
    path = str(tmp_path / ".rubedo")
    home_a = Home.ephemeral(
        path,
        db_url=f"sqlite:///file:home_a_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true",
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
    )
    home_b = Home.ephemeral(
        path,
        db_url=f"sqlite:///file:home_b_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true",
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
    )

    assert home_a is not home_b

    with home_a.session() as session:
        session.add(
            Run(
                id="run-a",
                kind="process",
                status="completed",
                started_at=utcnow_iso(),
            )
        )
        session.commit()

    with home_b.session() as session:
        assert session.query(Run).count() == 0
