import os
import pytest
from fastapi.testclient import TestClient
from rubedo import step, pipeline
from rubedo.server import create_app
from conftest import isolated_test_env

TEST_FOLDER = ".test_api_data"

TEST_HOME = None
client = None


@step
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


@step(name="count-lines")
def count_lines(scan: dict):
    text = scan["text"]
    return {"text": text, "path": scan["path"]}


def make_test_pipeline():
    return pipeline(name="p-test", steps=[scan, count_lines], home=TEST_HOME)


@pytest.fixture(autouse=True)
def setup_teardown():
    global TEST_HOME, client
    with isolated_test_env("api") as env:
        TEST_HOME = env.home
        with open(os.path.join(TEST_FOLDER, "a.txt"), "w") as f:
            f.write("one\ntwo")
        with open(os.path.join(TEST_FOLDER, "b.txt"), "w") as f:
            f.write("one")
        client = TestClient(create_app(home=TEST_HOME))
        make_test_pipeline().run(workers=1)
        yield
        client = None

def test_get_runs():
    response = client.get("/api/runs")
    assert response.status_code == 200
    runs = response.json()
    assert len(runs) == 1
    run = runs[0]
    assert run["created_count"] == 4  # 2 files x (scan + count-lines)
    assert run["reused_count"] == 0
    assert run["failed_count"] == 0
    assert run["status"] == "completed"


def test_get_run_detail():
    runs = client.get("/api/runs").json()
    run_id = runs[0]["id"]

    response = client.get(f"/api/runs/{run_id}")
    assert response.status_code == 200
    run = response.json()
    assert run["id"] == run_id


def test_get_run_coordinates():
    runs = client.get("/api/runs").json()
    run_id = runs[0]["id"]

    response = client.get(f"/api/runs/{run_id}/coordinates")
    assert response.status_code == 200
    coords = response.json()
    assert len(coords) == 4  # 2 files x (scan + count-lines)
    # Coordinates are content-addressed (row-<hash>), not "a.txt"/"b.txt".
    assert coords[0]["coordinate"].startswith("row-")
    assert coords[0]["status"] == "created"


def test_get_materializations():
    response = client.get("/api/materializations?limit=10&offset=0")
    assert response.status_code == 200
    mats = response.json()
    assert len(mats) == 5  # 4 lanes + 1 root-anchor
    assert mats[0]["output_address"] is not None


def test_selection_preview():
    response = client.post(
        "/api/selection/preview",
        json={"step": "count-lines", "index": {"path": "a.txt"}},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["materialization_count"] == 1
    # metadata is {} from Arrow (metadata_json was a Materialization column,
    # now deleted; RCS.metadata_json carries the rich per-attempt data)
    assert data["items"][0]["metadata"] == {}


def test_selection_invalidate():
    response = client.post(
        "/api/selection/invalidate?reason=api test",
        json={"step": "count-lines", "index": {"path": "a.txt"}},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["invalidated_count"] == 1
    assert len(data["addresses"]) == 1


def test_current_outputs_one_row_per_step_and_lane():
    """TODO 30: a two-step chain (scan -> count-lines) must surface a
    current-output row for *each* step's coordinate, not collapse down
    to the deepest step's row per lane."""
    response = client.get("/api/current-outputs")
    assert response.status_code == 200
    outputs = response.json()

    # 2 files x (scan + count-lines) = 4 rows, not 2 (one per lane).
    assert len(outputs) == 4

    step_names = {row["step_name"] for row in outputs}
    assert step_names == {"scan", "count-lines"}

    # No two rows share the same (pipeline_id, step_name, source_id, coordinate).
    keys = [
        (row["pipeline_id"], row["step_name"], row["source_id"], row["coordinate"])
        for row in outputs
    ]
    assert len(keys) == len(set(keys))

    # Each coordinate (lane) appears once for scan and once for count-lines.
    coords_by_step: dict = {}
    for row in outputs:
        coords_by_step.setdefault(row["step_name"], set()).add(row["coordinate"])
    assert coords_by_step["scan"] == coords_by_step["count-lines"]
    assert len(coords_by_step["scan"]) == 2


def test_current_outputs_no_collision_across_pipelines():
    """Two pipelines sharing the same root step (so identical content-addressed
    lane coordinates) must not collide in current-outputs — each pipeline's
    rows must survive independently."""
    other_pipeline = pipeline(name="p-test-2", steps=[scan, count_lines], home=TEST_HOME)
    other_pipeline.run(workers=1)

    response = client.get("/api/current-outputs")
    assert response.status_code == 200
    outputs = response.json()

    pipeline_ids = {row["pipeline_id"] for row in outputs}
    assert pipeline_ids == {"p-test", "p-test-2"}

    # 2 pipelines x 2 files x (scan + count-lines) = 8 rows total.
    assert len(outputs) == 8

    rows_per_pipeline: dict = {}
    for row in outputs:
        rows_per_pipeline.setdefault(row["pipeline_id"], []).append(row)
    assert len(rows_per_pipeline["p-test"]) == 4
    assert len(rows_per_pipeline["p-test-2"]) == 4
