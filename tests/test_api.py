import os
import tempfile
import pytest
from fastapi.testclient import TestClient

from batchbrain import process
from batchbrain.server import app
from batchbrain.db import get_session, init_db
import batchbrain.db as db
from batchbrain.models import Base, ProcessResult

client = TestClient(app)

def count_lines(path: str) -> ProcessResult:
    text = open(path, "r", encoding="utf-8").read()
    lines = text.split("\n")
    return ProcessResult(value={"text": text}, metadata={"line_count": len(lines), "empty": len(text) == 0})

@pytest.fixture(autouse=True)
def setup_teardown():
    orig_dir = os.getcwd()
    temp_dir = tempfile.mkdtemp()
    os.chdir(temp_dir)
    
    os.makedirs(".batchbrain/objects", exist_ok=True)
    init_db()
    Base.metadata.create_all(db.engine)
    
    os.makedirs("test_input", exist_ok=True)
    with open("test_input/a.txt", "w") as f: f.write("one\ntwo")
    with open("test_input/b.txt", "w") as f: f.write("one")
    
    # Run a process to populate DB
    process("test_input", count_lines, code_version="v1")
    
    yield
    
    Base.metadata.drop_all(db.engine)
    db.engine.dispose()
    os.chdir(orig_dir)

def test_get_runs():
    response = client.get("/api/runs")
    assert response.status_code == 200
    runs = response.json()
    assert len(runs) == 1
    run = runs[0]
    assert run["created_count"] == 2
    assert run["reused_count"] == 0
    assert run["failed_count"] == 0
    assert run["status"] == "succeeded"

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
    assert len(coords) == 2
    assert coords[0]["coordinate"] in ("a.txt", "b.txt")
    assert coords[0]["status"] == "created"

def test_get_materializations():
    response = client.get("/api/materializations?limit=10&offset=0")
    assert response.status_code == 200
    mats = response.json()
    assert len(mats) == 2
    assert mats[0]["output_address"] is not None

def test_get_current_outputs():
    response = client.get("/api/current-outputs")
    assert response.status_code == 200
    outputs = response.json()
    assert len(outputs) == 2
    assert outputs[0]["source_folder"] == "test_input"

def test_selection_preview():
    response = client.post("/api/selection/preview", json={
        "source_folder": "test_input",
        "coordinate_glob": "*a.txt"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["materialization_count"] == 1
    assert data["coordinate_count"] == 1
    assert data["items"][0]["metadata"]["line_count"] == 2

def test_selection_invalidate():
    response = client.post("/api/selection/invalidate", json={
        "selection": {
            "source_folder": "test_input",
            "coordinate_glob": "*a.txt"
        },
        "reason": "api test"
    })
    assert response.status_code == 200
    data = response.json()
    assert data["invalidated_count"] == 1
    assert len(data["materialization_ids"]) == 1
    
    # Verify current outputs reduced to 1
    cur_resp = client.get("/api/current-outputs")
    assert len(cur_resp.json()) == 1
