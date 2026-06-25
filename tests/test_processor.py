import os
import tempfile
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from batchbrain.registry import _REGISTRY, processor, get_processor
from batchbrain.models import ProcessResult, ExecutionRequest
from batchbrain.db import get_session, init_db, engine
from sqlalchemy.orm import close_all_sessions
from batchbrain.server import app
from pydantic import BaseModel

class MyInputs(BaseModel):
    my_val: int

@processor(
    id="test-proc",
    name="Test Proc",
    folder="some_dir",
    code_version="v1",
    input_model=MyInputs
)
def my_proc(path: str, inputs: MyInputs) -> ProcessResult:
    return ProcessResult(value={"val": inputs.my_val})

@processor(
    id="no-inputs",
    name="No Inputs",
    folder="some_dir",
    code_version="v1"
)
def no_inputs_proc(path: str) -> ProcessResult:
    return ProcessResult(value={"ok": True})

client = TestClient(app)

from batchbrain import db

@pytest.fixture(autouse=True)
def isolated_db():
    with tempfile.TemporaryDirectory() as tmp:
        old_cwd = os.getcwd()
        os.chdir(tmp)
        db.init_db()
        yield
        close_all_sessions()
        if db.engine:
            db.engine.dispose()
        os.chdir(old_cwd)

def test_list_processors():
    res = client.get("/api/processors")
    assert res.status_code == 200
    data = res.json()
    ids = [p["id"] for p in data]
    assert "test-proc" in ids
    assert "no-inputs" in ids

def test_run_processor_invalid_input():
    res = client.post("/api/processors/test-proc/run", json={"inputs": {"my_val": "not-an-int"}})
    assert res.status_code == 400
    assert "Invalid inputs" in res.json()["detail"]

@patch("subprocess.Popen")
def test_run_processor_success(mock_popen):
    res = client.post("/api/processors/test-proc/run", json={"inputs": {"my_val": 42}})
    assert res.status_code == 200
    data = res.json()
    assert "execution_id" in data
    assert data["status"] == "queued"
    
    mock_popen.assert_called_once()
    
    with get_session() as session:
        ex = session.query(ExecutionRequest).filter_by(id=data["execution_id"]).first()
        assert ex is not None
        assert ex.processor_id == "test-proc"
        assert ex.status == "queued"
        import json
        assert json.loads(ex.input_json) == {"my_val": 42}

@patch("subprocess.Popen")
def test_run_no_inputs(mock_popen):
    res = client.post("/api/processors/no-inputs/run", json={})
    assert res.status_code == 200

def test_get_executions():
    res = client.get("/api/executions")
    assert res.status_code == 200
