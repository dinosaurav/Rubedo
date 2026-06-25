import sys
import json
import traceback
from datetime import datetime, timezone
from batchbrain.db import get_session
from batchbrain.models import ExecutionRequest
from batchbrain.registry import get_processor
from batchbrain.api import process

def run_worker(execution_id: str):
    with get_session() as session:
        req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
        if not req:
            print(f"Execution request {execution_id} not found.")
            sys.exit(1)
        
        req.status = "running"
        req.started_at = datetime.now(timezone.utc).isoformat()
        session.commit()
        
        processor_id = req.processor_id
        input_json_str = req.input_json
        force = bool(req.force)
        folder_override = req.folder_override
        workers_override = req.workers_override
        
    try:
        spec = get_processor(processor_id)
        input_dict = json.loads(input_json_str) if input_json_str else {}
        
        if spec.input_model:
            validated_inputs = spec.input_model.model_validate(input_dict)
            validated_json = validated_inputs.model_dump(mode="json")
            bound_fn = lambda path: spec.fn(path, validated_inputs)
        else:
            validated_json = {}
            bound_fn = spec.fn
            
        effective_folder = folder_override if folder_override and spec.allow_folder_override else spec.folder
        if folder_override and not spec.allow_folder_override:
            raise ValueError(f"Processor '{processor_id}' does not allow folder overrides.")
            
        effective_workers = workers_override if workers_override is not None else spec.workers
        
        effective_config = {
            "processor_id": spec.id,
            "processor_static_config": spec.config or {},
            "processor_inputs": validated_json
        }
        
        summary = process(
            folder=effective_folder,
            fn=bound_fn,
            code_version=spec.code_version,
            config=effective_config,
            step=spec.step,
            workers=effective_workers,
            force=force
        )
        
        with get_session() as session:
            req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
            req.status = "succeeded"
            req.run_id = summary.run_id
            req.finished_at = datetime.now(timezone.utc).isoformat()
            session.commit()
            
    except Exception as e:
        err_msg = traceback.format_exc()
        with get_session() as session:
            req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
            req.status = "failed"
            req.error_message = str(e) + "\n" + err_msg
            req.finished_at = datetime.now(timezone.utc).isoformat()
            session.commit()
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m batchbrain.processor_worker <EXECUTION_ID>")
        sys.exit(1)
    run_worker(sys.argv[1])
