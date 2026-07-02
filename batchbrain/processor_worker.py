import sys
import json
import traceback
from batchbrain.util import utcnow_iso
from batchbrain.db import get_session
from batchbrain.models import ExecutionRequest


def run_worker(execution_id: str):
    with get_session() as session:
        req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
        if not req:
            print(f"Execution request {execution_id} not found.")
            sys.exit(1)

        req.status = "running"
        req.started_at = utcnow_iso()
        session.commit()

        processor_id = req.processor_id
        input_json_str = req.input_json
        force = bool(req.force)
        folder_override = req.folder_override
        workers_override = req.workers_override

    try:
        from batchbrain.processor_runner import run_processor

        input_dict = json.loads(input_json_str) if input_json_str else {}

        summary = run_processor(
            processor_id=processor_id,
            inputs=input_dict,
            force=force,
            folder=folder_override,
            workers=workers_override,
        )

        with get_session() as session:
            req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
            req.status = "succeeded"
            req.run_id = summary.run_id
            req.finished_at = utcnow_iso()
            session.commit()

    except Exception as e:
        err_msg = traceback.format_exc()
        with get_session() as session:
            req = session.query(ExecutionRequest).filter_by(id=execution_id).first()
            req.status = "failed"
            req.error_message = str(e) + "\n" + err_msg
            req.finished_at = utcnow_iso()
            session.commit()
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m batchbrain.processor_worker <EXECUTION_ID>")
        sys.exit(1)
    run_worker(sys.argv[1])
