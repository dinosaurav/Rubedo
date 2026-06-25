import json
from typing import Any, Dict, Optional
from batchbrain.registry import get_processor
from batchbrain.api import process, RunSummary

def run_processor(
    processor_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    *,
    force: bool = False,
    folder: Optional[str] = None,
    workers: Optional[int] = None,
) -> RunSummary:
    """
    Shared runner that turns a ProcessorSpec + inputs into a process(...) call.
    """
    spec = get_processor(processor_id)
    input_dict = inputs or {}
    
    # 1. Validate inputs via schema
    if spec.input_model:
        validated_inputs = spec.input_model.model_validate(input_dict)
        validated_json = validated_inputs.model_dump(mode="json")
        bound_fn = lambda path: spec.fn(path, validated_inputs)
    else:
        validated_json = {}
        bound_fn = spec.fn
        
    # 2. Enforce folder override rules
    effective_folder = folder if folder and spec.allow_folder_override else spec.folder
    if folder and not spec.allow_folder_override:
        raise ValueError(f"Processor '{processor_id}' does not allow folder overrides.")
        
    # 3. Build effective config
    effective_workers = workers if workers is not None else spec.workers
    effective_config = {
        "processor_id": spec.id,
        "processor_static_config": spec.config or {},
        "processor_inputs": validated_json
    }
    
    # 4. Call process
    return process(
        folder=effective_folder,
        fn=bound_fn,
        code_version=spec.code_version,
        config=effective_config,
        step=spec.step,
        workers=effective_workers,
        force=force
    )
