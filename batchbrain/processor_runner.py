import json
from typing import Any, Dict, Optional
from batchbrain.registry import get_processor, PipelineSpec
from batchbrain.api import process_pipeline, RunSummary

def run_processor(
    processor_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    *,
    force: bool = False,
    folder: Optional[str] = None,
    workers: Optional[int] = None,
) -> RunSummary:
    """
    Shared runner that turns a PipelineSpec + inputs into a process_pipeline(...) call.
    """
    spec = get_processor(processor_id)
    input_dict = inputs or {}
    
    first_step = spec.steps[0] if spec.steps else None
    
    # 1. Validate inputs via schema of the first step
    if first_step and first_step.input_model:
        validated_inputs = first_step.input_model.model_validate(input_dict)
        validated_json = validated_inputs.model_dump(mode="json")
    else:
        validated_json = {}
        
    # 2. Enforce folder override rules
    effective_folder = folder if folder and spec.allow_folder_override else spec.folder
    if folder and not spec.allow_folder_override:
        raise ValueError(f"Pipeline '{processor_id}' does not allow folder overrides.")
        
    # 3. Build effective config
    effective_config = {
        "processor_id": spec.id,
        "processor_inputs": validated_json
    }
    
    # 4. Call process_pipeline
    return process_pipeline(
        pipeline=spec,
        folder=effective_folder,
        config=effective_config,
        workers=workers,
        force=force,
        inputs=validated_json
    )
