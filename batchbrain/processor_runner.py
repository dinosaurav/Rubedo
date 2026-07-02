from typing import Any, Dict, Optional, Union
from batchbrain.registry import get_processor
from batchbrain.models import RunSummary
from batchbrain.runner import run_pipeline
from batchbrain.sources import Source, coerce_source


def run_processor(
    processor_id: str,
    inputs: Optional[Dict[str, Any]] = None,
    *,
    force: bool = False,
    source: Optional[Union[Source, str]] = None,
    workers: Optional[int] = None,
) -> RunSummary:
    """
    Shared runner that turns a PipelineSpec + inputs into a run_pipeline(...) call.
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

    # 2. Enforce source override rules
    if source is not None and not spec.allow_source_override:
        raise ValueError(f"Pipeline '{processor_id}' does not allow source overrides.")
    effective_source = coerce_source(source) if source is not None else spec.source

    # 3. Build effective config
    effective_config = {"processor_id": spec.id, "processor_inputs": validated_json}

    # 4. Call run_pipeline
    return run_pipeline(
        pipeline=spec,
        source=effective_source,
        config=effective_config,
        workers=workers,
        force=force,
        inputs=validated_json,
    )
