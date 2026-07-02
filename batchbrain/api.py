from typing import Any, Optional
from .models import RunSummary
from .selection import Selection


def select(
    *,
    source_folder: Optional[str] = None,
    coordinate_glob: Optional[str] = None,
    step: Optional[str] = None,
    code_version: Optional[str] = None,
    output_content_hash: Optional[str] = None,
    metadata: Optional[list] = None,
    invalidated: Optional[bool] = None,
) -> Selection:
    """
    Create a selection object for invalidation or querying.
    """
    from .selection import MetadataFilter

    meta_filters = None
    if metadata:
        meta_filters = []
        for m in metadata:
            if isinstance(m, dict):
                meta_filters.append(MetadataFilter(**m))
            elif isinstance(m, MetadataFilter):
                meta_filters.append(m)

    return Selection(
        source_folder=source_folder,
        coordinate_glob=coordinate_glob,
        step=step,
        code_version=code_version,
        output_content_hash=output_content_hash,
        metadata=meta_filters,
        invalidated=invalidated,
    )


def process_pipeline(
    pipeline,  # PipelineSpec
    folder: str,
    *,
    config: Optional[dict[str, Any]] = None,
    workers: Optional[int] = None,
    force: bool = False,
    inputs: Optional[dict] = None,
) -> RunSummary:
    """
    Process a folder of files using a DAG PipelineSpec.
    """
    from .runner import run_pipeline

    return run_pipeline(
        pipeline=pipeline,
        folder=folder,
        config=config,
        workers=workers,
        force=force,
        inputs=inputs,
    )
