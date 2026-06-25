from typing import Callable, Any, Optional
from .models import RunSummary, ProcessResult
from .runner import run_process
from .selection import Selection
from .invalidation import invalidate, recompute

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
        invalidated=invalidated
    )

def process(
    folder: str,
    fn: Callable[[str], Any],
    *,
    code_version: str,
    config: Optional[dict[str, Any]] = None,
    step: str = "process_file",
    workers: int = 4,
    force: bool = False,
) -> RunSummary:
    """
    Process a folder of files with a given function.
    """
    return run_process(
        folder=folder,
        fn=fn,
        code_version=code_version,
        config=config,
        step=step,
        workers=workers,
        force=force
    )
