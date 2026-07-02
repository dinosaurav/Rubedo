from typing import Callable, Optional, Dict, Any, Type, List
from pydantic import BaseModel
from dataclasses import dataclass
import importlib.util
import sys
import os

from .sources import Source


@dataclass
class StepSpec:
    name: str
    fn: Callable
    version: str
    depends_on: List[str]
    config_hash: str
    params_model: Optional[Type[BaseModel]] = None
    config: Optional[Dict[str, Any]] = None
    workers: int = 4
    code_hash: Optional[str] = None
    code_mode: str = "warn"  # warn | auto


@dataclass
class PipelineSpec:
    id: str
    name: str
    source: Source
    steps: List[StepSpec]


_REGISTRY: Dict[str, PipelineSpec] = {}


def clear_registry():
    _REGISTRY.clear()


def _hash_source(fn: Callable) -> Optional[str]:
    import inspect

    from .hashing import hash_text

    try:
        return hash_text(inspect.getsource(fn))
    except (OSError, TypeError):
        return None


def step(
    name: str,
    version: str,
    depends_on: Optional[List[str]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    config: Optional[Dict[str, Any]] = None,
    workers: int = 4,
    code: str = "warn",
):
    """Declare a step.

    version is the step's semantic identity — bump it for deliberate
    behavior changes (also the escape hatch for edits code hashing can't
    see, like helpers the step calls).

    code decides what a *source edit* means, independently of version:
      - "warn" (default): edits never recompute; reusing an output whose
        code has since changed produces a loud warning. Right for
        expensive/non-deterministic steps.
      - "auto": the function's source hash joins the cache identity, so any
        edit recomputes — no version bump needed. Right for cheap,
        deterministic steps.
    """
    if code not in ("warn", "auto"):
        raise ValueError(f"Step '{name}': code must be 'warn' or 'auto', got {code!r}")
    if version == "auto":
        raise ValueError(
            f"Step '{name}': version is a semantic label; use code='auto' "
            "to derive cache identity from the source instead"
        )

    def decorator(fn: Callable):
        from .hashing import hash_json

        code_hash = _hash_source(fn)
        if code == "auto" and code_hash is None:
            raise ValueError(
                f"Step '{name}': code='auto' requires an inspectable "
                "function source"
            )

        config_hash = hash_json(config or {})
        return StepSpec(
            name=name,
            fn=fn,
            version=version,
            depends_on=depends_on or [],
            config_hash=config_hash,
            params_model=params_model,
            config=config,
            workers=workers,
            code_hash=code_hash,
            code_mode=code,
        )

    return decorator


def pipeline(
    name: str,
    folder: Optional[str] = None,
    steps: Optional[List[StepSpec]] = None,
    id: Optional[str] = None,
    source: Optional[Source] = None,
):
    if (source is None) == (folder is None):
        raise ValueError("Pass exactly one of source= or folder= (FolderSource sugar)")
    if source is None:
        from .sources import FolderSource

        source = FolderSource(folder)

    pipe_id = id or name
    spec = PipelineSpec(
        id=pipe_id,
        name=name,
        source=source,
        steps=steps or [],
    )
    _REGISTRY[pipe_id] = spec
    return spec


def list_pipelines() -> List[PipelineSpec]:
    load_pipelines_module()
    return list(_REGISTRY.values())


def get_pipeline(pipeline_id: str) -> PipelineSpec:
    load_pipelines_module()
    if pipeline_id not in _REGISTRY:
        raise ValueError(f"Pipeline '{pipeline_id}' not found.")
    return _REGISTRY[pipeline_id]


def load_pipelines_module(path: Optional[str] = None):
    if path is None:
        path = os.environ.get("BATCHBRAIN_PIPELINES", "batchbrain_pipelines.py")
    if os.path.exists(path):
        module_name = "batchbrain_pipelines"
        if module_name not in sys.modules:
            spec = importlib.util.spec_from_file_location(
                module_name, os.path.abspath(path)
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
