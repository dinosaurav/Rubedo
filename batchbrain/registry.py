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


@dataclass
class PipelineSpec:
    id: str
    name: str
    source: Source
    steps: List[StepSpec]
    allow_source_override: bool = False


_REGISTRY: Dict[str, PipelineSpec] = {}


def clear_registry():
    _REGISTRY.clear()


def step(
    name: str,
    version: str,
    depends_on: Optional[List[str]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    config: Optional[Dict[str, Any]] = None,
    workers: int = 4,
):
    def decorator(fn: Callable):
        from .hashing import hash_json

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
        )

    return decorator


def pipeline(
    name: str,
    folder: Optional[str] = None,
    steps: Optional[List[StepSpec]] = None,
    id: Optional[str] = None,
    source: Optional[Source] = None,
    allow_source_override: bool = False,
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
        allow_source_override=allow_source_override,
    )
    _REGISTRY[pipe_id] = spec
    return spec


def list_processors() -> List[PipelineSpec]:
    load_processor_module()
    return list(_REGISTRY.values())


def get_processor(processor_id: str) -> PipelineSpec:
    load_processor_module()
    if processor_id not in _REGISTRY:
        raise ValueError(f"Processor/Pipeline '{processor_id}' not found.")
    return _REGISTRY[processor_id]


def load_processor_module(path: Optional[str] = None):
    if path is None:
        path = os.environ.get("BATCHBRAIN_PROCESSORS", "batchbrain_processors.py")
    if os.path.exists(path):
        module_name = "batchbrain_processors"
        if module_name not in sys.modules:
            spec = importlib.util.spec_from_file_location(
                module_name, os.path.abspath(path)
            )
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
