from typing import Callable, Optional, Dict, Any, Type, List
from pydantic import BaseModel
from dataclasses import dataclass
import importlib.util
import sys
import os


@dataclass
class StepSpec:
    name: str
    fn: Callable
    version: str
    depends_on: List[str]
    config_hash: str
    input_model: Optional[Type[BaseModel]] = None
    config: Optional[Dict[str, Any]] = None
    workers: int = 4


@dataclass
class PipelineSpec:
    id: str
    name: str
    folder: str
    steps: List[StepSpec]
    allow_folder_override: bool = False


_REGISTRY: Dict[str, PipelineSpec] = {}


def clear_registry():
    _REGISTRY.clear()


def step(
    name: str,
    version: str,
    depends_on: Optional[List[str]] = None,
    input_model: Optional[Type[BaseModel]] = None,
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
            input_model=input_model,
            config=config,
            workers=workers,
        )

    return decorator


def pipeline(
    name: str,
    folder: str,
    steps: List[StepSpec],
    id: Optional[str] = None,
    allow_folder_override: bool = False,
):
    pipe_id = id or name
    spec = PipelineSpec(
        id=pipe_id,
        name=name,
        folder=folder,
        steps=steps,
        allow_folder_override=allow_folder_override,
    )
    _REGISTRY[pipe_id] = spec
    return spec


def processor(
    id: str,
    name: str,
    folder: str,
    code_version: str,
    step: str = "process_file",
    input_model: Optional[Type[BaseModel]] = None,
    config: Optional[Dict[str, Any]] = None,
    workers: int = 4,
    allow_folder_override: bool = False,
):
    """Legacy single-step processor. Internally mapped to a 1-node DAG."""

    def decorator(fn: Callable):
        from .hashing import hash_json

        config_hash = hash_json(config or {})
        s = StepSpec(
            name=step,
            fn=fn,
            version=code_version,
            depends_on=[],
            config_hash=config_hash,
            input_model=input_model,
            config=config,
            workers=workers,
        )
        p = PipelineSpec(
            id=id,
            name=name,
            folder=folder,
            steps=[s],
            allow_folder_override=allow_folder_override,
        )
        _REGISTRY[id] = p
        return fn

    return decorator


def list_processors() -> List[PipelineSpec]:
    load_processor_module()
    return list(_REGISTRY.values())


def get_processor(processor_id: str) -> PipelineSpec:
    load_processor_module()
    if processor_id not in _REGISTRY:
        raise ValueError(f"Processor/Pipeline '{processor_id}' not found.")
    return _REGISTRY[processor_id]


def load_processor_module(path: str = "batchbrain_processors.py"):
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
