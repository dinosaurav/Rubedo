from typing import Callable, Optional, Dict, Any, Type
from pydantic import BaseModel
from dataclasses import dataclass
import importlib.util
import sys
import os

@dataclass
class ProcessorSpec:
    id: str
    name: str
    folder: str
    code_version: str
    fn: Callable
    step: str = "process_file"
    input_model: Optional[Type[BaseModel]] = None
    config: Optional[Dict[str, Any]] = None
    workers: int = 4
    allow_folder_override: bool = False

_REGISTRY: Dict[str, ProcessorSpec] = {}

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
    def decorator(fn: Callable):
        spec = ProcessorSpec(
            id=id,
            name=name,
            folder=folder,
            code_version=code_version,
            fn=fn,
            step=step,
            input_model=input_model,
            config=config,
            workers=workers,
            allow_folder_override=allow_folder_override,
        )
        _REGISTRY[id] = spec
        return fn
    return decorator

def list_processors() -> list[ProcessorSpec]:
    load_processor_module()
    return list(_REGISTRY.values())

def get_processor(processor_id: str) -> ProcessorSpec:
    load_processor_module()
    if processor_id not in _REGISTRY:
        raise ValueError(f"Processor '{processor_id}' not found.")
    return _REGISTRY[processor_id]

def load_processor_module(path: str = "batchbrain_processors.py"):
    if os.path.exists(path):
        module_name = "batchbrain_processors"
        if module_name not in sys.modules:
            spec = importlib.util.spec_from_file_location(module_name, os.path.abspath(path))
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
