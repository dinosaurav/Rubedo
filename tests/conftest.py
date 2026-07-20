"""Global test fixtures and helpers."""

from __future__ import annotations

import os
import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import pytest
from sqlalchemy.pool import StaticPool

from rubedo import Home


def make_home(env_folder: str) -> Home:
    """Ephemeral in-memory Home rooted at ``env_folder`` (objects/tables)."""
    Home.clear_registry_for_tests()
    abs_env = os.path.abspath(env_folder)
    os.makedirs(abs_env, exist_ok=True)
    return Home.ephemeral(
        abs_env,
        db_url=(
            f"sqlite:///file:testdb_{uuid.uuid4().hex}"
            "?mode=memory&cache=shared&uri=true"
        ),
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
    )


@dataclass
class TestEnv:
    """Isolated data dir + Home for one test."""

    home: Home
    data_dir: str
    env_dir: str

    def write(self, name: str, content: str) -> str:
        path = os.path.join(self.data_dir, name)
        os.makedirs(os.path.dirname(path) or self.data_dir, exist_ok=True)
        with open(path, "w") as f:
            f.write(content)
        return path


@contextmanager
def isolated_test_env(
    name: str, *, with_data: bool = True
) -> Iterator[TestEnv]:
    """Create/clean ``.test_<name>_data`` + ``.test_<name>_env``, yield TestEnv.

    Prefer this over copy-pasting rmtree/makedirs/make_home fixtures.
    """
    data_dir = os.path.abspath(f".test_{name}_data") if with_data else ""
    env_dir = os.path.abspath(f".test_{name}_env")
    dirs = [env_dir] + ([data_dir] if with_data else [])
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)
    home = make_home(env_dir)
    try:
        yield TestEnv(home=home, data_dir=data_dir, env_dir=env_dir)
    finally:
        for d in dirs:
            if os.path.exists(d):
                shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(autouse=True)
def _clear_home_registry_between_tests():
    Home.clear_registry_for_tests()
    yield
    Home.clear_registry_for_tests()
