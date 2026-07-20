import os
import uuid

import pytest
from sqlalchemy.pool import StaticPool

from rubedo import Home


def make_home(env_folder: str) -> Home:
    Home.clear_registry_for_tests()
    abs_env = os.path.abspath(env_folder)
    os.makedirs(abs_env, exist_ok=True)
    return Home(
        abs_env,
        db_url=f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true",
        db_connect_args={"check_same_thread": False},
        db_poolclass=StaticPool,
        _fresh=True,
    )


@pytest.fixture(autouse=True)
def _clear_home_registry_between_tests():
    Home.clear_registry_for_tests()
    yield
    Home.clear_registry_for_tests()