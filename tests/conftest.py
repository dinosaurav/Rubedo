"""Global test fixtures.

Per-test isolation for the lane_store (arrow-storage-rewrite branch): every
test gets a fresh, empty ``tables/`` directory under its own env folder so
Arrow IPC files from one test don't leak into another.  Mirrors the
per-test OBJECTS_DIR / STAGING_DIR pattern every test file already uses for
the object store; doing it once here means existing tests don't each need
the lane_store setup boilerplate while the parallel-write migration runs.
"""

import os
import shutil
import tempfile

import pytest


@pytest.fixture(autouse=True)
def _isolated_lane_store():
    tmp = tempfile.mkdtemp(prefix="rubedo-lane-store-")
    import rubedo.lane_store

    rubedo.lane_store.TABLES_DIR = os.path.join(tmp, "tables")
    rubedo.lane_store.clear_run_buffers()
    yield
    rubedo.lane_store.clear_run_buffers()
    shutil.rmtree(tmp, ignore_errors=True)