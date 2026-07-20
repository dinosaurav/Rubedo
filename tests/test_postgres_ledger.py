"""Opt-in real-Postgres coverage for the ledger plane (TODO 7b)."""
from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import inspect

from rubedo import Home, pipeline, step
from rubedo.ledger import (
    _insert_materialization_edge,
    _upsert_input_hash_usage,
)
from rubedo.models import (
    Base,
    ImmutabilityError,
    InputHashUsage,
    MaterializationEdge,
    Run,
    RunCoordinateStatus,
    RunEvent,
)


@pytest.fixture(scope="session")
def pg_url() -> str:
    url = os.environ.get("RUBEDO_TEST_PG_URL")
    if not url:
        pytest.skip("Postgres tests require RUBEDO_TEST_PG_URL")
    return url


@pytest.fixture
def pg_home(tmp_path, pg_url: str):
    Home.clear_registry_for_tests()
    home = Home.ephemeral(tmp_path / "home", db_url=pg_url)
    Base.metadata.drop_all(home.db.engine)
    Base.metadata.create_all(home.db.engine)
    yield home
    Base.metadata.drop_all(home.db.engine)
    home.db.dispose()
    Home.clear_registry_for_tests()


def _run_pipeline(home: Home, name: str = "pg-ledger"):
    @step
    def root():
        return {"path": "a.txt", "value": 1}

    @step
    def enrich(root: dict):
        return {"path": root["path"], "value": root["value"] + 1}

    pipe = pipeline(name=name, steps=[root, enrich], home=home)
    return pipe, pipe.run(workers=1)


def test_postgres_schema_and_claim_fulfill_lifecycle(pg_home):
    assert pg_home.db.engine.dialect.name == "postgresql"
    tables = set(inspect(pg_home.db.engine).get_table_names())
    assert {
        "runs",
        "input_hash_usages",
        "run_coordinate_statuses",
        "materialization_edges",
    } <= tables

    pipe, first = _run_pipeline(pg_home)
    second = pipe.run(workers=1)
    assert first.created_count == 2
    assert second.reused_count == 2

    with pg_home.session() as session:
        usages = session.query(InputHashUsage).all()
        assert len(usages) == 2
        assert all(row.fulfilled for row in usages)
        assert session.query(MaterializationEdge).count() == 1


def test_postgres_queries_and_output_selection(pg_home):
    _, summary = _run_pipeline(pg_home, "pg-query")

    cells = summary.cells("enrich", resolve_output=True)
    assert len(cells) == 1
    assert cells[0].output == {"path": "a.txt", "value": 2}
    current = pg_home.current(pipeline="pg-query", resolve_output=True)
    assert {cell.step_name for cell in current} == {"root", "enrich"}
    selected = pg_home.select("step:enrich path:a.txt", resolve_output=True)
    assert len(selected) == 1
    assert selected[0].output["value"] == 2


def test_postgres_immutability_guards_and_mutable_ihu(pg_home):
    _, summary = _run_pipeline(pg_home, "pg-immutable")

    with pg_home.session() as session:
        status = session.query(RunCoordinateStatus).first()
        status.status = "tampered"
        with pytest.raises(ImmutabilityError, match="append-only"):
            session.commit()
        session.rollback()

    with pg_home.session() as session:
        event = session.query(RunEvent).first()
        session.delete(event)
        with pytest.raises(ImmutabilityError, match="cannot be deleted"):
            session.commit()
        session.rollback()

    with pg_home.session() as session:
        run = session.get(Run, summary.run_id)
        run.status = "completed"
        session.commit()
        run.source_id = "tampered"
        with pytest.raises(ImmutabilityError, match="immutable"):
            session.commit()
        session.rollback()

    with pg_home.session() as session:
        usage = session.query(InputHashUsage).first()
        usage.fulfilled = False
        usage.last_run_id = "different"
        session.commit()


def test_postgres_concurrent_ihu_upserts(pg_home):
    address = "same-address"
    with pg_home.session() as session:
        _upsert_input_hash_usage(
            session,
            address=address,
            run_id="seed",
            fulfilled=True,
        )
        session.commit()

    barrier = threading.Barrier(2)

    def claim(run_id: str) -> None:
        with pg_home.session() as session:
            barrier.wait()
            _upsert_input_hash_usage(
                session,
                address=address,
                run_id=run_id,
            )
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(claim, ("run-a", "run-b")))

    with pg_home.session() as session:
        usage = session.get(InputHashUsage, address)
        assert usage is not None
        assert usage.fulfilled is True  # claims preserve a live generation
        assert usage.last_run_id in {"run-a", "run-b"}

    fulfill_address = "first-fulfill"
    fulfill_barrier = threading.Barrier(2)

    def fulfill(run_id: str) -> None:
        with pg_home.session() as session:
            fulfill_barrier.wait()
            _upsert_input_hash_usage(
                session,
                address=fulfill_address,
                run_id=run_id,
                fulfilled=True,
            )
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(fulfill, ("run-c", "run-d")))

    with pg_home.session() as session:
        usage = session.get(InputHashUsage, fulfill_address)
        assert usage is not None and usage.fulfilled is True


def test_postgres_concurrent_lineage_edge_insert(pg_home):
    barrier = threading.Barrier(2)

    def insert_edge() -> None:
        with pg_home.session() as session:
            barrier.wait()
            _insert_materialization_edge(
                session,
                parent_address="parent",
                child_address="child",
            )
            session.commit()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: insert_edge(), range(2)))

    with pg_home.session() as session:
        assert session.query(MaterializationEdge).count() == 1
