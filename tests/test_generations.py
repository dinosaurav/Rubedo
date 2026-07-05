"""Generations semantics: non-deterministic steps, supersede/resurrect, type fidelity."""

import itertools
import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Selection, invalidate, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import (
    Materialization,
    MaterializationLifecycle,
    RunCoordinateStatus,
)
from rubedo.store import init_store

TEST_FOLDER = ".test_generations_data"
ENV_FOLDER = ".test_generations_env"


@pytest.fixture(autouse=True)
def isolated_env():
    abs_test_folder = os.path.abspath(TEST_FOLDER)
    abs_env_folder = os.path.abspath(ENV_FOLDER)
    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{abs_env_folder}/store/objects"
    rubedo.store.STAGING_DIR = f"{abs_env_folder}/store/staging"

    os.environ["RUBEDO_DB_PATH"] = (
        f"sqlite:///file:testdb_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"
    )
    init_db()

    import rubedo.db

    if rubedo.db.engine is not None:
        rubedo.db.engine.dispose()

    from rubedo.models import Base
    from sqlalchemy.orm import sessionmaker

    rubedo.db.engine = create_engine(
        os.environ["RUBEDO_DB_PATH"],
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=rubedo.db.engine)
    rubedo.db.SessionLocal = sessionmaker(
        autocommit=False, autoflush=False, bind=rubedo.db.engine
    )

    init_store()

    yield

    for d in (abs_test_folder, abs_env_folder):
        if os.path.exists(d):
            shutil.rmtree(d)


def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


def make_nondeterministic_pipeline(pipe_id="gen"):
    """Root step returns something different on every execution."""
    counter = itertools.count()

    @step(name="generate", version="1")
    def generate(path):
        return {"attempt": next(counter), "input": open(path).read()}

    @step(name="summarize", version="1", depends_on=["generate"])
    def summarize(generate):
        return f"attempt={generate['attempt']}"

    return pipeline(
        id=pipe_id, name=pipe_id, folder=TEST_FOLDER, steps=[generate, summarize]
    )


def get_mats(step_name):
    with get_session() as session:
        return (
            session.query(Materialization)
            .filter_by(step_name=step_name)
            .order_by(Materialization.id)
            .all()
        )


def test_invalidate_nondeterministic_creates_new_generation():
    pipe = make_nondeterministic_pipeline()
    create_file("f1.txt", "hello")

    run(pipe, workers=1)
    (gen1,) = get_mats("generate")
    (sum1,) = get_mats("summarize")

    invalidate(Selection(step="generate"), reason="bad output")

    summary = run(pipe, workers=1)
    assert summary.failed_count == 0

    gens = get_mats("generate")
    assert len(gens) == 2, "recompute with different bytes must be a new generation"
    assert gens[0].id == gen1.id
    assert gens[0].is_live is False, "old generation stays as history, not deleted"
    assert gens[1].is_live is True
    assert gens[0].output_address == gens[1].output_address
    assert gens[0].output_content_hash != gens[1].output_content_hash

    # The lifecycle log preserves the full story: invalidated by the user
    with get_session() as session:
        lifecycle = (
            session.query(MaterializationLifecycle)
            .filter_by(materialization_id=gen1.id)
            .order_by(MaterializationLifecycle.id)
            .all()
        )
        assert [lc.action for lc in lifecycle] == ["invalidated"]
        assert lifecycle[0].reason == "bad output"

    # Old bytes survive on disk (immutability of committed outputs)
    assert os.path.exists(gens[0].output_path)
    assert os.path.exists(gens[1].output_path)

    # Downstream saw the new content hash and recomputed
    sums = get_mats("summarize")
    assert len(sums) == 2
    assert sums[1].input_hash == gens[1].output_content_hash
    with get_session() as session:
        rc = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=summary.run_id, step_name="summarize")
            .one()
        )
        assert rc.status == "created"
        assert rc.materialization_id == sums[1].id


def test_force_nondeterministic_supersedes_live_generation():
    pipe = make_nondeterministic_pipeline()
    create_file("f1.txt", "hello")

    run(pipe, workers=1)
    summary = run(pipe, workers=1, force=True)
    assert summary.failed_count == 0

    gens = get_mats("generate")
    assert len(gens) == 2
    assert gens[0].is_live is False
    assert gens[1].is_live is True

    with get_session() as session:
        lc = (
            session.query(MaterializationLifecycle)
            .filter_by(materialization_id=gens[0].id)
            .one()
        )
        assert lc.action == "superseded"
        assert lc.superseded_by_id == gens[1].id

    # Downstream recomputed off the new bytes rather than reusing stale output
    sums = get_mats("summarize")
    assert sums[-1].input_hash == gens[1].output_content_hash


def test_force_deterministic_reuses_live_row():
    @step(name="stable", version="1")
    def stable(path):
        return open(path).read().upper()

    pipe = pipeline(
id="det", name="det", folder=TEST_FOLDER, steps=[stable])
    create_file("f1.txt", "hello")

    run(pipe, workers=1)
    run(pipe, workers=1, force=True)

    mats = get_mats("stable")
    assert len(mats) == 1, "identical bytes are the same fact, not a new generation"
    assert mats[0].is_live is True


def test_params_are_part_of_cache_identity():
    from pydantic import BaseModel

    class Thresh(BaseModel):
        threshold: int = 0

    @step(name="score", version="1", params_model=Thresh)
    def score(path, params: Thresh):
        return {"ok": len(open(path).read()) >= params.threshold}

    @step(name="label", version="1", depends_on=["score"])
    def label(score):
        return "pass" if score["ok"] else "fail"

    pipe = pipeline(
id="par", name="par", folder=TEST_FOLDER, steps=[score, label])
    create_file("f1.txt", "hello")

    s1 = run(pipe, params={"threshold": 1}, workers=1)
    assert (s1.created_count, s1.reused_count) == (2, 0)

    # Same params: full cache hit
    s2 = run(pipe, params={"threshold": 1}, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 2)

    # Different params, different answer: score recomputes (params are in
    # its address) and label follows through the content-hash chain
    s3 = run(pipe, params={"threshold": 100}, workers=1)
    assert (s3.created_count, s3.reused_count) == (2, 0)

    # Different params, same answer: score recomputes but produces identical
    # bytes, so label is reused off the unchanged content hash
    s4 = run(pipe, params={"threshold": 2}, workers=1)
    assert (s4.created_count, s4.reused_count) == (1, 1)


def test_params_do_not_churn_param_free_pipelines():
    @step(name="upper", version="1")
    def upper(path):
        return open(path).read().upper()

    pipe = pipeline(
id="nopar", name="nopar", folder=TEST_FOLDER, steps=[upper])
    create_file("f1.txt", "hello")

    run(pipe, workers=1)
    s2 = run(pipe, params={"anything": 42}, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 1)


def test_string_payload_round_trips_as_string():
    @step(name="emit", version="1")
    def emit(path):
        return "123"  # would come back as int 123 under JSON guessing

    @step(name="check", version="1", depends_on=["emit"])
    def check(emit):
        assert isinstance(emit, str), f"expected str, got {type(emit)}"
        return emit + "!"

    pipe = pipeline(
id="types", name="types", folder=TEST_FOLDER, steps=[emit, check])
    create_file("f1.txt", "x")

    summary = run(pipe, workers=1)
    assert summary.failed_count == 0
    (mat,) = get_mats("emit")
    assert mat.content_type == "text"


def test_bytes_payload_round_trips_as_bytes():
    @step(name="emit_b", version="1")
    def emit_b(path):
        return b"\x00\x01binary"

    @step(name="check_b", version="1", depends_on=["emit_b"])
    def check_b(emit_b):
        assert isinstance(emit_b, bytes)
        return {"length": len(emit_b)}

    pipe = pipeline(
id="types-b", name="types-b", folder=TEST_FOLDER, steps=[emit_b, check_b])
    create_file("f1.txt", "x")

    summary = run(pipe, workers=1)
    assert summary.failed_count == 0
    (mat,) = get_mats("emit_b")
    assert mat.content_type == "bytes"
