"""Generations semantics: non-deterministic steps, supersede/resurrect, type fidelity."""

import itertools
import json as _json
import os

import pytest

from rubedo import Selection, invalidate, step, pipeline
from rubedo.models import RunCoordinateStatus, InputHashUsage
from rubedo.hashing import hash_bytes
from conftest import isolated_test_env

TEST_FOLDER = ".test_generations_data"
ENV_FOLDER = ".test_generations_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("generations") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    path = os.path.join(TEST_FOLDER, name)
    with open(path, "w") as f:
        f.write(content)
    return path


# These tests are all single-file, and genuine "generations" (repeat
# recomputes landing on the *same* address) require a stable lane — an
# expand root's own address tracks its yielded content ("identical rows
# collapse"), so a non-deterministic root would mint a fresh address
# every run, which is exactly the instability these tests must NOT have.
# A headless param-fed root (see test_headless_root.py) keeps the "@root"
# lane's address stable across recomputes; only the step's params/version
# move it.


def make_nondeterministic_pipeline(pipe_id="gen"):
    """Root step returns something different on every execution."""
    counter = itertools.count()

    @step
    def generate(params):
        return {"attempt": next(counter), "input": open(params["path"]).read()}

    @step
    def summarize(generate):
        return f"attempt={generate['attempt']}"

    return pipeline(name=pipe_id, steps=[generate, summarize], home=TEST_HOME)


def get_mats(step_name):
    """All Arrow rows for a step, sorted by ts (creation order)."""
    return [
        r for r in sorted(
            (r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == step_name),
            key=lambda r: r.get("ts", ""),
        )
    ]


def _is_live(addr):
    with TEST_HOME.session() as session:
        u = session.query(InputHashUsage).filter_by(address=addr).first()
        return bool(u and u.fulfilled)


def _object_path_for(row):
    """Object-store path for a spilled row, else None (inline values live
    in the Arrow row, not the object store)."""
    out = row.get("output")
    if isinstance(out, str) and out.startswith("objects:"):
        return TEST_HOME.store.object_path(out[len("objects:"):])
    return None


def test_invalidate_nondeterministic_creates_new_generation():
    pipe = make_nondeterministic_pipeline()
    path = create_file("f1.txt", "hello")
    params = {"path": path}

    pipe.run(params=params, workers=1)
    (gen1,) = get_mats("generate")
    (sum1,) = get_mats("summarize")

    invalidate(Selection(step="generate"), reason="bad output", home=TEST_HOME)

    summary = pipe.run(params=params, workers=1)
    assert summary.failed_count == 0

    gens = get_mats("generate")
    assert len(gens) == 2, "recompute with different bytes must be a new generation"
    assert gens[0].get("address") == gen1.get("address")
    # Both generations share the same address — the address is live
    # (the new generation is fulfilled).  The old generation is history
    # (an old Arrow row with a different output string).
    assert gens[0].get("output") != gens[1].get("output")
    assert _is_live(gens[1].get("address"))
    assert gens[0].get("address") == gens[1].get("address")
    assert gens[0].get("output") != gens[1].get("output")

    # Old output survives (immutability of committed outputs): spilled
    # objects are on disk; inline values persist in the append-only Arrow row.
    for g in (gens[0], gens[1]):
        p = _object_path_for(g)
        if p is not None:
            assert os.path.exists(p)
        else:
            assert g.get("output") is not None

    # Downstream saw the new output string and recomputed
    sums = get_mats("summarize")
    assert len(sums) == 2
    from rubedo.hashing import hash_bytes
    import json as _json
    _gen1_output = gens[1].get("output")
    _gen1_key = _gen1_output.encode("utf-8") if isinstance(_gen1_output, str) else _json.dumps(_gen1_output, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert sums[1].get("input_hash") == hash_bytes(_gen1_key)
    with TEST_HOME.session() as session:
        rc = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=summary.run_id, step_name="summarize")
            .one()
        )
        assert rc.status == "created"
        assert str(rc.output_address) == sums[1].get("address")


def test_force_nondeterministic_supersedes_live_generation():
    pipe = make_nondeterministic_pipeline()
    path = create_file("f1.txt", "hello")
    params = {"path": path}

    pipe.run(params=params, workers=1)
    summary = pipe.run(params=params, workers=1, force=True)
    assert summary.failed_count == 0

    gens = get_mats("generate")
    assert len(gens) == 2
    assert gens[0].get("output") != gens[1].get("output")
    assert _is_live(gens[1].get("address"))

    # Lifecycle rows gone in the new model

    # Downstream recomputed off the new bytes rather than reusing stale output
    sums = get_mats("summarize")
    _gen1_output = gens[1].get("output")
    _gen1_key = _gen1_output.encode("utf-8") if isinstance(_gen1_output, str) else _json.dumps(_gen1_output, sort_keys=True, separators=(",", ":")).encode("utf-8")
    assert sums[-1].get("input_hash") == hash_bytes(_gen1_key)


def test_force_deterministic_reuses_live_row():
    @step
    def stable(params):
        return open(params["path"]).read().upper()

    pipe = pipeline(name="det", steps=[stable], home=TEST_HOME)
    path = create_file("f1.txt", "hello")
    params = {"path": path}

    pipe.run(params=params, workers=1)
    pipe.run(params=params, workers=1, force=True)

    mats = get_mats("stable")
    assert len(mats) == 1, "identical bytes are the same fact, not a new generation"
    assert _is_live(mats[0].get("address"))


def test_params_are_part_of_cache_identity():
    from pydantic import BaseModel

    class Thresh(BaseModel):
        threshold: int = 0
        path: str = ""

    @step(params_model=Thresh)
    def score(params: Thresh):
        return {"ok": len(open(params.path).read()) >= params.threshold}

    @step
    def label(score):
        return "pass" if score["ok"] else "fail"

    pipe = pipeline(
        name="par", steps=[score, label], params_model=Thresh
    ,
        home=TEST_HOME,
    )
    path = create_file("f1.txt", "hello")

    s1 = pipe.run(params={"threshold": 1, "path": path}, workers=1)
    assert (s1.created_count, s1.reused_count) == (2, 0)

    # Same params: full cache hit
    s2 = pipe.run(params={"threshold": 1, "path": path}, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 2)

    # Different params, different answer: score recomputes (params are in
    # its address) and label follows through the content-hash chain
    s3 = pipe.run(params={"threshold": 100, "path": path}, workers=1)
    assert (s3.created_count, s3.reused_count) == (2, 0)

    # Different params, same answer: score recomputes but produces identical
    # bytes, so label is reused off the unchanged content hash
    s4 = pipe.run(params={"threshold": 2, "path": path}, workers=1)
    assert (s4.created_count, s4.reused_count) == (1, 1)


def test_params_do_not_churn_param_free_pipelines():
    path = create_file("f1.txt", "hello")

    # No "params" argument at all -> _step_accepts_params is False, so this
    # step's identity never folds in params, no matter what run() is given
    # — the point of this test.
    @step
    def upper():
        return open(path).read().upper()

    pipe = pipeline(name="nopar", steps=[upper], home=TEST_HOME)

    pipe.run(workers=1)
    s2 = pipe.run(params={"anything": 42}, workers=1)
    assert (s2.created_count, s2.reused_count) == (0, 1)


def test_string_payload_round_trips_as_string():
    @step
    def emit(params):
        return "123"  # would come back as int 123 under JSON guessing

    @step
    def check(emit):
        assert isinstance(emit, str), f"expected str, got {type(emit)}"
        return emit + "!"

    pipe = pipeline(name="types", steps=[emit, check], home=TEST_HOME)
    path = create_file("f1.txt", "x")

    summary = pipe.run(params={"path": path}, workers=1)
    assert summary.failed_count == 0
    (mat,) = get_mats("emit")
    assert mat.get("content_type") == "text"


def test_bytes_payload_round_trips_as_bytes():
    @step
    def emit_b(params):
        return b"\x00\x01binary"

    @step
    def check_b(emit_b):
        assert isinstance(emit_b, bytes)
        return {"length": len(emit_b)}

    pipe = pipeline(name="types-b", steps=[emit_b, check_b], home=TEST_HOME)
    path = create_file("f1.txt", "x")

    summary = pipe.run(params={"path": path}, workers=1)
    assert summary.failed_count == 0
    (mat,) = get_mats("emit_b")
    assert mat.get("content_type") == "bytes"
