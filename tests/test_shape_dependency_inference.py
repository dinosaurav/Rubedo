"""TODO 22: shape & dependency inference.

Three inferences, all resolving to the same explicit StepSpec the API
already builds — the engine, planner, and ledger never know inference
existed:
  - a generator function defaults to shape="expand"
  - join_on=/group_key= default shape to "join"/"reduce"
  - an omitted depends_on= is inferred from fn's parameter names once every
    sibling step is known (`pipeline.py::_build_spec`), each non-`params`
    parameter naming a registered step (signature order); *args/**kwargs
    skip inference; an explicit depends_on= (list or {"param": "step"}
    alias dict) always disables it.
"""


import pytest

from rubedo import pipeline, step
from rubedo.spec import definition
from conftest import isolated_test_env

TEST_FOLDER = ".test_shape_dep_inference_data"
ENV_FOLDER = ".test_shape_dep_inference_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("shape_dep_inference") as env:
        TEST_HOME = env.home
        yield

# ---------- shape inference (decoration time) ----------


def test_generator_function_infers_expand_shape():
    @step()
    def rows():
        yield {"v": 1}
        yield {"v": 2}

    assert rows.out_shape == "many"
    assert rows.depends_on == []


def test_explicit_non_expand_shape_on_generator_raises():
    with pytest.raises(ValueError, match="generator function must have out_shape='many'"):

        @step(shape="map")
        def rows():
            yield 1


def test_join_on_infers_join_shape_without_explicit_shape():
    s = step(depends_on=["a", "b"], join_on={"a": "k", "b": "k"})(lambda a, b: None)
    assert s.in_shape == "join"


def test_group_key_infers_reduce_shape_without_explicit_shape():
    s = step(depends_on=["a"], group_key="g")(lambda a: None)
    assert s.in_shape == "aggregate"


def test_conflicting_explicit_shape_with_join_on_raises():
    with pytest.raises(ValueError, match="join_on requires in_shape='join'"):
        step(shape="map", depends_on=["a", "b"], join_on={"a": "k", "b": "k"})(
            lambda a, b: None
        )


def test_conflicting_explicit_shape_with_group_key_raises():
    with pytest.raises(ValueError, match="group_key requires in_shape='aggregate' or 'fold'"):
        step(shape="map", depends_on=["a"], group_key="g")(lambda a: None)


# ---------- depends_on inference (build time — pipeline.py::_build_spec) ----------


def test_param_name_infers_depends_on():
    @step()
    def scan():
        yield {"v": 1}

    @step()
    def extract(scan):
        return scan

    p = pipeline(name="infer", steps=[scan, extract], home=TEST_HOME)
    assert p.spec.steps[1].depends_on == ["scan"]


def test_parentless_generator_behaves_as_a_source_with_zero_kwargs():
    @step()
    def rows():
        yield {"v": 1}
        yield {"v": 2}

    p = pipeline(name="root-src", steps=[rows], home=TEST_HOME)
    assert p.spec.steps[0].depends_on == []
    assert p.spec.steps[0].out_shape == "many"

    summary = p.run()
    assert summary.created_count == 2


def test_unmatched_parameter_raises_naming_step_parameter_and_candidates():
    @step()
    def scan():
        yield {"v": 1}

    @step()
    def extract(nope):
        return nope

    p = pipeline(name="bad-param", steps=[scan, extract], home=TEST_HOME)
    with pytest.raises(ValueError) as exc_info:
        p.spec

    msg = str(exc_info.value)
    assert "extract" in msg
    assert "nope" in msg
    assert "scan" in msg


def test_varargs_signature_skips_inference():
    @step()
    def scan():
        yield {"v": 1}

    @step()
    def sink(*args, **kwargs):
        return None

    p = pipeline(name="varargs", steps=[scan, sink], home=TEST_HOME)
    # Ambiguous signature: inference is skipped entirely, so `sink` falls
    # out as its own (unrelated) root rather than raising.
    assert p.spec.steps[1].depends_on == []


def test_explicit_depends_on_disables_inference_even_when_matching():
    @step()
    def scan():
        yield {"v": 1}

    # Explicit depends_on=[] on a step whose parameter *would* match a
    # sibling step name if inferred: explicit always wins, so this step
    # stays a (second, independent) root.
    @step(depends_on=[])
    def extract(scan):
        return scan

    p = pipeline(name="explicit-wins", steps=[scan, extract], home=TEST_HOME)
    assert p.spec.steps[1].depends_on == []


# ---------- depends_on dict alias form ----------


def test_depends_on_dict_alias_binds_execution_kwarg():
    @step(shape="expand")
    def scan():
        yield {"v": 1}

    @step(depends_on={"raw": "scan"})
    def extract(raw):
        return {"got": raw["v"]}

    p = pipeline(name="alias", steps=[scan, extract], home=TEST_HOME)
    assert p.spec.steps[1].depends_on == ["scan"]
    assert p.spec.steps[1].depends_on_aliases == {"scan": "raw"}

    summary = p.run()
    values = list(summary.output_for("extract").values())
    assert values == [{"got": 1}]


def test_depends_on_dict_alias_on_join():
    @step(shape="expand")
    def orders_src():
        yield {"oid": 1, "cust": "a"}

    @step(shape="expand")
    def customers_src():
        yield {"cid": "a", "name": "Acme"}

    @step
    def order(orders_src):
        return {"oid": orders_src["oid"], "cust": orders_src["cust"]}

    @step
    def customer(customers_src):
        return {"cid": customers_src["cid"], "name": customers_src["name"]}

    # dict alias binds the parent step names to the function's param names
    @step(
        depends_on={"o": "order", "c": "customer"},
        join_on={"order": "cust", "customer": "cid"},
    )
    def enrich(o, c):
        return {"oid": o["oid"], "name": c["name"]}

    p = pipeline(name="join-alias", steps=[orders_src, customers_src, order, customer, enrich], home=TEST_HOME)
    assert p.spec.steps[-1].depends_on == ["order", "customer"]
    assert p.spec.steps[-1].depends_on_aliases == {"order": "o", "customer": "c"}

    summary = p.run()
    values = list(summary.output_for("enrich").values())
    assert values == [{"oid": 1, "name": "Acme"}]


def test_depends_on_dict_alias_on_reduce():
    @step(shape="expand")
    def scan():
        yield {"v": 1}
        yield {"v": 2}

    @step(depends_on={"raw": "scan"}, shape="reduce")
    def total(raw):
        return {"sum": sum(v["v"] for v in raw.values())}

    p = pipeline(name="reduce-alias", steps=[scan, total], home=TEST_HOME)
    assert p.spec.steps[-1].depends_on == ["scan"]
    assert p.spec.steps[-1].depends_on_aliases == {"scan": "raw"}

    summary = p.run()
    values = list(summary.output_for("total").values())
    assert values == [{"sum": 3}]


# ---------- traps: byte-identical definition(), untouched addresses ----------


def test_inferred_pipeline_definition_is_byte_identical_to_explicit_twin():
    @step()
    def scan():
        yield {"v": 1}

    @step()
    def extract(scan):
        return scan

    inferred = pipeline(name="twin", steps=[scan, extract], home=TEST_HOME)

    @step(name="scan", version="0", shape="expand")
    def scan_explicit():
        yield {"v": 1}

    @step(name="extract", version="0", depends_on=["scan"])
    def extract_explicit(scan):
        return scan

    explicit = pipeline(name="twin", steps=[scan_explicit, extract_explicit], home=TEST_HOME)

    inferred_def = inferred.definition()
    explicit_def = explicit.definition()
    for s in inferred_def["steps"] + explicit_def["steps"]:
        s.pop("source", None)
    assert inferred_def == explicit_def

    inf_spec_def = definition(inferred.spec)
    exp_spec_def = definition(explicit.spec)
    for s in inf_spec_def["steps"] + exp_spec_def["steps"]:
        s.pop("source", None)
    assert inf_spec_def == exp_spec_def


def test_rerun_over_existing_store_fully_reuses():
    @step()
    def scan():
        yield {"v": 1}
        yield {"v": 2}

    @step()
    def extract(scan):
        return {"doubled": scan["v"] * 2}

    p = pipeline(name="reuse-check", steps=[scan, extract], home=TEST_HOME)
    first = p.run()
    assert first.created_count == 4  # 2 scan lanes + 2 extract lanes
    assert first.reused_count == 0

    second = p.run()
    assert second.created_count == 0
    assert second.reused_count == 4


@pytest.mark.filterwarnings("ignore:Step 'extract' source code changed")
def test_explicit_twin_reuses_the_inferred_pipelines_store():
    """Addresses are untouched by inference: a pipeline built with explicit
    shape=/depends_on= matching an inferred pipeline's resolved values reads
    back the same store with zero recomputation."""

    @step()
    def scan():
        yield {"v": 1}

    @step()
    def extract(scan):
        return {"doubled": scan["v"] * 2}

    inferred = pipeline(name="same-store", steps=[scan, extract], home=TEST_HOME)
    first = inferred.run()
    assert first.created_count == 2

    @step(name="scan", version="0", shape="expand")
    def scan_explicit():
        yield {"v": 1}

    @step(name="extract", version="0", depends_on=["scan"])
    def extract_explicit(scan):
        return {"doubled": scan["v"] * 2}

    explicit = pipeline(name="same-store", steps=[scan_explicit, extract_explicit], home=TEST_HOME)
    second = explicit.run()
    assert second.created_count == 0
    assert second.reused_count == 2
