import csv
import os

import pytest

from rubedo import step, pipeline
from rubedo.models import MaterializationEdge, RunCoordinateStatus, RunEvent
from conftest import isolated_test_env

DATA = ".test_join_data"
ENV = ".test_join_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("join") as env:
        TEST_HOME = env.home
        yield

def write_csv(name, text):
    with open(os.path.join(DATA, name), "w") as f:
        f.write(text)


def csv_source(name):
    """CSV recipe: a root expand step yielding each row dict. `name` is
    both the step name and the `<name>.csv` file under DATA."""
    path = os.path.join(DATA, f"{name}.csv")

    @step(name=name)
    def _scan():
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                yield row

    return _scan


def assert_run(pipe, **kw):
    summary = pipe.run(workers=1, **kw)
    if summary.failed_count > 0:
        with TEST_HOME.session() as session:
            for e in (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            ):
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


def _outputs(step_name):
    return {
        cell.coordinate: cell.output
        for cell in TEST_HOME.select(f"step:{step_name}", resolve_output=True)
        if cell.output_address
    }


def test_two_way_equijoin():
    write_csv("orders.csv", "oid,cust\no1,c1\no2,c1\no3,c2\n")
    write_csv("customers.csv", "cid,name\nc1,Alice\nc2,Bob\n")

    orders_src = csv_source("orders")
    customers_src = csv_source("customers")

    @step
    def order(orders):
        return {"oid": orders["oid"], "cust": orders["cust"]}

    @step
    def customer(customers):
        return {"cid": customers["cid"], "name": customers["name"]}

    @step(
        depends_on=["order", "customer"],
        join_on={"order": "cust", "customer": "cid"},
    )
    def enrich(order, customer):
        return {"oid": order["oid"], "name": customer["name"]}

    pipe = pipeline(
        name="j",
        steps=[orders_src, customers_src, order, customer, enrich],
    
        home=TEST_HOME,
    )
    assert_run(pipe)

    outs = _outputs("enrich")
    # o1,o2 match Alice(c1); o3 matches Bob(c2)
    assert {v["oid"]: v["name"] for v in outs.values()} == {
        "o1": "Alice", "o2": "Alice", "o3": "Bob",
    }
    # each joined lane edges to both its sides
    with TEST_HOME.session() as session:
        enrich_rows = [r for r in TEST_HOME.lanes.all_filled_rows() if r.get("step_name") == "enrich"]
        assert len(enrich_rows) >= 1
        addr = enrich_rows[0].get("address")
        assert session.query(MaterializationEdge).filter_by(child_address=addr).count() == 2

    # re-run: joins reused (identity = the two sides' content)
    s2 = assert_run(pipe)
    assert s2.created_count == 0
    assert s2.reused_count > 0


def test_four_way_star_join():
    # four sources all keyed by the same uid value
    for src in ("s_a", "s_b", "s_c", "s_d"):
        write_csv(f"{src}.csv", f"uid,v\nu1,{src}1\nu2,{src}2\n")

    def loader(src_name, step_name):
        @step(name=step_name, depends_on=[src_name])
        def load(**kwargs):
            row = kwargs[src_name]
            return {"uid": row["uid"], "v": row["v"]}
        return load

    srcs = [csv_source(n) for n in ("s_a", "s_b", "s_c", "s_d")]
    a, b, c, d = (
        loader(src_name, step_name)
        for src_name, step_name in zip(("s_a", "s_b", "s_c", "s_d"), ("a", "b", "c", "d"))
    )

    @step(
        depends_on=["a", "b", "c", "d"],
        join_on={"a": "uid", "b": "uid", "c": "uid", "d": "uid"},
    )
    def merge(a, b, c, d):
        return "".join([a["v"], b["v"], c["v"], d["v"]])

    pipe = pipeline(
        name="star",
        steps=[*srcs, a, b, c, d, merge],
    
        home=TEST_HOME,
    )
    assert_run(pipe)

    outs = _outputs("merge")
    # one merged lane per shared uid (u1, u2)
    assert sorted(outs.values()) == ["s_a1s_b1s_c1s_d1", "s_a2s_b2s_c2s_d2"]

def test_join_failed_parent_lane():
    write_csv("a_csv.csv", "id,val\n1,A\n2,B\n3,fail\n")
    write_csv("b_csv.csv", "id,val\n1,X\n2,Y\n3,Z\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        if a_csv["val"] == "fail":
            raise ValueError("bad data")
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(
        depends_on=["a", "b"], join_on={"a": "id", "b": "id"},
        on_failed="block",
    )
    def merge(a, b):
        return a["v"] + b["v"]

    pipe = pipeline(
        name="join_fail",
        steps=[a_src, b_src, load_a, load_b, merge],
    
        home=TEST_HOME,
    )
    s1 = pipe.run(workers=1)
    
    assert s1.failed_count == 1
    assert s1.blocked_count == 1
    
    with TEST_HOME.session() as session:
        status = session.query(RunCoordinateStatus).filter_by(run_id=s1.run_id, step_name="merge").one()
        assert status.status == "blocked"
        assert "a:row-" in status.metadata_json

def test_join_failed_parent_lane_use_passed():
    write_csv("a_csv.csv", "id,val\n1,A\n2,B\n3,fail\n")
    write_csv("b_csv.csv", "id,val\n1,X\n2,Y\n3,Z\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        if a_csv["val"] == "fail":
            raise ValueError("bad data")
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(depends_on=["a", "b"], join_on={"a": "id", "b": "id"})
    def merge(a, b):
        return a["v"] + b["v"]

    pipe = pipeline(
        name="join_fail_pass",
        steps=[a_src, b_src, load_a, load_b, merge],
    
        home=TEST_HOME,
    )
    s1 = pipe.run(workers=1)

    assert s1.failed_count == 1
    assert s1.blocked_count == 0
    # 3 a_csv + 2 a (1 fails) + 3 b_csv + 3 b + 2 merge
    assert s1.created_count == 13

    outs = _outputs("merge")
    assert sorted(outs.values()) == ["AX", "BY"]


def test_join_requires_join_on():
    with pytest.raises(ValueError, match="requires join_on"):
        step(name="bad", shape="join", depends_on=["a", "b"])(
            lambda a, b: None
        )


def test_join_needs_two_parents():
    with pytest.raises(ValueError, match="at least two parents"):
        step(
            name="bad", shape="join",
            depends_on=["a"], join_on={"a": "k"},
        )(lambda a: None)


def test_join_on_must_match_depends_on():
    with pytest.raises(ValueError, match="must match depends_on"):
        step(
            name="bad", shape="join",
            depends_on=["a", "b"], join_on={"a": "k", "c": "k"},
        )(lambda a, b: None)

def test_join_empty():
    write_csv("a_csv.csv", "id,val\n1,A\n")
    write_csv("b_csv.csv", "id,val\n2,B\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(depends_on=["a", "b"], join_on={"a": "id", "b": "id"})
    def merge(a, b):
        return a["v"] + b["v"]

    pipe = pipeline(
        name="join_empty",
        steps=[a_src, b_src, load_a, load_b, merge],
    
        home=TEST_HOME,
    )
    s1 = pipe.run(workers=1)

    assert s1.failed_count == 0
    assert s1.blocked_count == 0
    assert s1.created_count == 4  # 1 a_csv + 1 a + 1 b_csv + 1 b
    assert _outputs("merge") == {}
