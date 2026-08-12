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
    # two orders share cust=c1 → cartesian-multiplicity warning (intentional)
    with pytest.warns(UserWarning, match="duplicate lanes"):
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
    with pytest.warns(UserWarning, match="duplicate lanes"):
        s2 = assert_run(pipe)
    assert s2.created_count == 0
    assert s2.reused_count > 0


def test_join_duplicate_keys_warns_many_to_many():
    write_csv("a_csv.csv", "id,val\n1,A1\n1,A2\n")
    write_csv("b_csv.csv", "id,val\n1,B1\n1,B2\n")

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
        name="join_dups",
        steps=[a_src, b_src, load_a, load_b, merge],
        home=TEST_HOME,
    )
    with pytest.warns(UserWarning, match=r"2×2 = 4 lane"):
        assert_run(pipe)
    assert sorted(_outputs("merge").values()) == ["A1B1", "A1B2", "A2B1", "A2B2"]


def test_join_null_key_rejected():
    write_csv("a_csv.csv", "id,val\n1,A\n")
    write_csv("b_csv.csv", "id,val\n1,B\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        return {"id": None, "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(depends_on=["a", "b"], join_on={"a": "id", "b": "id"})
    def merge(a, b):
        return a["v"] + b["v"]

    pipe = pipeline(
        name="join_null_key",
        steps=[a_src, b_src, load_a, load_b, merge],
        home=TEST_HOME,
    )
    with pytest.raises(ValueError, match="cannot cartesian-match"):
        pipe.run(workers=1)


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


def test_join_mode_union_binary_null_pads():
    write_csv("orders.csv", "oid,cust\no1,c1\no2,c9\n")
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
        join_mode="union",
    )
    def enrich(order, customer):
        return {
            "oid": None if order is None else order["oid"],
            "name": None if customer is None else customer["name"],
        }

    pipe = pipeline(
        name="outer",
        steps=[orders_src, customers_src, order, customer, enrich],
        home=TEST_HOME,
    )
    assert_run(pipe)

    outs = list(_outputs("enrich").values())
    by_oid = {o["oid"]: o["name"] for o in outs if o["oid"] is not None}
    assert by_oid == {"o1": "Alice", "o2": None}
    # unmatched customer c2
    assert any(o["oid"] is None and o["name"] == "Bob" for o in outs)
    # coords use @missing for absent sides
    coords = set(_outputs("enrich"))
    assert any("|@missing" in c or c.endswith("|@missing") for c in coords)
    assert any(c.startswith("@missing|") for c in coords)


def test_join_mode_union_reuses_unmatched():
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

    @step(
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
    )
    def merge(a, b):
        return {
            "a": None if a is None else a["v"],
            "b": None if b is None else b["v"],
        }

    pipe = pipeline(
        name="outer_reuse",
        steps=[a_src, b_src, load_a, load_b, merge],
        home=TEST_HOME,
    )
    s1 = assert_run(pipe)
    assert len(_outputs("merge")) == 2
    s2 = assert_run(pipe)
    assert s2.created_count == 0
    assert s2.reused_count >= s1.created_count


def test_join_mode_intersect_to_union_reuses_matched():
    write_csv("a_csv.csv", "id,val\n1,A\n2,A2\n")
    write_csv("b_csv.csv", "id,val\n1,B\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(depends_on=["a", "b"], join_on={"a": "id", "b": "id"})
    def merge_inner(a, b):
        return (None if a is None else a["v"]) + (None if b is None else b["v"])

    pipe_inner = pipeline(
        name="flip_mode",
        steps=[a_src, b_src, load_a, load_b, merge_inner],
        home=TEST_HOME,
    )
    assert_run(pipe_inner)
    matched_addr = {
        c.coordinate: c.output_address
        for c in TEST_HOME.select("step:merge_inner", resolve_output=True)
        if c.output_address
    }
    assert len(matched_addr) == 1

    @step(
        name="merge_inner",
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
        version="0",
    )
    def merge_outer(a, b):
        left = "" if a is None else a["v"]
        right = "" if b is None else b["v"]
        return left + right

    # Same step name+version+pipeline: rebuild pipe with union mode.
    # Redefining the step function with same version may warn code-drift.
    pipe_outer = pipeline(
        name="flip_mode",
        steps=[a_src, b_src, load_a, load_b, merge_outer],
        home=TEST_HOME,
    )
    with pytest.warns(UserWarning):
        s2 = assert_run(pipe_outer)
    outs = _outputs("merge_inner")
    # matched id=1 reused; unmatched id=2 created
    assert any(v == "AB" for v in outs.values())
    assert any(v == "A2" for v in outs.values())
    assert s2.reused_count >= 1
    after = {
        c.coordinate: c.output_address
        for c in TEST_HOME.select("step:merge_inner", resolve_output=True)
        if c.output_address and "@missing" not in c.coordinate
    }
    # The matched pair's address is unchanged across the mode flip.
    (matched_coord,) = matched_addr.keys()
    assert after[matched_coord] == matched_addr[matched_coord]


def test_join_mode_union_nway():
    write_csv("a_csv.csv", "uid,v\nu1,A\nu2,A2\n")
    write_csv("b_csv.csv", "uid,v\nu1,B\n")
    write_csv("c_csv.csv", "uid,v\nu1,C\nu3,C3\n")

    def loader(src_name, step_name):
        @step(name=step_name, depends_on=[src_name])
        def load(**kwargs):
            row = kwargs[src_name]
            return {"uid": row["uid"], "v": row["v"]}
        return load

    srcs = [csv_source(n) for n in ("a_csv", "b_csv", "c_csv")]
    a, b, c = (
        loader(src, name)
        for src, name in zip(("a_csv", "b_csv", "c_csv"), ("a", "b", "c"))
    )

    @step(
        depends_on=["a", "b", "c"],
        join_on={"a": "uid", "b": "uid", "c": "uid"},
        join_mode="union",
    )
    def merge(a, b, c):
        return {
            "a": None if a is None else a["v"],
            "b": None if b is None else b["v"],
            "c": None if c is None else c["v"],
        }

    pipe = pipeline(
        name="nway_outer",
        steps=[*srcs, a, b, c, merge],
        home=TEST_HOME,
    )
    assert_run(pipe)
    outs = list(_outputs("merge").values())
    assert {"a": "A", "b": "B", "c": "C"} in outs
    assert {"a": "A2", "b": None, "c": None} in outs
    assert {"a": None, "b": None, "c": "C3"} in outs


def test_join_mode_union_failed_warns():
    write_csv("a_csv.csv", "id,val\n1,A\n2,fail\n")
    write_csv("b_csv.csv", "id,val\n1,X\n2,Y\n")

    a_src = csv_source("a_csv")
    b_src = csv_source("b_csv")

    @step(name="a")
    def load_a(a_csv):
        if a_csv["val"] == "fail":
            raise ValueError("bad")
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
        on_failed="use_passed",
    )
    def merge(a, b):
        return {
            "a": None if a is None else a["v"],
            "b": None if b is None else b["v"],
        }

    pipe = pipeline(
        name="outer_fail",
        steps=[a_src, b_src, load_a, load_b, merge],
        home=TEST_HOME,
    )
    with pytest.warns(UserWarning, match="failed ≈ unmatched"):
        pipe.run(workers=1)
    outs = list(_outputs("merge").values())
    assert {"a": "A", "b": "X"} in outs
    # id=2: a failed → b may emit with a=None
    assert any(o.get("a") is None and o.get("b") == "Y" for o in outs)


def test_join_mode_rejects_invalid():
    with pytest.raises(ValueError, match="join_mode"):
        step(
            name="bad",
            join_on={"a": "k", "b": "k"},
            join_mode="left",  # type: ignore[arg-type]
        )(lambda a, b: None)


def test_declarative_join_union_nests_none():
    from rubedo import pipeline as pipeline_factory

    write_csv("orders.csv", "oid,cust\no1,c1\no2,c9\n")
    write_csv("customers.csv", "cid,name\nc1,Alice\n")

    p = pipeline_factory(name="decl_outer", home=TEST_HOME)

    @p.step
    def orders():
        import csv
        import os
        with open(os.path.join(DATA, "orders.csv"), newline="") as f:
            yield from csv.DictReader(f)

    @p.step
    def customers():
        import csv
        import os
        with open(os.path.join(DATA, "customers.csv"), newline="") as f:
            yield from csv.DictReader(f)

    p.join(
        name="joined",
        join_on={"orders": "cust", "customers": "cid"},
        join_mode="union",
    )
    p.run(workers=1)
    outs = list(_outputs("joined").values())
    matched = [o for o in outs if o.get("orders") and o.get("customers")]
    assert len(matched) == 1
    assert matched[0]["orders"]["oid"] == "o1"
    unmatched = [o for o in outs if o.get("customers") is None]
    assert len(unmatched) == 1
    assert unmatched[0]["orders"]["oid"] == "o2"


def test_join_mode_union_match_appears_orphans_missing_lane():
    """Acceptance 4b: when the missing side appears, @missing lane orphans
    and a new matched address is created (remove+add, not in-place)."""
    write_csv("a_csv.csv", "id,val\n1,A\n")
    write_csv("b_csv.csv", "id,val\n")  # empty — a is unmatched

    @step(name="a_csv", check_cache=False)
    def scan_a():
        path = os.path.join(DATA, "a_csv.csv")
        with open(path, newline="") as f:
            yield from csv.DictReader(f)

    @step(name="b_csv", check_cache=False)
    def scan_b():
        path = os.path.join(DATA, "b_csv.csv")
        with open(path, newline="") as f:
            yield from csv.DictReader(f)

    @step(name="a")
    def load_a(a_csv):
        return {"id": a_csv["id"], "v": a_csv["val"]}

    @step(name="b")
    def load_b(b_csv):
        return {"id": b_csv["id"], "v": b_csv["val"]}

    @step(
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
    )
    def merge(a, b):
        return {
            "a": None if a is None else a["v"],
            "b": None if b is None else b["v"],
        }

    pipe = pipeline(
        name="appear",
        steps=[scan_a, scan_b, load_a, load_b, merge],
        home=TEST_HOME,
    )
    s1 = assert_run(pipe)
    cells1 = {
        c.coordinate: c.output_address
        for c in TEST_HOME.select("step:merge", resolve_output=True)
        if c.output_address
    }
    assert len(cells1) == 1
    (missing_coord, missing_addr) = next(iter(cells1.items()))
    assert missing_coord.endswith("|@missing")
    # Unmatched edges only to the present side
    with TEST_HOME.session() as session:
        assert (
            session.query(MaterializationEdge)
            .filter_by(child_address=missing_addr)
            .count()
            == 1
        )

    write_csv("b_csv.csv", "id,val\n1,B\n")  # match appears
    s2 = assert_run(pipe)
    with TEST_HOME.session() as session:
        coords_r2 = {
            r.coordinate
            for r in session.query(RunCoordinateStatus).filter_by(
                run_id=s2.run_id, step_name="merge"
            )
            if r.status in ("created", "reused")
        }
    assert missing_coord not in coords_r2  # orphaned from this run
    assert any("@missing" not in c and "|" in c for c in coords_r2)
    cells2 = {
        c.coordinate: c.output_address
        for c in TEST_HOME.select("step:merge", resolve_output=True)
        if c.output_address and "@missing" not in c.coordinate
    }
    assert len(cells2) == 1
    (matched_coord, matched_addr) = next(iter(cells2.items()))
    assert matched_addr != missing_addr
    assert matched_coord != missing_coord
    # New matched lane edges to both sides
    with TEST_HOME.session() as session:
        assert (
            session.query(MaterializationEdge)
            .filter_by(child_address=matched_addr)
            .count()
            == 2
        )
    assert s1.created_count > 0


def test_join_input_hash_sentinel_on_extended_join_on():
    """Acceptance 6: every depends_on slot is hashed; extending join_on
    with an absent new side must not collide with the old 2-way address."""
    from rubedo.hashing import compute_output_address, hash_json
    from rubedo.planning import JOIN_MISSING, MatRef, _compute_join_input_hash

    @step(
        name="merge",
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
    )
    def merge2(a, b):
        return a

    @step(
        name="merge",
        depends_on=["a", "b", "c"],
        join_on={"a": "id", "b": "id", "c": "id"},
        join_mode="union",
    )
    def merge3(a, b, c):
        return a

    ref_a = MatRef("1", "addr-a", "hash-a")
    # 2-way: a present, b absent
    h2 = _compute_join_input_hash(merge2, {"a": ref_a})
    assert h2 == hash_json({"a": "hash-a", "b": JOIN_MISSING})
    # 3-way: a present, b+c absent — must differ (sentinel for c)
    h3 = _compute_join_input_hash(merge3, {"a": ref_a})
    assert h3 == hash_json({"a": "hash-a", "b": JOIN_MISSING, "c": JOIN_MISSING})
    assert h2 != h3
    assert compute_output_address("merge", "0", h2, "pipe") != compute_output_address(
        "merge", "0", h3, "pipe"
    )
    # All-present 2-way matches the plain multi-parent dict shape
    ref_b = MatRef("2", "addr-b", "hash-b")
    h2_full = _compute_join_input_hash(merge2, {"a": ref_a, "b": ref_b})
    assert h2_full == hash_json({"a": "hash-a", "b": "hash-b"})


def test_join_rejects_reserved_missing_parent_coordinate():
    from rubedo.planning import JOIN_MISSING, _plan_join
    from rubedo.planning import MatRef
    from unittest.mock import MagicMock

    @step(
        depends_on=["a", "b"],
        join_on={"a": "id", "b": "id"},
        join_mode="union",
    )
    def merge(a, b):
        return a

    home = MagicMock()
    session = MagicMock()
    ref = MatRef("1", "addr", "hash", output={"id": "1", "v": "A"})
    coord_step_mats = {
        (JOIN_MISSING, "a"): ref,
        ("row-b", "b"): MatRef("2", "addr-b", "hash-b", output={"id": "1", "v": "B"}),
    }
    with pytest.raises(ValueError, match="reserved"):
        _plan_join(
            session,
            home,
            merge,
            coord_step_mats,
            params_hash="",
            force=False,
            accepts_params=False,
            pipeline_id="p",
        )
