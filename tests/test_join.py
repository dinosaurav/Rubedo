import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import CsvSource, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, MaterializationEdge, RunCoordinateStatus, RunEvent
from rubedo.store import init_store, read_materialization_output

DATA = ".test_join_data"
ENV = ".test_join_env"


@pytest.fixture(autouse=True)
def isolated_env():
    dirs = [os.path.abspath(d) for d in (DATA, ENV)]
    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)
        os.makedirs(d, exist_ok=True)

    import rubedo.store

    rubedo.store.OBJECTS_DIR = f"{os.path.abspath(ENV)}/store/objects"
    rubedo.store.STAGING_DIR = f"{os.path.abspath(ENV)}/store/staging"

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

    for d in dirs:
        if os.path.exists(d):
            shutil.rmtree(d)


def write_csv(name, text):
    with open(os.path.join(DATA, name), "w") as f:
        f.write(text)


def assert_run(pipe, **kw):
    summary = run(pipe, workers=1, **kw)
    if summary.failed_count > 0:
        with get_session() as session:
            for e in (
                session.query(RunEvent)
                .filter_by(run_id=summary.run_id, level="error")
                .all()
            ):
                print(f"FAIL: {e.step_name}:{e.coordinate} -> {e.message}")
    return summary


def _outputs(step_name):
    result = {}
    with get_session() as session:
        for st in (
            session.query(RunCoordinateStatus)
            .filter_by(step_name=step_name)
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        ):
            mat = session.get(Materialization, st.materialization_id)
            if mat and mat.is_live:
                result[st.coordinate] = read_materialization_output(mat)
    return result


def test_two_way_equijoin():
    write_csv("orders.csv", "oid,cust\no1,c1\no2,c1\no3,c2\n")
    write_csv("customers.csv", "cid,name\nc1,Alice\nc2,Bob\n")

    @step(name="order", version="1", source="orders", index=["cust"])
    def order(row):
        return {"oid": row["oid"], "cust": row["cust"]}

    @step(name="customer", version="1", source="customers", index=["cid"])
    def customer(row):
        return {"cid": row["cid"], "name": row["name"]}

    @step(
        name="enrich", version="1", shape="join",
        depends_on=["order", "customer"],
        join_on={"order": "cust", "customer": "cid"},
    )
    def enrich(order, customer):
        return {"oid": order["oid"], "name": customer["name"]}

    pipe = pipeline(
        id="j", name="j",
        sources={
            "orders": CsvSource(os.path.join(DATA, "orders.csv"), key="oid"),
            "customers": CsvSource(os.path.join(DATA, "customers.csv"), key="cid"),
        },
        steps=[order, customer, enrich],
    )
    assert_run(pipe)

    outs = _outputs("enrich")
    # o1,o2 match Alice(c1); o3 matches Bob(c2)
    assert {v["oid"]: v["name"] for v in outs.values()} == {
        "o1": "Alice", "o2": "Alice", "o3": "Bob",
    }
    # each joined lane edges to both its sides
    with get_session() as session:
        mat = (
            session.query(Materialization)
            .filter_by(step_name="enrich", is_live=True)
            .first()
        )
        assert session.query(MaterializationEdge).filter_by(child_id=mat.id).count() == 2

    # re-run: joins reused (identity = the two sides' content)
    s2 = assert_run(pipe)
    assert s2.created_count == 0
    assert s2.reused_count > 0


def test_four_way_star_join():
    # four sources all keyed by the same uid value
    for src in ("a", "b", "c", "d"):
        write_csv(f"{src}.csv", f"uid,v\nu1,{src}1\nu2,{src}2\n")

    def loader(name):
        @step(name=name, version="1", source=name, index=["uid"])
        def load(row):
            return {"uid": row["uid"], "v": row["v"]}
        return load

    a, b, c, d = (loader(n) for n in ("a", "b", "c", "d"))

    @step(
        name="merge", version="1", shape="join",
        depends_on=["a", "b", "c", "d"],
        join_on={"a": "uid", "b": "uid", "c": "uid", "d": "uid"},
    )
    def merge(a, b, c, d):
        return "".join([a["v"], b["v"], c["v"], d["v"]])

    pipe = pipeline(
        id="star", name="star",
        sources={
            n: CsvSource(os.path.join(DATA, f"{n}.csv"), key="uid")
            for n in ("a", "b", "c", "d")
        },
        steps=[a, b, c, d, merge],
    )
    assert_run(pipe)

    outs = _outputs("merge")
    # one merged lane per shared uid (u1, u2)
    assert sorted(outs.values()) == ["a1b1c1d1", "a2b2c2d2"]


def test_join_requires_join_on():
    with pytest.raises(ValueError, match="requires join_on"):
        step(name="bad", version="1", shape="join", depends_on=["a", "b"])(
            lambda a, b: None
        )


def test_join_needs_two_parents():
    with pytest.raises(ValueError, match="at least two parents"):
        step(
            name="bad", version="1", shape="join",
            depends_on=["a"], join_on={"a": "k"},
        )(lambda a: None)


def test_join_on_must_match_depends_on():
    with pytest.raises(ValueError, match="must match depends_on"):
        step(
            name="bad", version="1", shape="join",
            depends_on=["a", "b"], join_on={"a": "k", "c": "k"},
        )(lambda a, b: None)
