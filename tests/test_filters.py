"""Filters: a step declines a coordinate by returning Filtered."""

import json
import os
import shutil
import uuid

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from rubedo import Filtered, plan, run, step, pipeline
from rubedo.db import init_db, get_session
from rubedo.models import Materialization, MaterializationIndexEntry, RunCoordinateStatus
from rubedo.store import init_store

TEST_FOLDER = ".test_filters_data"
ENV_FOLDER = ".test_filters_env"


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


@step(name="scan", version="1", shape="expand", index=["path"])
def scan():
    """Folder recipe (TODO 14): a root expand step yielding each file's
    content — the replacement for the old folder=TEST_FOLDER sugar.
    Indexed on `path` so tests can find "the lane for x.txt" without the
    coordinate being that literal string."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def build_pipeline(calls=None):
    """screen filters short files; summarize runs only on survivors."""
    calls = calls if calls is not None else []

    @step(name="screen", version="1", depends_on=["scan"])
    def screen(scan):
        calls.append(scan["path"])
        text = scan["text"]
        if len(text) < 10:
            return Filtered(reason=f"too short ({len(text)} chars)")
        return text

    @step(name="summarize", version="1", depends_on=["screen"])
    def summarize(screen):
        return screen.upper()

    pipe = pipeline(id="flt", name="flt", steps=[scan, screen, summarize])
    return pipe, calls


def statuses(step_name):
    with get_session() as session:
        return {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(step_name=step_name)
            .order_by(RunCoordinateStatus.id)
            .all()
        }


def coord_for_path(run_id, filename):
    """The coordinate a given run minted for `filename`, found via scan's
    indexed `path` field — coordinates are row-<hash>, not the filename.
    A dependent 1:1 map step (screen, summarize) shares its ancestor's
    coordinate all the way down the chain."""
    with get_session() as session:
        rows = (
            session.query(RunCoordinateStatus)
            .filter_by(run_id=run_id, step_name="scan")
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        )
        for rc in rows:
            hit = (
                session.query(MaterializationIndexEntry)
                .filter_by(
                    materialization_id=rc.materialization_id,
                    field="path",
                    value=filename,
                )
                .first()
            )
            if hit:
                return rc.coordinate
    return None


def test_filtered_coordinate_skips_downstream():
    create_file("long.txt", "long enough content here")
    create_file("short.txt", "tiny")
    pipe, _ = build_pipeline()

    summary = run(pipe, workers=1)
    # scan(long) + scan(short) [scan itself never filters] + screen(long) +
    # summarize(long)
    assert summary.created_count == 4
    assert summary.filtered_count == 2  # screen(short) + summarize(short)
    assert summary.failed_count == 0

    coord_long = coord_for_path(summary.run_id, "long.txt")
    coord_short = coord_for_path(summary.run_id, "short.txt")

    screen_rcs = statuses("screen")
    assert screen_rcs[coord_long].status == "created"
    assert screen_rcs[coord_short].status == "filtered"
    assert json.loads(screen_rcs[coord_short].metadata_json)["reason"].startswith(
        "too short"
    )

    sum_rcs = statuses("summarize")
    assert sum_rcs[coord_long].status == "created"
    assert sum_rcs[coord_short].status == "filtered"
    assert json.loads(sum_rcs[coord_short].metadata_json) == {
        "filtered_parents": ["screen"]
    }
    # Downstream never materialized anything for the filtered coordinate
    assert sum_rcs[coord_short].materialization_id is None


def test_filter_decision_is_cached():
    create_file("short.txt", "tiny")
    pipe, calls = build_pipeline([])

    run(pipe, workers=1)
    assert calls == ["short.txt"]

    summary = run(pipe, workers=1)
    assert calls == ["short.txt"], "cached verdict: filter step must not re-execute"
    assert summary.filtered_count == 2
    assert summary.created_count == 0

    with get_session() as session:
        mats = session.query(Materialization).all()
        assert len(mats) == 2  # scan's real lane + screen's filtered marker
        screen_mat = next(m for m in mats if m.step_name == "screen")
        assert screen_mat.filtered is True
        assert screen_mat.is_live is True


def test_content_change_reverses_the_verdict():
    create_file("f.txt", "tiny")
    pipe, _ = build_pipeline()
    summary1 = run(pipe, workers=1)
    assert summary1.filtered_count == 2

    # File grows past the threshold: new input hash, fresh decision
    create_file("f.txt", "now long enough to pass the filter")
    summary2 = run(pipe, workers=1)
    assert summary2.filtered_count == 0
    assert summary2.created_count == 3  # scan(f) + screen(f) + summarize(f)

    coord = coord_for_path(summary2.run_id, "f.txt")
    sum_rcs = statuses("summarize")
    assert sum_rcs[coord].status == "created"


def test_plan_shows_filtered_chain():
    """A headless param-fed root (not the folder-scan expand used above):
    an expand root's downstream lanes are opaque to plan() (see
    test_plan.py — plan() can only ever say "pending" past a root expand,
    never preview a specific coordinate's cached verdict). A single-file,
    param-fed "@root" lane keeps plan() able to preview screen's cached
    "reuse" and summarize's cascaded "filtered" verdict."""

    @step(name="screen", version="1")
    def screen(params):
        text = params["text"]
        if len(text) < 10:
            return Filtered(reason=f"too short ({len(text)} chars)")
        return text

    @step(name="summarize", version="1", depends_on=["screen"])
    def summarize(screen):
        return screen.upper()

    pipe = pipeline(id="flt2", name="flt2", steps=[screen, summarize])
    params = {"text": "tiny"}
    run(pipe, params=params, workers=1)

    p = plan(pipe, params=params)
    actions = {(i.coordinate, i.step_name): i.action for i in p.items}
    assert actions[("@root", "screen")] == "reuse"  # the verdict is cached
    assert actions[("@root", "summarize")] == "filtered"


def test_skip_cache_step_cannot_filter():
    create_file("f.txt", "anything")

    @step(name="util", version="1", skip_cache=True, depends_on=["scan"])
    def util(scan):
        return Filtered("nope")

    @step(name="use", version="1", depends_on=["util"])
    def use(util):
        return util

    pipe = pipeline(id="bad", name="bad", steps=[scan, util, use])
    summary = run(pipe, workers=1)
    assert summary.failed_count == 1

    with get_session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert "must be materialized" in rc.error_message
