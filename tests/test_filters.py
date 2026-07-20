"""Filters: a step declines a coordinate by returning Filtered."""

import json
import os

import pytest

from rubedo import Filtered, step, pipeline
from rubedo.models import InputHashUsage, RunCoordinateStatus
from conftest import isolated_test_env

TEST_FOLDER = ".test_filters_data"
ENV_FOLDER = ".test_filters_env"

TEST_HOME = None


@pytest.fixture(autouse=True)
def isolated_env():
    global TEST_HOME
    with isolated_test_env("filters") as env:
        TEST_HOME = env.home
        yield

def create_file(name, content):
    with open(os.path.join(TEST_FOLDER, name), "w") as f:
        f.write(content)


@step(check_cache=False)
def scan():
    """Folder recipe: walk TEST_FOLDER, yield each file's content."""
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


def build_pipeline(calls=None):
    """screen filters short files; summarize runs only on survivors."""
    calls = calls if calls is not None else []

    @step
    def screen(scan):
        calls.append(scan["path"])
        text = scan["text"]
        if len(text) < 10:
            return Filtered(reason=f"too short ({len(text)} chars)")
        return text

    @step
    def summarize(screen):
        return screen.upper()

    pipe = pipeline(name="flt", steps=[scan, screen, summarize], home=TEST_HOME)
    return pipe, calls


def statuses(step_name):
    with TEST_HOME.session() as session:
        return {
            c.coordinate: c
            for c in session.query(RunCoordinateStatus)
            .filter_by(step_name=step_name)
            .order_by(RunCoordinateStatus.id)
            .all()
        }


def coord_for_path(run_id, filename):
    """The coordinate a given run minted for `filename`, found via scan's
    `path` output field — coordinates are row-<hash>, not the filename.
    A dependent 1:1 map step (screen, summarize) shares its ancestor's
    coordinate all the way down the chain."""
    cells = TEST_HOME.select(
        f"step:scan path:{filename}", run_id=run_id, resolve_output=True
    )
    assert cells, f"no lane for path={filename}"
    return cells[0].coordinate


def test_filtered_coordinate_skips_downstream():
    create_file("long.txt", "long enough content here")
    create_file("short.txt", "tiny")
    pipe, _ = build_pipeline()

    summary = pipe.run(workers=1)
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
    assert sum_rcs[coord_short].output_address is None


def test_filter_decision_is_cached():
    create_file("short.txt", "tiny")
    pipe, calls = build_pipeline([])

    pipe.run(workers=1)
    assert calls == ["short.txt"]

    summary = pipe.run(workers=1)
    assert calls == ["short.txt"], "cached verdict: filter step must not re-execute"
    assert summary.filtered_count == 2
    assert summary.created_count == 0

    with TEST_HOME.session() as session:
        rows = TEST_HOME.lanes.all_filled_rows()
        assert len(rows) == 3  # scan's real lane + root-anchor + screen's filtered marker
        screen_row = next(r for r in rows if r.get("step_name") == "screen")
        assert screen_row.get("filtered") is True
        ihu = session.query(InputHashUsage).filter_by(address=screen_row["address"]).first()
        assert ihu is not None and ihu.fulfilled is True


def test_content_change_reverses_the_verdict():
    create_file("f.txt", "tiny")
    pipe, _ = build_pipeline()
    summary1 = pipe.run(workers=1)
    assert summary1.filtered_count == 2

    # File grows past the threshold: new input hash, fresh decision
    create_file("f.txt", "now long enough to pass the filter")
    summary2 = pipe.run(workers=1)
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

    @step
    def screen(params):
        text = params["text"]
        if len(text) < 10:
            return Filtered(reason=f"too short ({len(text)} chars)")
        return text

    @step
    def summarize(screen):
        return screen.upper()

    pipe = pipeline(name="flt2", steps=[screen, summarize], home=TEST_HOME)
    params = {"text": "tiny"}
    pipe.run(params=params, workers=1)

    p = pipe.plan(params=params)
    actions = {(i.coordinate, i.step_name): i.action for i in p.items}
    assert actions[("@root", "screen")] == "reuse"  # the verdict is cached
    assert actions[("@root", "summarize")] == "filtered"


def test_skip_cache_step_cannot_filter():
    create_file("f.txt", "anything")

    @step(skip_cache=True)
    def util(scan):
        return Filtered("nope")

    @step
    def use(util):
        return util

    pipe = pipeline(name="bad", steps=[scan, util, use], home=TEST_HOME)
    summary = pipe.run(workers=1)
    assert summary.failed_count == 1

    with TEST_HOME.session() as session:
        rc = session.query(RunCoordinateStatus).filter_by(step_name="use").one()
        assert "must be materialized" in rc.error_message
