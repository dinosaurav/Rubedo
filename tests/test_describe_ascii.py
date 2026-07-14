"""describe(format="ascii"): hand-rolled terminal DAG rendering.

Pure spec-level tests — describe() never touches the ledger/store, so
these build PipelineSpecs directly with @step and skip the usual
DB/store fixture entirely.
"""

import pytest

from rubedo import pipeline


def _count_lines_shaped():
    """A linear chain: expand root -> map -> map -> reduce, mirroring
    examples/count_lines/count_lines.py's DAG shape."""
    p = pipeline(name="count-lines")

    @p.step(name="input_files", version="1")
    def input_files():
        yield "a.txt"

    @p.step(name="read_lines", version="v1", depends_on=["input_files"])
    def read_lines(input_files):
        return {"lines": []}

    @p.step(name="count_lines", version="v1", depends_on=["read_lines"])
    def count_lines(read_lines):
        return {}

    @p.step(name="total_lines", version="v1", depends_on=["count_lines"], shape="reduce")
    def total_lines(count_lines):
        return 0

    return p


def _newsroom_shaped():
    """join -> expand -> group_key reduce, mirroring
    examples/newsroom/newsroom.py's DAG shape."""
    p = pipeline(name="newsroom")

    @p.step(name="feeds", version="1")
    def feeds():
        yield {"feed_id": "f1", "publisher": "TechCorp"}

    @p.step(name="publishers", version="1")
    def publishers():
        yield {"publisher": "TechCorp", "region": "US"}

    @p.step(name="feed", version="1", depends_on=["feeds"], index=["publisher"])
    def feed(feeds):
        return feeds

    @p.step(name="publisher", version="1", depends_on=["publishers"], index=["publisher"])
    def publisher(publishers):
        return publishers

    @p.step(
        name="feed_meta", version="1", shape="join",
        depends_on=["feed", "publisher"],
        join_on={"feed": "publisher", "publisher": "publisher"},
    )
    def feed_meta(feed, publisher):
        return {}

    @p.step(
        name="articles", version="1", depends_on=["feed_meta"],
        shape="expand", index=["region"],
    )
    def articles(feed_meta):
        yield {}

    @p.step(
        name="digest", version="1", depends_on=["articles"],
        shape="reduce", group_key="region",
    )
    def digest(articles):
        return {}

    return p


COUNT_LINES_ASCII = (
    "Pipeline 'count-lines'\n"
    "┌──────────────────────┐\n"
    "│ input_files [expand] │\n"
    "└──────────────────────┘\n"
    "       ┌────┘\n"
    "┌────────────┐\n"
    "│ read_lines │\n"
    "└────────────┘\n"
    "       │\n"
    "┌─────────────┐\n"
    "│ count_lines │\n"
    "└─────────────┘\n"
    "       └────┐\n"
    "┌──────────────────────┐\n"
    "│ total_lines [reduce] │\n"
    "└──────────────────────┘"
)

NEWSROOM_ASCII = (
    "Pipeline 'newsroom'\n"
    "┌────────────────┐  ┌─────────────────────┐\n"
    "│ feeds [expand] │  │ publishers [expand] │\n"
    "└────────────────┘  └─────────────────────┘\n"
    "    ┌────┘      ┌──────────────┘\n"
    "┌──────┐  ┌───────────┐\n"
    "│ feed │  │ publisher │\n"
    "└──────┘  └───────────┘\n"
    "    └─────┐     │\n"
    "          ├─────┘\n"
    "┌──────────────────┐\n"
    "│ feed_meta [join] │\n"
    "└──────────────────┘\n"
    "          │\n"
    "┌───────────────────┐\n"
    "│ articles [expand] │\n"
    "└───────────────────┘\n"
    "         ┌┘\n"
    "┌─────────────────┐\n"
    "│ digest [reduce] │\n"
    "└─────────────────┘"
)


def test_ascii_count_lines_shaped_is_byte_identical():
    pipe = _count_lines_shaped()
    assert pipe.describe(format="ascii") == COUNT_LINES_ASCII
    # every step name present
    for name in ("input_files", "read_lines", "count_lines", "total_lines"):
        assert name in COUNT_LINES_ASCII
    # non-map shape tags present
    assert "[expand]" in COUNT_LINES_ASCII
    assert "[reduce]" in COUNT_LINES_ASCII


def test_ascii_newsroom_shaped_is_byte_identical():
    pipe = _newsroom_shaped()
    assert pipe.describe(format="ascii") == NEWSROOM_ASCII
    for name in ("feeds", "publishers", "feed", "publisher", "feed_meta", "articles", "digest"):
        assert name in NEWSROOM_ASCII
    assert "[join]" in NEWSROOM_ASCII
    assert "[expand]" in NEWSROOM_ASCII
    assert "[reduce]" in NEWSROOM_ASCII


def test_ascii_is_deterministic_across_calls():
    pipe = _newsroom_shaped()
    first = pipe.describe(format="ascii")
    second = pipe.describe(format="ascii")
    assert first == second


def test_ascii_falls_back_to_text_when_a_layer_is_too_wide():
    pipe = pipeline(name="wide")
    for i in range(15):
        def _make(i):
            @pipe.step(name=f"step_number_{i:02d}", version="1")
            def s(**kwargs):
                return {}
            return s
        _make(i)

    ascii_out = pipe.describe(format="ascii")
    text_out = pipe.describe(format="text")
    assert ascii_out == text_out
    # sanity: the wide layer really is over the ascii width budget
    assert len(pipe.spec.steps) == 15


def test_unknown_format_raises_and_lists_all_three():
    pipe = _count_lines_shaped()
    with pytest.raises(ValueError, match="expected 'text', 'mermaid', or 'ascii'"):
        pipe.describe(format="bogus")


# ---------- format=None: TTY-vs-piped default (TODO 24) ----------
#
# pytest captures stdout, so sys.stdout.isatty() is already False in this
# whole suite — describe() with no format= has always exercised the
# "piped" branch here, which is exactly the behavior these tests pin down
# explicitly (and why no other test in the suite needed to change).


def test_default_format_is_text_when_stdout_is_not_a_tty(monkeypatch):
    pipe = _count_lines_shaped()
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    assert pipe.describe() == pipe.describe(format="text")


def test_default_format_is_ascii_when_stdout_is_a_tty(monkeypatch):
    pipe = _count_lines_shaped()
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    assert pipe.describe() == pipe.describe(format="ascii") == COUNT_LINES_ASCII


def test_explicit_format_wins_over_tty_autodetection(monkeypatch):
    pipe = _count_lines_shaped()
    # Even when stdout *looks* like a TTY, an explicit format= always wins.
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)
    assert pipe.describe(format="text") != COUNT_LINES_ASCII
    monkeypatch.setattr("sys.stdout.isatty", lambda: False)
    assert pipe.describe(format="ascii") == COUNT_LINES_ASCII
