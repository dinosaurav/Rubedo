"""Every producer shape in one pipeline: multi-source join → expand → reduce.

    feeds.csv ──────▶ feed ──────┐
                                 ├─▶ feed_meta ──▶ articles ──▶ digest
    publishers.csv ─▶ publisher ─┘   (join on       (expand      (reduce,
                                      publisher)     per article) group_key=region)

Two sources meet, fan out, and fold back:

  1. JOIN   feeds ⋈ publishers on the publisher name, so each feed learns its
            publisher's region        → pair lanes  `feed|publisher`
  2. EXPAND each feed into a lane per article ("scraping" it; cached so a
            re-run re-scrapes nothing) → minted lanes `feed|publisher/<art>`
  3. REDUCE the articles grouped by region with group_key="region"
                                      → one digest lane per region

Everything is self-contained: it writes two small CSVs to your temp dir and
"scrapes" from an in-memory table, so there are no network calls.

Run it:

    uv run python examples/newsroom/newsroom.py
"""

import csv
import os
import tempfile

from rubedo import CsvSource, describe, pipeline, run, step
from rubedo.db import get_session
from rubedo.models import Materialization, RunCoordinateStatus
from rubedo.store import read_materialization_output

FEEDS = [("f1", "TechCorp"), ("f2", "BizWire"), ("f3", "TechCorp")]
PUBLISHERS = [("TechCorp", "US"), ("BizWire", "EU")]

# What "scraping" a feed returns — deterministic, keyed by feed id.
FEED_ARTICLES = {
    "f1": ["GPU prices fall", "Chip roadmap leaks"],
    "f2": ["Markets rally", "IPO filed"],
    "f3": ["New language ships", "Framework 2.0 lands"],
}


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


@step(name="feed", version="1", source="feeds", index=["publisher"])
def feed(row: dict) -> dict:
    return {"feed_id": row["feed_id"], "publisher": row["publisher"]}


@step(name="publisher", version="1", source="publishers", index=["publisher"])
def publisher(row: dict) -> dict:
    return {"publisher": row["publisher"], "region": row["region"]}


@step(
    name="feed_meta", version="1", shape="join",
    depends_on=["feed", "publisher"],
    join_on={"feed": "publisher", "publisher": "publisher"},
)
def feed_meta(feed: dict, publisher: dict) -> dict:
    # the feed now carries its publisher's region
    return {"feed_id": feed["feed_id"], "region": publisher["region"]}


@step(
    name="articles", version="1", depends_on=["feed_meta"],
    shape="expand", index=["region"],
)
def articles(feed_meta: dict):
    fid = feed_meta["feed_id"]
    print(f"  scraping feed {fid} ...")  # runs once per feed, then cached
    for i, title in enumerate(FEED_ARTICLES[fid]):
        yield f"{fid}-{i}", {"title": title, "region": feed_meta["region"]}


@step(
    name="digest", version="1", depends_on=["articles"],
    shape="reduce", group_key="region",
)
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}


def make_pipeline(folder):
    write_csv(os.path.join(folder, "feeds.csv"), ["feed_id", "publisher"], FEEDS)
    write_csv(os.path.join(folder, "publishers.csv"), ["publisher", "region"], PUBLISHERS)
    return pipeline(
        id="newsroom",
        name="Newsroom",
        sources={
            "feeds": CsvSource(os.path.join(folder, "feeds.csv"), key="feed_id"),
            "publishers": CsvSource(os.path.join(folder, "publishers.csv"), key="publisher"),
        },
        steps=[feed, publisher, feed_meta, articles, digest],
    )


def print_digests():
    with get_session() as session:
        for st in (
            session.query(RunCoordinateStatus)
            .filter_by(step_name="digest")
            .filter(RunCoordinateStatus.materialization_id.isnot(None))
            .all()
        ):
            mat = session.get(Materialization, st.materialization_id)
            if mat and mat.is_live:
                out = read_materialization_output(mat)
                print(f"  [{st.coordinate}] {out['count']} articles: {out['headlines']}")


def main():
    folder = os.path.join(tempfile.gettempdir(), "rubedo_newsroom")
    os.makedirs(folder, exist_ok=True)
    pipe = make_pipeline(folder)

    print(describe(pipe))
    print()

    s1 = run(pipe)
    print(f"\nrun 1: created={s1.created_count} reused={s1.reused_count}")
    print_digests()

    s2 = run(pipe)
    print(f"\nrun 2: created={s2.created_count} reused={s2.reused_count}  (no feed re-scraped)")


if __name__ == "__main__":
    main()
