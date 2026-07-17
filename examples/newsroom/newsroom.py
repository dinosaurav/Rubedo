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

from rubedo import pipeline


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


FOLDER = os.path.join(tempfile.gettempdir(), "rubedo_newsroom")

p = pipeline(name="newsroom")


@p.step
def feeds():
    with open(os.path.join(FOLDER, "feeds.csv")) as f:
        for row in csv.DictReader(f):
            yield row


@p.step
def publishers():
    with open(os.path.join(FOLDER, "publishers.csv")) as f:
        for row in csv.DictReader(f):
            yield row


@p.step
def feed(feeds: dict) -> dict:
    return {"feed_id": feeds["feed_id"], "publisher": feeds["publisher"]}


@p.step
def publisher(publishers: dict) -> dict:
    return {"publisher": publishers["publisher"], "region": publishers["region"]}


@p.step(
    join_on={"feed": "publisher", "publisher": "publisher"},
)
def feed_meta(feed: dict, publisher: dict) -> dict:
    # the feed now carries its publisher's region
    return {"feed_id": feed["feed_id"], "region": publisher["region"]}


@p.step
def articles(feed_meta: dict):
    fid = feed_meta["feed_id"]
    print(f"  scraping feed {fid} ...")  # runs once per feed, then cached
    for title in FEED_ARTICLES[fid]:
        yield {"title": title, "region": feed_meta["region"]}  # yield payloads


@p.step(group_key="region")
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}


def main():
    os.makedirs(FOLDER, exist_ok=True)
    write_csv(os.path.join(FOLDER, "feeds.csv"), ["feed_id", "publisher"], FEEDS)
    write_csv(os.path.join(FOLDER, "publishers.csv"), ["publisher", "region"], PUBLISHERS)

    print(p.describe())
    print()

    s1 = p.run()
    print(f"\nrun 1: created={s1.created_count} reused={s1.reused_count}")

    s2 = p.run()
    print(f"\nrun 2: created={s2.created_count} reused={s2.reused_count}  (no feed re-scraped)")

    print("\n--- Final Output (digest) ---")
    import json
    print(json.dumps(s2.output_for("digest"), indent=2, default=str))


if __name__ == "__main__":
    main()
