"""Demo pipeline for observing live run progress in the dashboard.

A 4-step DAG with deliberately slow, bounded-random-time work so you have
~1-2 minutes to watch the live topology animate:

    scan → fetch → enrich → aggregate

    scan      (expand root, 8 lanes, 0.5-1.5s each)
    fetch     (map, 8 lanes, 2-6s each)
    enrich    (map, 8 lanes, 1-4s each)
    aggregate (reduce, 1 lane, 1-2s)

Usage:
    uv run python private/demo_live.py
    rubedo serve          # then open http://127.0.0.1:8000
"""
import random
import time

from rubedo import pipeline

p = pipeline(name="demo-live")


@p.step
def scan():
    for i in range(8):
        time.sleep(random.uniform(0.5, 1.5))
        yield {"id": i, "url": f"https://demo.example.com/item/{i}"}


@p.step
def fetch(scan: dict):
    time.sleep(random.uniform(2.0, 6.0))
    return {"id": scan["id"], "url": scan["url"], "body": f"<html>page {scan['id']}</html>"}


@p.step
def enrich(fetch: dict):
    time.sleep(random.uniform(1.0, 4.0))
    return {
        "id": fetch["id"],
        "url": fetch["url"],
        "title": f"Item {fetch['id']}",
        "word_count": len(fetch["body"].split()),
    }


@p.step(shape="reduce")
def aggregate(enrich: dict):
    return {
        "total_items": len(enrich),
        "total_words": sum(v["word_count"] for v in enrich.values()),
        "ids": sorted(v["id"] for v in enrich.values()),
    }


if __name__ == "__main__":
    print(p.describe())
    print()
    summary = p.run()
    print(f"\nRun ID: {summary.run_id}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")
