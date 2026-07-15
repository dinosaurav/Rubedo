"""Demo pipeline for observing live run progress in the dashboard.

A 7-step DAG with two parallel branches joining at a reduce, with
deliberately slow, bounded-random-time work so you have ~1-2 minutes to
watch the live topology animate:

         ┌→ fetch ──→ enrich ──────────────────────┐
scan ────┤                                         ├→ merge ──→ report
         └→ classify ──→ tag (stale_after=3s) ─────┘

    scan      (expand root, 8 lanes, 0.5-1.5s each)
    fetch     (map, 8 lanes, 2-6s each)
    enrich    (map, 8 lanes, 1-4s each)
    classify  (map, 8 lanes, 1-3s each)
    tag       (map, 8 lanes, 0.5-2s each, stale_after="3s")
    merge     (reduce, 1 lane, 1-2s)
    report    (reduce, 1 lane, 1-2s)

The two branches from scan run concurrently — fetch→enrich alongside
classify→tag — then join at merge. tag has stale_after="3s" so a second
run (without --force) will reuse some steps but re-execute tag because
its outputs expired.

Usage:
    uv run python private/demo_live.py [--force]
    rubedo serve          # then open http://127.0.0.1:8000
"""
import argparse
import random
import time

from rubedo import pipeline

p = pipeline(name="demo-live", schedule="deep")


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


@p.step
def classify(scan: dict):
    time.sleep(random.uniform(1.0, 3.0))
    category = "odd" if scan["id"] % 2 else "even"
    return {"id": scan["id"], "category": category}


@p.step(stale_after="3s")
def tag(classify: dict):
    time.sleep(random.uniform(0.5, 2.0))
    return {
        "id": classify["id"],
        "category": classify["category"],
        "tag": f"{classify['category']}-tag-{classify['id']}",
    }


@p.step(shape="reduce")
def merge(enrich: dict, tag: dict):
    return {
        "enriched": sorted(v["title"] for v in enrich.values()),
        "tagged": sorted(v["tag"] for v in tag.values()),
    }


@p.step(shape="reduce")
def report(merge: dict):
    data = merge["@all"]
    return {
        "total_enriched": len(data["enriched"]),
        "total_tagged": len(data["tagged"]),
        "items": data["enriched"],
        "tags": data["tagged"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Demo pipeline for live progress UI")
    parser.add_argument("--force", action="store_true", help="Force re-execution of all steps")
    args = parser.parse_args()

    print(p.describe())
    print()
    summary = p.run(force=args.force)
    print(f"\nRun ID: {summary.run_id}")
    print(f"Created: {summary.created_count}, Reused: {summary.reused_count}")
