"""Readability stats for public-domain books — real download + CPU parallelism.

    books.csv ─▶ fetch ─▶ clean ─▶ analyze ─▶ report (reduce)
                 (HTTP)   (skip_   (process    (rank)
                          cache)    executor)

Real API: Project Gutenberg (public, no key). Then two Rubedo features worth
showing together:

  - `clean` is skip_cache=True: a quick, idempotent helper that strips the
    Gutenberg boilerplate. It is never materialized — its identity fuses into
    analyze's cache key and it runs in-memory only when analyze actually runs.
  - `analyze` is executor="process": the token crunching is CPU-bound, so it
    runs in a process pool for real parallelism (module-level function, as the
    process executor requires).

Run it:

    uv run python examples/gutenberg_stats/gutenberg_stats.py

Downloads are cached as materializations, so a re-run re-analyzes nothing.
"""

import os
import re
import urllib.request

from rubedo import pipeline


GUTENBERG = "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"

p = pipeline(name="gutenberg-stats")


@p.step
def books():
    import csv
    with open(os.path.join(os.path.dirname(__file__), "books.csv")) as f:
        for row in csv.DictReader(f):
            yield row


@p.step(retries=3, retry_delay=2, rate_limit="10/min")
def fetch(books: dict) -> dict:
    """Download one book. row is {id, title} from books.csv."""
    row = books
    url = GUTENBERG.format(id=row["id"])
    req = urllib.request.Request(url, headers={"User-Agent": "rubedo-example"})
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode("utf-8", errors="replace")
    return {"title": row["title"], "text": text}


@p.step(skip_cache=True)
def clean(fetch: dict) -> dict:
    """Strip the *** START/END *** Gutenberg boilerplate. Quick, pure, inline."""
    text = fetch["text"]
    start = re.search(r"\*\*\* START OF.*?\*\*\*", text, re.S)
    end = re.search(r"\*\*\* END OF.*?\*\*\*", text, re.S)
    body = text[start.end() : end.start()] if start and end else text
    return {"title": fetch["title"], "text": body}


@p.step(executor="process", index=["longest_word"])
def analyze(clean: dict) -> dict:
    """CPU-bound token crunching, run in a worker process."""
    words = re.findall(r"[a-zA-Z']+", clean["text"].lower())
    total = len(words)
    unique = len(set(words))
    longest = max(words, key=len) if words else ""
    avg_len = round(sum(len(w) for w in words) / total, 2) if total else 0
    return {
        "title": clean["title"],
        "words": total,
        "unique": unique,
        "lexical_diversity": round(unique / total, 3) if total else 0,
        "avg_word_len": avg_len,
        "longest_word": longest,
    }


@p.step(depends_on=["analyze"], shape="reduce")
def report(analyze: dict) -> str:
    """Rank books by lexical diversity."""
    rows = sorted(analyze.values(), key=lambda s: s["lexical_diversity"], reverse=True)
    lines = [
        f"{s['lexical_diversity']:.3f}  {s['title']:<38} "
        f"{s['words']:>7} words, {s['unique']:>6} unique"
        for s in rows
    ]
    return "Books by lexical diversity (richest first):\n" + "\n".join(lines)


def main():
    print(p.describe())
    print()
    summary = p.run()
    print(f"created={summary.created_count} reused={summary.reused_count}")
    print("\n--- Final Output (report) ---")
    import json
    print(json.dumps(summary.output_for("report"), indent=2, default=str))


if __name__ == "__main__":
    main()
