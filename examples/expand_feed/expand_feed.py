"""Fan one feed into a lane per article with an expand step.

    tech.json ─▶ fetch ─▶ articles (expand) ─▶ headline (map)
                          1:N — a lane per article

`expand` is the 1:N shape: the step yields payloads and each one mints its own
content-addressed downstream lane — `articles` is a generator, so the shape is
inferred. The whole expansion is cached against its parent — a "fetch"/scrape
runs once — so a re-run re-headlines nothing (notice no "fetching" line the
second time).

Run it:

    uv run python examples/expand_feed/expand_feed.py
"""

import json
import os
import tempfile

from rubedo import pipeline


def make_feed(folder):
    articles = [
        {"id": "a1", "title": "gpu prices fall"},
        {"id": "a2", "title": "new language ships"},
        {"id": "a3", "title": "startup raises seed"},
    ]
    with open(os.path.join(folder, "tech.json"), "w") as f:
        json.dump(articles, f)


p = pipeline(name="expand-feed")


@p.step
def feed_files():
    folder = os.path.join(tempfile.gettempdir(), "rubedo_expand_feed")
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            yield path


@p.step
def fetch(feed_files: str) -> list:
    print(f"  fetching {os.path.basename(feed_files)} ...")  # runs once, then cached
    return json.load(open(feed_files))


@p.step
def articles(fetch: list):
    for art in fetch:  # 1:N — yield a payload per article; content-addressed lanes
        yield art


@p.step
def headline(articles: dict) -> str:
    return articles["title"].upper()


def main():
    folder = os.path.join(tempfile.gettempdir(), "rubedo_expand_feed")
    os.makedirs(folder, exist_ok=True)
    make_feed(folder)

    print(p.describe())
    print()

    s1 = p.run()
    print(f"run 1: created={s1.created_count} reused={s1.reused_count}")
    s2 = p.run()
    print(f"run 2: created={s2.created_count} reused={s2.reused_count}  (fetch was cached)")
    
    print("\n--- Final Output (headline) ---")
    import json
    print(json.dumps(s2.output_for("headline"), indent=2, default=str))


if __name__ == "__main__":
    main()
