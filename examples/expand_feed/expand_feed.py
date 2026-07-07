"""Fan one feed into a lane per article with shape="expand".

    tech.json ─▶ fetch ─▶ articles (expand) ─▶ headline (map)
                          1:N — a lane per article

`expand` is the 1:N shape: the step yields (subkey, value) pairs and each pair
becomes its own downstream lane. The whole expansion is cached against its
parent — a "fetch"/scrape runs once — so a re-run re-headlines nothing (notice
no "fetching" line the second time).

Run it:

    uv run python examples/expand_feed/expand_feed.py
"""

import json
import os
import tempfile

from rubedo import FolderSource, describe, PipelineBuilder, run


def make_feed(folder):
    articles = [
        {"id": "a1", "title": "gpu prices fall"},
        {"id": "a2", "title": "new language ships"},
        {"id": "a3", "title": "startup raises seed"},
    ]
    with open(os.path.join(folder, "tech.json"), "w") as f:
        json.dump(articles, f)


p = PipelineBuilder(id="expand-feed", name="Expand Feed")


@p.step(name="fetch", version="1")
def fetch(path: str) -> list:
    print(f"  fetching {os.path.basename(path)} ...")  # runs once, then cached
    return json.load(open(path))


@p.step(name="articles", version="1", depends_on=["fetch"], shape="expand")
def articles(fetch: list):
    for art in fetch:  # 1:N — yield a payload per article; content-addressed lanes
        yield art


@p.step(name="headline", version="1", depends_on=["articles"])
def headline(articles: dict) -> str:
    return articles["title"].upper()


def main():
    folder = os.path.join(tempfile.gettempdir(), "rubedo_expand_feed")
    os.makedirs(folder, exist_ok=True)
    make_feed(folder)

    pipe = p.build(source=FolderSource(folder))
    print(describe(pipe))
    print()

    s1 = run(pipe)
    print(f"run 1: created={s1.created_count} reused={s1.reused_count}")
    s2 = run(pipe)
    print(f"run 2: created={s2.created_count} reused={s2.reused_count}  (fetch was cached)")


if __name__ == "__main__":
    main()
