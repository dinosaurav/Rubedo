"""Hacker News digest — a real fan-out/fan-in pipeline over two live APIs.

    top stories ─▶ screen ─▶ classify ─▶ digest (reduce)
                   (filter)   (Claude)     (Claude)

Real APIs, no mocks:
  - Hacker News Firebase API (public, no key)
  - Anthropic Claude for the classification and the editor's note
    (set ANTHROPIC_API_KEY in your environment)

Run it (pulls in the anthropic SDK just for this process):

    uv run --with anthropic python examples/hn_digest.py

The point of doing this in Batchit: classifying a story with an LLM is
expensive and non-idempotent. Each classification is cached by the story's
content, so a second run reclassifies nothing and makes *zero* Claude calls —
and a story that gets filtered out is decided once, not once per run.
"""

import json
import urllib.request

import anthropic
from pydantic import BaseModel

from batchbrain import Filtered, Source, SourceItem, describe, pipeline, run, step

HN = "https://hacker-news.firebaseio.com/v0"


def _get(url: str):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)


class HackerNewsTop(Source):
    """Top HN stories. Each story id is a coordinate; the payload is the story.

    content_hash is the id, so a story is classified once and reused forever —
    we deliberately don't re-fetch when its score drifts. Swap in a hash of the
    story body if you'd rather re-run when the content actually changes.
    """

    def __init__(self, limit: int = 15):
        self.limit = limit

    @property
    def id(self) -> str:
        return f"hn:top:{self.limit}"

    def scan(self):
        ids = _get(f"{HN}/topstories.json")[: self.limit]
        return [SourceItem(coordinate=str(i), content_hash=str(i), ref=i) for i in ids]

    def load(self, item: SourceItem) -> dict:
        return _get(f"{HN}/item/{item.ref}.json") or {}


class Screen(BaseModel):
    min_score: int = 100


class Topic(BaseModel):
    topic: str  # one or two words, e.g. "AI", "Databases", "Career"
    blurb: str  # one plain-English sentence


@step(name="screen", version="1", params_model=Screen)
def screen(story: dict, params: Screen) -> dict:
    """Drop low-signal stories before we spend any tokens on them."""
    if not story.get("title"):
        return Filtered("no title (probably a job post or deleted item)")
    if story.get("score", 0) < params.min_score:
        return Filtered(f"score {story.get('score', 0)} < {params.min_score}")
    return {"title": story["title"], "url": story.get("url", ""), "score": story["score"]}


@step(
    name="classify",
    version="1",
    depends_on=["screen"],
    retries=3,
    retry_delay=2,
    rate_limit="30/min",
    index=["topic"],  # search your outputs by topic: Selection.parse("topic:AI")
)
def classify(screen: dict) -> dict:
    """Ask Claude for a topic + one-line blurb. Expensive + non-idempotent → cached."""
    client = anthropic.Anthropic()
    resp = client.messages.parse(
        model="claude-opus-4-8",  # swap to claude-haiku-4-5 for cheaper runs
        max_tokens=512,
        messages=[
            {"role": "user", "content": f"Classify this Hacker News headline.\n\n{screen['title']}"}
        ],
        output_format=Topic,
    )
    t = resp.parsed_output
    return {
        "title": screen["title"],
        "topic": t.topic,
        "blurb": t.blurb,
        "score": screen["score"],
    }


@step(name="digest", version="1", depends_on=["classify"], shape="reduce")
def digest(classify: dict) -> str:
    """Fan in every classified story and let Claude write the editor's note."""
    stories = sorted(classify.values(), key=lambda s: s["score"], reverse=True)
    headlines = [f"- [{s['topic']}] {s['title']} ({s['score']} pts)" for s in stories]
    client = anthropic.Anthropic()
    resp = client.messages.create(
        model="claude-opus-4-8",
        max_tokens=512,
        messages=[
            {
                "role": "user",
                "content": "In two punchy sentences, tell me what's on Hacker News "
                "today from these headlines:\n\n" + "\n".join(headlines),
            }
        ],
    )
    note = next(b.text for b in resp.content if b.type == "text")
    return note + "\n\n" + "\n".join(headlines)


def make_pipeline():
    return pipeline(
        id="hn-digest",
        name="Hacker News Digest",
        source=HackerNewsTop(limit=15),
        steps=[screen, classify, digest],
    )


def main():
    pipe = make_pipeline()
    print(describe(pipe))
    print()
    summary = run(pipe, params={"min_score": 100})
    print(
        f"created={summary.created_count} reused={summary.reused_count} "
        f"filtered={summary.filtered_count}"
    )
    print("\nRun it again — every classification is cached, so it makes no Claude calls.")


if __name__ == "__main__":
    main()
