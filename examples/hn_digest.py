"""Hacker News digest — a real fan-out/fan-in pipeline over two live APIs.

    top stories ─▶ screen ─▶ classify ─▶ digest (reduce)
                   (filter)   (LLM)        (LLM)

Real APIs, no mocks:
  - Hacker News Firebase API (public, no key)
  - An LLM via OpenRouter for the classification and the editor's note

Auth: put your key in a .env file at the repo root (already gitignored):

    OPENROUTER_API_KEY=sk-or-...

Then run it (zero extra dependencies — plain stdlib HTTP):

    uv run python examples/hn_digest.py

The point of doing this in Batchit: classifying a story with an LLM is
expensive and non-idempotent. Each classification is cached by the story's
content, so a second run reclassifies nothing and makes *zero* LLM calls —
and a story that gets filtered out is decided once, not once per run.
"""

import json
import os
import urllib.request

from pydantic import BaseModel

from batchbrain import Filtered, Source, SourceItem, describe, pipeline, run, step

HN = "https://hacker-news.firebaseio.com/v0"
OPENROUTER = "https://openrouter.ai/api/v1/chat/completions"
# Cheap + capable; override with OPENROUTER_MODEL to try another.
MODEL = os.environ.get("OPENROUTER_MODEL", "minimax/minimax-m2.5")


def _load_env():
    """Load KEY=VALUE lines from the nearest .env, without overriding real env vars."""
    d = os.path.dirname(os.path.abspath(__file__))
    for _ in range(4):
        path = os.path.join(d, ".env")
        if os.path.isfile(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip().strip("\"'"))
            return
        d = os.path.dirname(d)


_load_env()


def _get(url: str):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.load(r)


def _chat(prompt: str, max_tokens: int = 400, json_mode: bool = False) -> str:
    """One-shot chat completion via OpenRouter's OpenAI-compatible endpoint."""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise RuntimeError(
            "OPENROUTER_API_KEY is not set — put it in a .env file at the repo root."
        )
    payload = {"model": MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens}
    if json_mode:
        payload["response_format"] = {"type": "json_object"}
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        OPENROUTER,
        data=body,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)["choices"][0]["message"]["content"]


def _first_json(text: str) -> dict:
    """Pull the first {...} object out of a model reply (cheap models add prose)."""
    try:
        return json.loads(text[text.index("{") : text.rindex("}") + 1])
    except (ValueError, json.JSONDecodeError):
        return {}


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
    """Ask the LLM for a topic + one-line blurb. Expensive + non-idempotent → cached."""
    raw = _chat(
        "Classify this Hacker News headline. Respond with ONLY a JSON object like "
        '{"topic": "AI", "blurb": "one plain-English sentence"} — topic is one or two '
        "words.\n\n" + screen["title"],
        max_tokens=200,
        json_mode=True,
    )
    obj = _first_json(raw)
    return {
        "title": screen["title"],
        "topic": obj.get("topic", "unknown"),
        "blurb": obj.get("blurb", ""),
        "score": screen["score"],
    }


@step(name="digest", version="1", depends_on=["classify"], shape="reduce")
def digest(classify: dict) -> str:
    """Fan in every classified story and let the LLM write the editor's note."""
    stories = sorted(classify.values(), key=lambda s: s["score"], reverse=True)
    headlines = [f"- [{s['topic']}] {s['title']} ({s['score']} pts)" for s in stories]
    note = _chat(
        "In two punchy sentences, tell me what's on Hacker News today from these "
        "headlines:\n\n" + "\n".join(headlines),
        max_tokens=300,
    )
    return note.strip() + "\n\n" + "\n".join(headlines)


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
    print(
        "\nRun it again — classifications are cached, so nothing is re-classified. "
        "(The digest may re-run if HN's front page shifted; its inputs changed.)"
    )


if __name__ == "__main__":
    main()
