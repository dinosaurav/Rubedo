"""Hacker News digest — a real fan-out/fan-in pipeline over two live APIs.

    top_story ─▶ screen ─▶ classify ─▶ digest (reduce)
    (source)     (fetch+    (LLM)        (LLM)
                  filter)

Real APIs, no mocks:
  - Hacker News Firebase API (public, no key)
  - An LLM via OpenRouter for the classification and the editor's note

Auth: put your key in a .env file at the repo root (already gitignored):

    OPENROUTER_API_KEY=sk-or-...

Then run it (zero extra dependencies — plain stdlib HTTP):

    uv run python examples/hn_digest/hn_digest.py

The point of doing this in Rubedo: classifying a story with an LLM is
expensive and non-idempotent. Each classification is cached by the story's
content, so a second run reclassifies nothing and makes *zero* LLM calls —
and a story that gets filtered out is decided once, not once per run.

The `top_story` root only yields a story's id — a stable coordinate that
never changes. `screen` (its only consumer) does the actual HN item fetch,
so it only ever runs once per story: later runs reuse its cached output
without refetching, which means a story's score drifting after that first
fetch is *never* revisited. That's deliberate — it's what keeps `classify`
(the expensive LLM call) from re-running just because a score ticked up.
"""

import json
import os
import urllib.request

from pydantic import BaseModel
from dotenv import load_dotenv

from rubedo import Filtered, pipeline

load_dotenv()

HN = "https://hacker-news.firebaseio.com/v0"
OPENROUTER = "https://openrouter.ai/api/v1/chat/completions"
# Cheap + capable; override with OPENROUTER_MODEL to try another.
MODEL = os.environ.get("OPENROUTER_MODEL", "minimax/minimax-m2.5")


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
        return json.load(r)["choices"][0]["message"]["content"] or ""


def _first_json(text: str) -> dict:
    """Pull the first {...} object out of a model reply (cheap models add prose)."""
    try:
        return json.loads(text[text.index("{") : text.rindex("}") + 1])
    except (ValueError, json.JSONDecodeError):
        return {}


TOP_LIMIT = 15


class Screen(BaseModel):
    min_score: int = 100


p = pipeline(name="hn-digest")


@p.step()
def top_story():
    """Root: today's top story ids. Just the id — a stable, score-independent
    coordinate — so `screen` (the actual fetch) runs at most once per story."""
    for story_id in _get(f"{HN}/topstories.json")[:TOP_LIMIT]:
        yield {"id": story_id}


@p.step(params_model=Screen)
def screen(top_story: dict, params: Screen) -> dict | Filtered:
    """Fetch the story and drop low-signal ones before spending any tokens."""
    story = _get(f"{HN}/item/{top_story['id']}.json") or {}
    if not story.get("title"):
        return Filtered("no title (probably a job post or deleted item)")
    if story.get("score", 0) < params.min_score:
        return Filtered(f"score {story.get('score', 0)} < {params.min_score}")
    return {"title": story["title"], "url": story.get("url", ""), "score": story["score"]}


@p.step(
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


@p.step(depends_on=["classify"], shape="reduce")
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


def main():
    print(p.describe())
    print()
    summary = p.run(params={"min_score": 100})
    print(
        f"created={summary.created_count} reused={summary.reused_count} "
        f"filtered={summary.filtered_count}"
    )
    print("\n--- Final Output (digest) ---")
    import json
    print(json.dumps(summary.output_for("digest"), indent=2, default=str))
    print(
        "\nRun it again — classifications are cached, so nothing is re-classified. "
        "(The digest may re-run if HN's front page shifted; its inputs changed.)"
    )


if __name__ == "__main__":
    main()
