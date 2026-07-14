"""Repo health report from the live GitHub REST API — a fan-in diamond.

    fetch_repo ─┐
                ├─▶ score ─▶ report (reduce)
    activity  ──┘

Real API: GitHub. Set GITHUB_TOKEN for the 5000/hr authenticated limit; it also
works unauthenticated (60/hr) for a quick look. No LLM here — this one is all
REST, to show the same engine over a very different shape of work.

Run it:

    uv run python examples/github_health/github_health.py

Each repo is a CSV row (a coordinate). fetch_repo and activity are two chained
network calls per repo; both carry retries and a rate limit because the GitHub
API is exactly the kind of flaky, quota'd dependency Rubedo is built around.
Re-running only re-hits GitHub for rows whose CSV entry changed.
"""

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from rubedo import ProcessResult, pipeline

from dotenv import load_dotenv
load_dotenv()

API = "https://api.github.com"


def _get(path: str):
    req = urllib.request.Request(f"{API}{path}", headers={"User-Agent": "rubedo-example"})
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


p = pipeline(name="repo-health")

@p.step(name="repos", version="1")
def repos():
    import csv
    with open(os.path.join(os.path.dirname(__file__), "repos.csv")) as f:
        for row in csv.DictReader(f):
            yield row

@p.step(name="fetch_repo", version="1", depends_on=["repos"], retries=3, retry_delay=2, rate_limit="20/min")
def fetch_repo(repos: dict) -> dict:
    """Repo metadata. row['repo'] is 'owner/name' from repos.csv."""
    row = repos
    r = _get(f"/repos/{row['repo']}")
    return {
        "repo": r["full_name"],
        "stars": r["stargazers_count"],
        "open_issues": r["open_issues_count"],
        "language": r.get("language") or "unknown",
    }


@p.step(name="activity", version="1", depends_on=["fetch_repo"], retries=3, rate_limit="20/min")
def activity(fetch_repo: dict) -> dict:
    """Commits in the last 30 days — a cheap liveness signal."""
    since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    commits = _get(f"/repos/{fetch_repo['repo']}/commits?since={since}&per_page=100")
    return {"commits_30d": len(commits)}


@p.step(name="score", version="1", depends_on=["fetch_repo", "activity"], index=["language"])
def score(fetch_repo: dict, activity: dict) -> ProcessResult:
    """A naive health score: reward stars and recent commits, penalize open issues."""
    health = (
        fetch_repo["stars"] // 100 + activity["commits_30d"] - fetch_repo["open_issues"] // 50
    )
    return ProcessResult(
        value={
            "repo": fetch_repo["repo"],
            "language": fetch_repo["language"],
            "stars": fetch_repo["stars"],
            "commits_30d": activity["commits_30d"],
            "health": health,
        },
        metadata={"health": health},
    )


@p.step(name="report", version="1", depends_on=["score"], shape="reduce")
def report(score: dict) -> str:
    """Fan in every repo's score into one ranked table."""
    rows = sorted(score.values(), key=lambda s: s["health"], reverse=True)
    lines = [
        f"{s['health']:>5}  {s['repo']:<28} {s['language']:<12} "
        f"stars={s['stars']} commits/30d={s['commits_30d']}"
        for s in rows
    ]
    return "Repo health (best first):\n" + "\n".join(lines)


def main():
    pipe = p
    print(pipe.describe())
    print()
    try:
        summary = pipe.run()
    except urllib.error.HTTPError as e:
        if e.code == 403:
            print("GitHub rate-limited you — set GITHUB_TOKEN and try again.")
            return
        raise
    print(f"created={summary.created_count} reused={summary.reused_count}")
    
    print("\n--- Final Output (report) ---")
    import json
    print(json.dumps(summary.output_for("report"), indent=2, default=str))


if __name__ == "__main__":
    main()
