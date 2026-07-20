"""Paper scout — sample a rate-limited API enrichment before full rollout.

    discover ─▶ fetch_work ─▶ reading_list
    (search)      (12/min)    └▶ assess v1 ─┐
                                      v2 ──┴▶ run diff

This keyless example queries OpenAlex for recent research, fetches full metadata
for a deterministic two-paper pilot, then runs the complete six-paper batch.
The full run reuses the pilot's fetches instead of calling OpenAlex again. It
then trials an access-aware shortlist policy against a citations-only baseline,
prints the cohort-aware diff, and rolls out the accepted policy.

Run from the repository root:

    uv run python examples/paper_scout/paper_scout.py

Configuration:

    PAPER_SCOUT_QUERY="retrieval augmented generation"
    PAPER_SCOUT_LIMIT=6
    PAPER_SCOUT_SAMPLE=2
    PAPER_SCOUT_SAMPLE_ONLY=1   # inspect the pilot without rolling out
    OPENALEX_EMAIL=you@example.com  # optional polite-pool identifier
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

from pydantic import BaseModel, Field

from rubedo import RunScope, pipeline, step

OPENALEX = "https://api.openalex.org"


def _get_json(url: str):
    email = os.environ.get("OPENALEX_EMAIL", "")
    agent = "rubedo-paper-scout/1.0"
    if email:
        agent += f" (mailto:{email})"
    request = urllib.request.Request(url, headers={"User-Agent": agent})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


class ScoutParams(BaseModel):
    query: str = "retrieval augmented generation"
    limit: int = Field(default=6, ge=1, le=20)


p = pipeline(name="paper-scout", params_model=ScoutParams)


@p.step(check_cache=False)
def discover(params: dict):
    """Search afresh, yielding stable OpenAlex ids rather than full records."""
    query = urllib.parse.urlencode({
        "search": params["query"],
        "per-page": params["limit"],
        "select": "id",
    })
    payload = _get_json(f"{OPENALEX}/works?{query}")
    for work in payload.get("results", []):
        if work.get("id"):
            yield {"openalex_id": work["id"].rsplit("/", 1)[-1]}


@p.step(
    retries=3,
    retry_on=(urllib.error.URLError, TimeoutError),
    retry_delay=1,
    rate_limit="12/min",
)
def fetch_work(discover: dict) -> dict:
    """The constrained call: at most one request every five seconds."""
    work = _get_json(f"{OPENALEX}/works/{discover['openalex_id']}")
    authors = [
        authorship["author"]["display_name"]
        for authorship in work.get("authorships", [])[:3]
        if authorship.get("author", {}).get("display_name")
    ]
    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    return {
        "openalex_id": discover["openalex_id"],
        "title": work.get("title") or "(untitled)",
        "year": work.get("publication_year"),
        "authors": authors,
        "citations": work.get("cited_by_count", 0),
        "open_access": bool((work.get("open_access") or {}).get("is_oa")),
        "venue": source.get("display_name") or "",
        "url": primary_location.get("landing_page_url") or work.get("id"),
    }


@p.step(in_shape="aggregate")
def reading_list(fetch_work: dict) -> str:
    """Compile the complete batch; a pilot aggregate could never impersonate it."""
    works = sorted(
        fetch_work.values(),
        key=lambda work: (work["citations"], work["year"] or 0),
        reverse=True,
    )
    lines = []
    for work in works:
        access = "open" if work["open_access"] else "closed"
        authors = ", ".join(work["authors"]) or "unknown authors"
        lines.append(
            f"- {work['title']} ({work['year']}; {work['citations']} citations; "
            f"{access})\n  {authors}\n  {work['url']}"
        )
    return "\n".join(lines)


@step(name="assess", version="1")
def assess_v1(fetch_work: dict) -> dict:
    """Baseline policy: only heavily cited papers enter the shortlist."""
    return {
        "openalex_id": fetch_work["openalex_id"],
        "title": fetch_work["title"],
        "decision": "read" if fetch_work["citations"] >= 300 else "skip",
        "policy": "citations-only",
    }


@step(name="assess", version="2")
def assess_v2(fetch_work: dict) -> dict:
    """Candidate policy: give accessible emerging work a fairer chance."""
    citations = fetch_work["citations"]
    should_read = citations >= 200 or (
        fetch_work["open_access"] and citations >= 100
    )
    return {
        "openalex_id": fetch_work["openalex_id"],
        "title": fetch_work["title"],
        "decision": "read" if should_read else "skip",
        "policy": "access-aware",
    }


def _assessment_pipeline(assessment):
    """Use the same pipeline identity so fetched metadata remains reusable."""
    return pipeline(
        name="paper-scout",
        steps=[discover, fetch_work, assessment],
        params_model=ScoutParams,
        home=p.home,
    )


def main():
    params = {
        "query": os.environ.get(
            "PAPER_SCOUT_QUERY", "retrieval augmented generation"
        ),
        "limit": int(os.environ.get("PAPER_SCOUT_LIMIT", "6")),
    }
    sample_size = int(os.environ.get("PAPER_SCOUT_SAMPLE", "2"))

    print(p.describe())
    print(f"\nDiscovering {params['limit']} papers about {params['query']!r}…")
    baseline = p.run(params=params, targets=[discover], workers=1)
    if baseline.failed_count or baseline.blocked_count:
        raise RuntimeError(f"discovery failed: {baseline.failures()}")
    candidates = baseline.cells("discover", status=("created", "reused"))
    if not candidates:
        raise RuntimeError("OpenAlex returned no papers for this query")

    scope = RunScope.sample_n(
        anchor=fetch_work,
        cells=candidates,
        n=min(sample_size, len(candidates)),
        seed=f"paper-scout:{params['query']}",
        origin={"from_run": baseline.run_id},
    )
    print(
        f"Fetching a deterministic {len(scope.lanes)}-paper pilot at 12/min "
        f"(run {baseline.run_id})…"
    )
    trial = p.run(
        params=params,
        scope=scope,
        targets=[fetch_work],
        workers=4,
    )
    if trial.failed_count or trial.blocked_count:
        raise RuntimeError(f"pilot failed: {trial.failures()}")
    for work in trial.output_for("fetch_work").values():
        print(
            f"  • {work['title']} — {work['citations']} citations"
            f"{' — open access' if work['open_access'] else ''}"
        )

    if os.environ.get("PAPER_SCOUT_SAMPLE_ONLY") == "1":
        print("\nPilot complete. Unset PAPER_SCOUT_SAMPLE_ONLY to roll out.")
        return

    print("\nRolling out the full cohort; pilot fetches will be cache hits…")
    full = p.run(params=params, workers=4)
    if full.failed_count or full.blocked_count:
        raise RuntimeError(f"rollout failed: {full.failures()}")
    pilot_addresses = {
        cell.output_address
        for cell in trial.cells("fetch_work")
        if cell.output_address is not None
    }
    pilot_reused = sum(
        cell.status == "reused" and cell.output_address in pilot_addresses
        for cell in full.cells("fetch_work")
    )
    print(
        f"created={full.created_count} reused={full.reused_count} "
        f"(pilot fetches reused={pilot_reused}/{len(pilot_addresses)})"
    )
    print("\n--- Reading list ---")
    outputs = full.output_for("reading_list")
    print(next(iter(outputs.values()), "(empty)"))

    print("\n--- Shortlist policy A/B ---")
    policy_v1 = _assessment_pipeline(assess_v1)
    policy_baseline = policy_v1.run(params=params, workers=4)
    if policy_baseline.failed_count or policy_baseline.blocked_count:
        raise RuntimeError(f"policy baseline failed: {policy_baseline.failures()}")

    policy_v2 = _assessment_pipeline(assess_v2)
    policy_scope = RunScope.sample_n(
        anchor=assess_v2,
        cells=policy_baseline.cells("assess"),
        n=min(sample_size, len(policy_baseline.cells("assess"))),
        seed=f"paper-scout-policy:{params['query']}",
        origin={"from_run": policy_baseline.run_id},
    )
    policy_trial = policy_v2.run(
        params=params,
        scope=policy_scope,
        targets=[assess_v2],
        workers=4,
    )
    if policy_trial.failed_count or policy_trial.blocked_count:
        raise RuntimeError(f"policy trial failed: {policy_trial.failures()}")

    comparison = policy_baseline.diff(policy_trial, step="assess")
    print(comparison)

    print("\nRolling out policy v2; sampled assessments will be cache hits…")
    policy_full = policy_v2.run(params=params, workers=4)
    if policy_full.failed_count or policy_full.blocked_count:
        raise RuntimeError(f"policy rollout failed: {policy_full.failures()}")

    recent = p.home.runs(pipeline="paper-scout", limit=5)
    print(
        "recent runs: "
        + ", ".join(f"{item.kind}/{item.status}" for item in recent)
    )


if __name__ == "__main__":
    main()
