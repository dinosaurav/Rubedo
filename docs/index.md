# Rubedo docs

**A Python library for batch pipelines.** You write steps as ordinary
functions. Rubedo stores every result and only recomputes what changed —
so fixing the last step doesn't re-pay a thousand LLM calls, scrapes, or
APIs.

> **At a glance.** Local-first library, not an orchestrator: DAG pipelines
> over keyed collections with content-addressed row-level caching, an
> append-only run ledger, and surgical invalidation. Think dbt-style state
> for Python tasks. Every output lives at
> `hash(step, code_version, input_hash, pipeline)`. State lives in
> `.rubedo/`. Pre-1.0, MIT licensed.

```bash
pip install rubedo
```

## A pipeline is two functions

No API key. A folder of files in, a line count out — the same shape as
the [tutorial](tutorial.md) and the README. When the last step *is* an
LLM, this is still the graph: a source that yields items, a map that
spends, a second run that doesn't. That paid version is the
[landing-page walkthrough](https://rubedo.run/).

```python
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step(check_cache=False)   # rescan the folder every run
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):  # argument name = parent step
    return {"line_count": len(scan["text"].splitlines())}

print(p.plan())               # dry-run: what would recompute, and why
summary = p.run()
print(summary.created_count, summary.reused_count)
```

**What this is doing**

1. **`scan`** lists a folder and yields one item per file.
   `check_cache=False` re-reads every run, so new and edited files show up.
2. **`count_lines`** runs once per file. The argument name `scan` is the
   parent — no YAML, no DAG file.
3. **`plan()`, then `run()`.** `plan()` writes nothing. `run()` executes.
   Run it twice: first run creates; second run reuses.

Look at the run in a browser: `rubedo serve` (needs
`pip install "rubedo[server]"`) — covered in
[Inspect a run](guides/inspecting-runs.md).

## Read in this order

1. **[Tutorial](tutorial.md)** — install, build a classifier, query by
   content, edit an input, bump a version, invalidate a selection. What
   `plan()` prints lives there.
2. **[How it works](concepts/model.md)** — lanes, addresses, shapes,
   sources, the ledger, the four promises.
3. **[Examples](examples.md)** — the same ideas against real services.

How-to jobs (retries, joins, invalidation, the dashboard, trials, sharing
the cache) sit under **How to** in the sidebar. Signatures:
**[API](reference/api/index.md)** and **[CLI](reference/cli.md)**.
Guarantees: **[Invariants](development/invariants.md)**.
