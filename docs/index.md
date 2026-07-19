# Rubedo

**Content-addressed caching and run history for Python batch pipelines — built for steps you can't afford to re-run.**

Rubedo is a local-first batch engine: you define a DAG of Python steps over a collection of items — files in a folder, rows in a CSV, rows in a SQL table — and Rubedo runs it with **dbt-style state**. Every step output is stored immutably at a deterministic address, so re-running a pipeline recomputes only what actually changed. An append-only run ledger records what happened to every item in every run, and lineage edges connect each output to the outputs it was derived from.

It exists for **non-idempotent, expensive steps** — LLM calls, scraping, paid APIs — where "just re-run the script" means paying for everything again and hoping the results come back the same.

```python
import csv
from rubedo import pipeline

p = pipeline(name="summarize")

@p.step
def leads():
    with open("leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def summarize(leads: dict):
    return {"summary": call_llm(leads["notes"])}   # runs once per distinct row — ever

p.run()   # second run: created=0, reused=everything
```

Rubedo is a library, not a platform: no daemon, no registry, no magic module. The engine never imports your code — you import the engine. State lives in a `.rubedo/` directory (SQLite ledger + content-addressed object store).

## Where to start

- **[Getting started](getting-started.md)** — install, the quickstart, and the run-it-twice payoff.
- **[Tutorial](tutorial.md)** — build a pipeline step by step: incrementality, versioning, indexing, invalidation.
- **[Examples](examples.md)** — self-contained example pipelines against real services (Hacker News, GitHub, LLMs via OpenRouter).

## Understand the model

- **[The model](concepts/model.md)** — lanes, addresses, the ledger, and the promises it keeps.
- **[Shapes](concepts/shapes.md)** — `map`, `aggregate`, `expand`, `join`.
- **[Sources](concepts/sources.md)** — folders, CSV rows, SQL tables, multi-source pipelines.
- **[Code changes & versioning](concepts/versioning.md)** — what an edit means to the cache.

## Day-to-day guides

- **[Execution policies](guides/execution-policies.md)** — retries, rate limits, assertions, process pools, scheduling.
- **[Search & invalidation](guides/search-and-invalidation.md)** — index outputs by content, invalidate surgically.
- **[Inspecting runs](guides/inspecting-runs.md)** — `p.plan()`, `trace()`, `rubedo du`, the dashboard.
- **[Retention & GC](guides/retention.md)** — keep-windows, `rubedo gc`, bytes-never-facts.

## Reference

- **[API Reference](reference/api/index.md)** — every public function and class.
- **[CLI](reference/cli.md)** — every subcommand and flag.
- **[Changelog](changelog.md)** — every released version, kept in [Keep a Changelog](https://keepachangelog.com/) form.
- **Development** — [contributing](development/contributing.md), plus the canonical design notes published verbatim from the repo: the [invariants](development/invariants.md), the [producer model](development/producer-model.md), and the [retention model](development/retention.md).

## Project status

Pre-1.0 and moving fast: the API is unstable and there are no migrations or backwards-compatibility shims. The [README](https://github.com/dinosaurav/Rubedo#readme) carries the canonical overview; [CONTRIBUTING](https://github.com/dinosaurav/Rubedo/blob/main/CONTRIBUTING.md) covers setup and conventions. MIT licensed.
