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

```python
import csv
from rubedo import pipeline

p = pipeline(name="summarize")

@p.step(check_cache=False)   # re-read the CSV every run
def leads():
    with open("leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def summarize(leads: dict):  # argument name = parent step
    return {"summary": call_llm(leads["notes"])}

p.run()   # second run: already-seen rows skip the LLM
```

**What this is doing**

1. **`leads`** yields one item per CSV row. `check_cache=False` re-reads
   the file every run, so new and edited rows show up.
2. **`summarize`** calls the LLM once per row. The argument name `leads`
   is the dependency — no YAML, no DAG file.
3. **Run it again.** Already-seen rows skip the LLM. Only new or edited
   rows pay.

## Read in this order

1. **[Tutorial](tutorial.md)** — install, build a classifier, query by
   content, edit an input, bump a version, invalidate a selection.
2. **[How it works](concepts/model.md)** — lanes, addresses, shapes,
   sources, the ledger, the four promises.
3. **[Examples](examples.md)** — the same ideas against real services.

Signatures: **[API](reference/api/index.md)** and **[CLI](reference/cli.md)**.
Guarantees: **[Invariants](development/invariants.md)**.
