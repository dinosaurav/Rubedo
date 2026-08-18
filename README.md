# Rubedo

**A Python library for batch pipelines.** You write steps as ordinary functions. Rubedo stores every result and only recomputes what changed — so fixing the last step doesn't re-pay a thousand LLM calls, scrapes, or APIs.

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status: pre-1.0](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#project-status)

> **At a glance.** Local-first library, not an orchestrator: DAG pipelines over keyed collections (files, CSV rows, URLs) with content-addressed row-level caching, an append-only run ledger, and surgical invalidation. Think dbt-style state for Python tasks, built for non-idempotent steps. Every output lives at `hash(step, code_version, input_hash, pipeline)` — pipeline name always last, so identically named steps in two pipelines never share a cache entry or a liveness row. State lives in `.rubedo/` (SQLite control plane + Arrow IPC lane store + object store); optional S3-compatible store and Postgres ledger. Pre-1.0, MIT licensed.

## Why

If you've processed a thousand rows through an LLM and then needed to change the prompt, you know the failure modes:

- **Re-running re-pays.** Without durable per-item state, every code tweak or crash means re-running every API call before it. Rubedo keeps the rows that still hold and only recomputes the ones that don't.
- **A pickle file cannot see your pipeline.** `functools.cache` and ad-hoc caches go stale silently. They can't tell you *why* something recomputed, and they can't invalidate downstream when an input changes.
- **Orchestrators are a different tool.** Airflow, Prefect, and Dagster schedule and monitor services. Rubedo is dbt-style incrementality inside a local Python script — row by row, only what changed. You import it; you don't operate it.
- **Make/Snakemake track files.** Rubedo tracks *content*, at row granularity, with a queryable history of every run.

A library, not a platform: no daemon, no registry, no magic module. The engine never imports your code — you import the engine. State lives in a `.rubedo/` directory (SQLite control plane + Arrow IPC lane store + content-addressed object store), created on first run and gitignored automatically — and each of those planes can be pointed at a shared Postgres database or an S3-compatible bucket when one machine stops being enough (see [sharing state](#local-by-default-shared-when-you-need-it)).

> **Note:** `.rubedo/` resolves **relative to the current working directory** — pipelines, the CLI, and the server must all run from the same directory (typically your project root) to see the same state. Running from somewhere else silently creates a fresh, empty store there. To run from anywhere, pin the location with the `RUBEDO_HOME` (or `RUBEDO_DB_PATH`) environment variable.

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only FastAPI backend for the web dashboard; the `s3` extra (`pip install "rubedo[s3]"`) adds the S3-compatible cloud store backend. To hack on Rubedo itself (or run the bundled examples), clone the repo and `uv sync`.

## Quickstart

No API key. A folder of files in, a line count out — boring on purpose, so you can see reuse without paying for it. Pipelines are plain Python objects; define them wherever your code lives:

```python
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step(force=True)   # rescan the folder every run
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):  # argument name = parent step
    return {"line_count": len(scan["text"].splitlines())}

print(p.describe())           # the graph, before ever running (also: format="mermaid", format="ascii")
print(p.plan())                # dry-run: what would p.run() do, and why
summary = p.run()              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")
```

**What this is doing**

1. **`scan`** lists a folder and yields one item per file. `force=True` means it re-reads the folder every run, so new and edited files show up.
2. **`count_lines`** runs once per file. The argument name `scan` is the parent — Rubedo builds the graph from the function signature. No YAML, no DAG file.
3. **`plan()`, then `run()`.** `plan()` is a dry-run: what would recompute, and why. `run()` executes.

Nothing else is spelled out that the code already says: `scan` is a parentless generator, so it's an `expand`-shaped source; names default to the function names and `version` to `"0"`. Sources are cached like any step by default, so one that watches external state (a folder, a CSV, a table) must declare that it re-enumerates every run — that's what lets the edit below get noticed.

Run it twice and watch the point of the whole project:

```text
# first run          created=8  reused=0   ← every file is new
# second run         created=0  reused=8   ← nothing changed, nothing recomputed
# edit one file...   created=2  reused=6   ← scan + count for that file; the rest stay
```

`created=2` is two steps for one file, not two files. The other files don't re-run. Each run also snapshots the pipeline's definition (steps, edges, policies) into the ledger, so history and the dashboard can show the graph of anything that has ever run — no imports of user code required.

Prefer steps defined away from the pipeline that uses them? `pipeline(steps=[...])` takes an explicit list of `@step`-decorated functions, and it's one object either way — no separate builder class; the two forms compose freely:

```python
from rubedo import step, pipeline

@step(force=True)
def scan(): ...

@step
def count_lines(scan): ...

p = pipeline(name="count-lines", steps=[scan, count_lines])
```

The copy-paste version with query, edit, version bump, and invalidation is the [tutorial](https://rubedo.run/docs/tutorial/).

## When the last step is an LLM

Same graph, expensive step — one call per row, cached. That's the [landing-page](https://rubedo.run/) walkthrough (`inbox` → `decide`). A CSV is the same idea:

```python
import csv
from rubedo import pipeline

p = pipeline(name="enrich-leads")

@p.step(force=True)   # re-read the CSV every run
def leads():
    with open("data/leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def enrich(leads: dict):  # one call per row; argument name = parent
    return {"email": leads["email"], "summary": call_llm(leads["notes"])}
```

Re-run: already-seen rows skip the LLM; new or edited rows pay.

Each row is a **content-addressed lane** (`row-<hash>`): identical rows collapse to one lane, and an edited row shows up as removed + created. To find a row by a human field (email, id), query the output struct — the lane key is never a human key.

A step consumes up to two things, each with its own slot in the cache key: **data** (always hashed) and **params** (hashed only for steps that declare a `params` parameter — so turning a knob recomputes exactly the steps that read it).

## Sources, shapes, policies

There's no `Source` protocol. A parentless generator is a source (`shape="expand"` inferred). Folder / CSV / SQL recipes live in [How it works](https://rubedo.run/docs/concepts/model/#sources); cloud LIST-only and the rest are in [sources.md](docs/concepts/sources.md).

**Shapes.** Default is `map` (1:1). `aggregate` / `fold` fan in; `expand` fans out; `join` is an N-way equijoin (`join_mode="intersect"` inner, `"union"` symmetric outer). A parentless non-generator is a source-less `@root` lane whose input is its params. Broadcast, traps, and `p.join(...)` / `p.union(...)`: [shapes](docs/concepts/shapes.md). Practical join: [Enrich and join tables](docs/guides/data-enrichment.md).

**Policies** — none of these enter cache identity:

```python
def check_price_positive(val: dict):
    if val["price"] < 0: raise ValueError("Negative price")

@step(retries=3, retry_on=(TimeoutError, ConnectionError), retry_delay=1, retry_backoff=2,
      rate_limit="30/min", stale_after="24h", assertions=[check_price_positive])
def enrich(row: dict): ...
```

Retries, rate limits, assertions, `executor="process"` / a Future-shaped factory pool, `schedule="broad"|"deep"`, and `Filtered`: [Retries, rate limits, assertions](docs/guides/execution-policies.md). `use_cache=False` fuses a cheap helper into its consumers and never materializes it — don't skip anything expensive, flaky, or non-deterministic ([When code changes](docs/concepts/versioning.md)).

## Find a row. Invalidate just that.

Outputs are searchable by their content — a step's output struct fields are the query language's open vocabulary:

```python
from rubedo import Selection, invalidate

invalidate(Selection(index={"company": "acme"}))          # recompute acme's rows next run
Selection.parse("step:extract company:acme live:true")     # query-string form (Python, CLI, and UI)
```

That is **surgical invalidation**: only the matched rows (and, with `downstream=True`, what they contaminated) recompute. Invalidation is a logical tombstone, never a delete. `trace()` is the preview of the blast radius. Full language and the fan-in trap: [Find and invalidate a row](docs/guides/search-and-invalidation.md).

## Code changes and caching

Two independent axes on `@step`:

- **`version`** is the semantic identity — bump it for deliberate behavior changes (also the escape hatch for edits the engine can't see, like helpers your step calls).
- **`code`** decides what a *source edit* means. `code="auto"` folds the function's source hash into the cache identity. `code="warn"` (the default) never recomputes on edits, but warns loudly whenever it reuses an output whose code has since changed.

`stale_after="24h"` is a wall-clock TTL, independent of both. Details: [When code changes](docs/concepts/versioning.md).

## Inspecting runs

`p.plan()` is a read-only dry-run: it tells you what `p.run()` would do to every lane and why (reuse, execute, blocked, filtered, stale, code-drift) without writing anything. A `force=True` source always plans as `execute`; everything downstream shows `pending` until it actually runs — that's why the tutorial's first-run plan looks coarse.

Everything a run wrote is queryable through **`Home`**:

```python
from rubedo import Home

home = Home.default()
home.current()                              # the latest full run's cells
home.select("step:enrich company:acme")     # same query language as the CLI and UI
home.runs(pipeline="triage", limit=10)      # run history, newest first
```

The **web dashboard** is a read-only browser over the same ledger (`rubedo serve` → http://127.0.0.1:8000). Treat it as a local tool, not something to expose publicly.

Partial runs, sampling, run-to-run diff, `trace()`, and `rubedo du`: [Inspect a run](docs/guides/inspecting-runs.md) and [Trial a change](docs/guides/trials.md).

```bash
rubedo ls                          # recent runs
rubedo show <run_id> --failed      # what broke, per lane
rubedo invalidate "step:enrich company:acme" --reason "bad prompt"
rubedo serve                       # dashboard
```

## Retention and garbage collection

The store keeps every generation forever by default. `pipeline(..., retention=5)` keeps only the last 5 runs' outputs; `rubedo gc` (dry-run by default) reconciles against a byte budget. Retention deletes **bytes, never facts**. Details: [Inspect a run](docs/guides/inspecting-runs.md#keep-the-store-small); full model: [notes/retention.md](notes/retention.md).

## Local by default, shared when you need it

All state hangs off a `Home` — ledger (SQLite by default), content-addressed object store, Arrow lane tables. To share beyond one machine:

```python
home = Home(".rubedo", store_url="s3://my-bucket/rubedo")   # or RUBEDO_STORE_URL
p = pipeline(name="scrape", home=home)
```

A second home against the same bucket and ledger reuses the first's outputs. Multi-machine ledgers move to Postgres via `db_url=`. Caveats: `rubedo gc --delete` currently refuses cloud stores (dry-run works), and the cloud planes are the newest part of the engine. Setup: [Share the cache](docs/guides/cloud-storage.md).

## Examples

Every example in [`examples/`](examples/) is a self-contained folder that talks to **real** services (Hacker News, GitHub, Open-Meteo, Project Gutenberg, an LLM via OpenRouter) using only the standard library:

```bash
uv run python examples/count_lines/count_lines.py    # run it twice — watch everything reuse
```

See the [examples README](examples/README.md) for the full table of what each one demonstrates.

## Design

The control plane is an **append-only SQL ledger** (SQLite by default, Postgres for shared deployments — immutability enforced at the ORM layer), while outputs land in **append-only Arrow IPC files**. Committed outputs are immutable, every liveness transition is recorded in the `input_hash_usages` table, and workers can die at any point without corrupting committed state. Planning is read-only and value-free; execution is DB-free; all writes go through one commit path. [notes/invariants.md](notes/invariants.md) is the canonical vocabulary; [notes/producer-model.md](notes/producer-model.md) covers sources, `expand`, and `join`.

## Performance

The data plane is columnar: each step's outputs live in a per-step, append-only **Arrow IPC** file, and the reuse checks that dominate plan time are vectorized Arrow scans rather than per-row SQLite queries. On top of that store:

- **Reuse lookups are O(matches), not O(history).** Each loaded table carries an in-memory `address → row` index; warm lookups **1.6×** faster and sparse lookups **2.8×** faster in the micro benchmarks.
- **Liveness is one SQLite query per run.** The set of fulfilled addresses loads once at run start — a `.plan()` over a 5K-lane store went from 0.35s to 0.22s when this landed.
- **Tables stay in memory while they're needed.** A parent step's table is flushed to disk only once no future segment reads it.
- **Data can stay in Arrow end-to-end.** Annotate an aggregate's parent
  `pa.Table` / `pl.DataFrame` to receive fan-in as a table; `as_table=True`
  stores a DataFrame as one cache entry. That's also why any output field
  is searchable and joinable with no index declaration.

[`benchmarks/`](benchmarks/) is the before/after harness. Scenarios report **work counters** alongside timings — see [`benchmarks/README.md`](benchmarks/README.md).

## Project status

Pre-1.0 and moving fast: the API is unstable and there are **no migrations or backwards-compatibility shims** — schema changes mean deleting `.rubedo/` and re-running. The core model (content-addressed lanes, the five shapes, multi-source, the ledger protocol) is designed and built; hardening and polish are ongoing in [notes/TODO.md](notes/TODO.md).

## Contributing

Small fixes and discussion are welcome; larger features should start as an issue before any code — see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the verification checklist, and conventions (the short version: small commits, no compat shims, prefer deleting a concept to adding a knob).

## License

[MIT](LICENSE)
