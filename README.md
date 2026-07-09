# Rubedo

**Content-addressed caching and run history for Python batch pipelines — built for steps you can't afford to re-run.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status: pre-1.0](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#project-status)

Rubedo is a local-first batch engine: you define a DAG of Python steps over a collection of items — files in a folder, rows in a CSV, rows in a SQL table — and Rubedo runs it with **dbt-style state**. Every step output is stored immutably at a deterministic address (`hash(step, code_version, input_hash)`), so re-running a pipeline recomputes only what actually changed. An append-only run ledger records what happened to every item in every run, and lineage edges connect each output to the outputs it was derived from.

It exists for **non-idempotent, expensive steps** — LLM calls, scraping, paid APIs — where "just re-run the script" means paying for everything again and hoping the results come back the same.

## Why

If you've ever processed a thousand rows through an LLM and then needed to fix the last step, you know the failure modes:

- **Re-running re-pays.** Without durable per-item state, every code tweak or crash means re-running every API call before it.
- **`functools.cache` and pickle files don't know your DAG.** Ad-hoc caches can't tell you *why* something recomputed, can't invalidate downstream when an input changes, and silently go stale when the code does.
- **Orchestrators are the wrong tool.** Airflow/Prefect/Dagster schedule and monitor services; they don't give you row-level, content-addressed incrementality inside a local script. dbt does — but only for SQL.
- **Make/Snakemake track files.** Rubedo tracks *content*, at row granularity, with a queryable history of every run.

Rubedo is a library, not a platform: no daemon, no registry, no magic module. The engine never imports your code — you import the engine. State lives in a `.rubedo/` directory (SQLite ledger + content-addressed object store), created on first run and gitignored automatically.

> **Note:** `.rubedo/` resolves **relative to the current working directory** — pipelines, the CLI, and the server must all run from the same directory (typically your project root) to see the same state. Running from somewhere else silently creates a fresh, empty store there. To run from anywhere, pin the location with the `RUBEDO_HOME` (or `RUBEDO_DB_PATH`) environment variable.

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only FastAPI backend for the web dashboard. To hack on Rubedo itself (or run the bundled examples), clone the repo and `uv sync`.

## Quickstart

Pipelines are plain Python objects — define them wherever your code lives:

```python
from rubedo import ProcessResult, step, pipeline, run, plan, describe

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

p = pipeline(id="count-lines", name="Count Lines", folder="input",
             steps=[read_lines, count_lines])

print(describe(p))            # the DAG, before ever running (also: format="mermaid")
print(plan(p))                # dry-run: what would run() do to my data, and why
summary = run(p)              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")
```

Run it twice and watch the point of the whole project:

```text
# first run          created=8  reused=0
# second run         created=0  reused=8   ← nothing changed, nothing recomputed
# edit one file...   created=2  reused=6   ← only that file's lanes re-run
```

Each run also snapshots the pipeline's definition (steps, edges, policies) into the ledger, so history and the dashboard can show the DAG of anything that has ever run — no imports of user code required.

Prefer a fluent style? `PipelineBuilder` builds the same object:

```python
from rubedo import PipelineBuilder
p = PipelineBuilder(id="count-lines", name="Count Lines", folder="input")

@p.step(name="read_lines", version="read-v1")
def read_lines(path: str): ...

count_lines = p.build()
```

## Sources

Items come from a `Source` — anything that can enumerate `(coordinate, content_hash)` pairs and load payloads. `folder="..."` is sugar for `FolderSource` (each file is a lane; root steps receive its path). `CsvSource` makes each row a lane and hands root steps the row dict; `TableSource` does the same for SQL rows, with an optional `batch_size` streaming mode:

```python
from rubedo import CsvSource, step, pipeline

@step(name="enrich", version="v1")
def enrich(row: dict):
    return {"email": row["email"], "summary": call_llm(row["notes"])}

leads = pipeline(id="enrich-leads", name="Enrich Leads",
                 source=CsvSource("data/leads.csv"),
                 steps=[enrich])
```

Each row is a **content-addressed lane** (`row-<hash>`): identical rows collapse to one lane, and an edited row shows up as removed + created — so incrementality survives row reordering, deduplication, and appends for free. To find or track a row by a human field (email, id), index it with `@step(index=[...])` and query — the lane key is never a human key.

A step consumes up to two things, each with its own slot in the cache key: **data** (the source payload for root steps, parent outputs for dependent steps — always hashed) and **params** (run-level knobs, validated against the pipeline's `params_model` and hashed only for steps that declare a `params` parameter — so turning a knob recomputes exactly the steps that read it).

## Built for flaky, expensive work

Steps carry their own execution policies:

```python
def check_price_positive(val: dict):
    if val["price"] < 0: raise ValueError("Negative price")

@step(name="enrich", version="1.0.0",
      retries=3, retry_on=(TimeoutError, ConnectionError), retry_delay=1, retry_backoff=2,
      rate_limit="30/min", stale_after="24h", assertions=[check_price_positive])
def enrich(row: dict): ...
```

- **Retries** apply only to exceptions matching `retry_on` (keep it narrow — retrying a deterministic bug on a paid API just multiplies cost). Every attempt lands in the run event log.
- **`rate_limit`** paces the step evenly across all its workers, retries included.
- **`stale_after`** expires outputs: past the TTL the step re-executes — different bytes supersede the old generation (downstream recomputes), identical bytes just refresh the clock.
- **`assertions`** run against the output value before it commits; if any raise, the step fails and bad data never propagates downstream.
- **`executor="process"`** switches a step from the default thread pool to a process pool (`loky` + `cloudpickle`, so closures are fine) for CPU-bound work.

A step can **decline an item** by returning `Filtered(reason=...)`: downstream steps skip it with status `filtered` instead of executing, and the verdict itself is cached like any output — an expensive LLM-based filter runs once per input, not once per run. When the input changes, the decision is made fresh.

`skip_cache=True` marks an inline util — a quick, idempotent helper that keeps other steps readable. It's never materialized or recorded: its identity fuses into its consumers' cache keys, and it executes lazily (memoized per run) only when a consumer actually runs, so fully-cached runs skip it entirely. If a step is expensive, flaky, or non-deterministic, it deserves materialization — don't skip it.

## Shapes

By default a step is `map` — 1:1 per lane. Three more shapes cover fan-in, fan-out, and joins:

- **`reduce`** (N:1) — fan in over all a parent's surviving lanes: `@step(shape="reduce")` receives `{lane: value}` and returns one output. Add `group_key="field"` to fan in *per group* instead — one output per value of an indexed field. By default it drops failed parent lanes and proceeds with what passed (`on_failed="use_passed"`).
- **`expand`** (1:N) — the step `yield`s a payload per item and each becomes its own content-addressed downstream lane (fetch a feed → a lane per article). The whole expansion is cached against its parent, so a scrape runs once and a re-run re-expands nothing; `stale_after` gives periodic re-scrape.
- **`join`** — an N-way equijoin across multiple sources, matched on an indexed field, minting one lane per matched tuple:

```python
@step(name="order", version="1", source="orders", index=["cust"])
def order(row): return {"oid": row["oid"], "cust": row["cust"]}

@step(name="customer", version="1", source="customers", index=["cid"])
def customer(row): return {"cid": row["cid"], "name": row["name"]}

@step(name="enrich", version="1", shape="join",
      depends_on=["order", "customer"],
      join_on={"order": "cust", "customer": "cid"})
def enrich(order, customer):        # one lane per matched pair
    return {"oid": order["oid"], "name": customer["name"]}

p = pipeline(id="enrich", name="Enrich",
             sources={"orders": CsvSource("orders.csv"),
                      "customers": CsvSource("customers.csv")},
             steps=[order, customer, enrich])
```

Multiple sources are declared with `sources={name: Source}` (single `source=`/`folder=` are the one-source sugar), and each root step names its source with `@step(source="name")`. See [`examples/newsroom`](examples/newsroom/) for join → expand → `group_key` working together.

## Search and surgical invalidation

Outputs are **searchable by their content**: `@step(index=["company", "meta.region"])` extracts those value fields into an index at commit time, so you can select by what a step *computed*, regardless of file names or row keys:

```python
from rubedo import Selection, invalidate

invalidate(Selection(index={"company": "acme"}))          # recompute acme's rows next run
Selection.parse("step:extract company:acme live:true")     # query-string form (Python, CLI, and UI)
```

Reserved prefixes (`step:`, `live:`, `version:<2.0`-style ranges, lane-key globs) cover engine facts; any other `field:value` matches an indexed field. A label is just data you chose to index — non-unique, multi-valued, attachable at any step, never part of cache identity. Invalidation is a logical tombstone, never a delete: history stays intact, and the next run recomputes exactly the invalidated lanes plus their downstream.

## Code changes and caching

Two independent axes on `@step`:

- **`version`** is the semantic identity — bump it for deliberate behavior changes (also the escape hatch for edits the engine can't see, like helpers your step calls).
- **`code`** decides what a *source edit* means. `code="auto"` folds the function's source hash into the cache identity, so any edit recomputes without version bookkeeping (right for cheap, deterministic steps). `code="warn"` (the default) never recomputes on edits, but warns loudly — in the run output, the event log, and `plan()` — whenever it reuses an output whose code has since changed, so recomputing an expensive LLM step stays a deliberate choice.

## Inspecting runs

`plan()` is a read-only dry-run: it tells you what `run()` would do to every lane and why (reuse, execute, blocked, filtered, stale, code-drift) without writing anything.

`trace()` follows lineage from any selection — upstream to the source items everything came from (roots show their stored payload), downstream to everything derived from it. "This output looks wrong — what produced it, and what did it contaminate?" is one command:

```python
from rubedo import Selection, trace
print(trace(Selection.parse("company:acme")))    # or: rubedo trace "company:acme"
```

By default only live outputs seed a trace; `include_superseded=True` (CLI `--all`) seeds history too. Traversal always follows the real derivation edges either way — superseded generations are marked, never hidden.

The **CLI** browses and invalidates against the local ledger:

```bash
rubedo ls                          # recent runs
rubedo show <run_id> --failed      # what broke, per lane (--json for scripts)
rubedo invalidate "step:enrich company:acme" --reason "bad prompt"
```

The **web dashboard** is a read-only browser over runs, materializations, lineage, and current outputs, with search to drill into specific values or errors:

```bash
uv run uvicorn rubedo.server:app --reload   # API on :8000
cd web && npm run dev                       # UI on :5173
```

Running, recomputing, and invalidation always happen from library code or the CLI; the UI never mutates state.

## Examples

Every example in [`examples/`](examples/) is a self-contained folder that talks to **real** services (Hacker News, GitHub, Open-Meteo, Project Gutenberg, an LLM via OpenRouter) using only the standard library:

```bash
uv run python examples/count_lines/count_lines.py    # run it twice — watch everything reuse
```

See the [examples README](examples/README.md) for the full table of what each one demonstrates.

## Design

The ledger is **append-only** and enforced at the ORM layer: committed outputs are immutable, every liveness transition is recorded, and workers can die at any point without corrupting committed state. Planning is read-only and value-free; execution is DB-free; all writes go through one commit path. [notes/invariants.md](notes/invariants.md) is the canonical vocabulary and the eight invariants the engine guarantees; [notes/producer-model.md](notes/producer-model.md) covers the design behind sources, `expand`, and `join`.

## Project status

Pre-1.0 and moving fast: the API is unstable and there are **no migrations or backwards-compatibility shims** — schema changes mean deleting `.rubedo/` and re-running. The core model (content-addressed lanes, the four shapes, multi-source, the ledger protocol) is designed and built; hardening and polish are ongoing in [notes/TODO.md](notes/TODO.md).

## Contributing

Small fixes and discussion are welcome; larger features should start as an issue before any code — see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the verification checklist, and conventions (the short version: small commits, no compat shims, prefer deleting a concept to adding a knob).

## License

[MIT](LICENSE)
