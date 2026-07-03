# Batchit

A local-first batch processing engine that runs DAG pipelines over collections of coordinates — files in a folder, rows in a CSV — with content-addressed caching, durable run history, and surgical invalidation.

Every step output is stored immutably at a deterministic address — `hash(step, code_version, input_hash, config_hash)` — so re-running a pipeline only recomputes what actually changed. A run ledger records what happened to every coordinate in every run (`created`, `reused`, `failed`, `blocked`, `removed`), and lineage edges connect each output to the outputs it was derived from.

## Quickstart

Pipelines are plain Python objects — define them wherever your code lives:

```python
from batchbrain import ProcessResult, step, pipeline, run, plan, describe

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

p = pipeline(id="count-lines", name="Count Lines", folder="examples/input",
             steps=[read_lines, count_lines])

print(describe(p))            # the DAG, before ever running (also: format="mermaid")
print(plan(p))                # dry-run: what would run() do to my data, and why
summary = run(p)              # execute
print(summary.created_count, summary.reused_count)
```

There is no registry and no magic module: the engine never imports your code — you import the engine. Each run snapshots the pipeline's definition (steps, edges, policies) into the ledger, so the UI can show the DAG of anything that has run.

Then inspect it in the web UI — a read-only browser over runs, materializations, lineage, and current outputs, plus surgical invalidation ("this output is bad, redo it"):

```bash
uv run uvicorn batchbrain.server:app --reload   # API on :8000
cd web && npm run dev                            # UI on :5173
```

Running and recomputing always happen from library code; the UI's only write action is invalidation.

## Sources

Coordinates come from a `Source` — anything that can enumerate `(coordinate, content_hash)` pairs and load payloads. `folder="..."` above is sugar for `FolderSource`, where each file is a coordinate and root steps receive its path. `CsvSource` makes each row a coordinate and hands root steps the row dict:

```python
from batchbrain import CsvSource, ProcessResult, step, pipeline

@step(name="enrich", version="v1")
def enrich(row: dict):
    return {"email": row["email"], "summary": call_llm(row["notes"])}

leads = pipeline(id="enrich-leads", name="Enrich Leads",
                 source=CsvSource("data/leads.csv", key="email"),
                 steps=[enrich])
```

`key` names the column(s) that identify a row and is deliberately required: it keeps coordinates stable when rows are edited or inserted, so only changed rows recompute. Pass `key=None` to opt into content-addressed coordinates, where an edited row shows up as removed + created instead.

Steps carry their own execution policies — built for flaky work like LLM calls and scraping:

```python
@step(name="enrich", version="1.0.0",
      retries=3, retry_on=(TimeoutError, ConnectionError), retry_delay=1, retry_backoff=2,
      rate_limit="30/min", stale_after="24h")
def enrich(row: dict): ...
```

Retries apply only to exceptions matching `retry_on` (keep it narrow — retrying a deterministic bug on a paid API just multiplies cost); every attempt is recorded in the run event log, and the final status notes the attempt count. `rate_limit` paces the step evenly across all its workers, retries included. `stale_after` expires outputs: past the TTL the step re-executes — different bytes supersede the old generation (downstream recomputes), identical bytes just refresh its clock.

A step can **decline a coordinate** by returning `Filtered(reason=...)`: downstream steps skip it with status `filtered` instead of executing, and the verdict itself is cached like any output — an expensive LLM-based filter runs once per input, not once per run. When the input changes, the decision is made fresh.

`skip_cache=True` marks an inline util — a quick, idempotent helper that exists to keep other steps readable. It's never materialized or recorded: its identity fuses into its consumers' cache keys, and it executes lazily (memoized per run) only when a consumer actually runs, so fully-cached runs skip it entirely. Values pass in memory without a serialization round-trip; retries/rate limits don't apply. If a step is expensive, flaky, or non-deterministic, it deserves materialization — don't skip it.

Outputs are **searchable by their content**: `@step(index=["company", "meta.region"])` extracts those value fields into an index at commit time, so you can select by what a step computed — `invalidate(Selection(index={"company": "acme"}))` — regardless of file names or row keys. A label is just data you chose to index: non-unique, multi-valued (list fields index per element), attachable at any step, and never part of cache identity. Lane keys (`coordinate_glob`) cover source-shaped questions; indexed fields cover content-shaped ones; metadata filters cover diagnostics.

A step consumes up to three things, each with its own slot in the cache key: **data** (the source payload for root steps, parent outputs for dependent steps — always hashed), **params** (run-level knobs, validated against `params_model` and hashed for exactly the steps that declare a `params` parameter), and **static config** (`@step(config=...)`, fixed at registration). Root steps receive the payload positionally; dependent steps receive parent outputs by parameter name, matching `depends_on`.

State lives in `.batchbrain/` (SQLite database + content-addressed object store), created on first run and gitignored automatically.

## Concepts

See [docs/invariants.md](docs/invariants.md) for the core vocabulary (coordinate, materialization, output address, manifest) and the invariants the engine guarantees — most importantly: a materialization row exists only if its output bytes committed atomically, committed outputs are immutable, and invalidation is a logical tombstone, never a silent delete.

**Code changes and caching** are two independent axes on `@step`. `version` is the semantic identity — bump it for deliberate behavior changes (also the escape hatch for edits the engine can't see, like helpers your step calls). `code` decides what a *source edit* means: `code="auto"` folds the function's source hash into the cache identity, so any edit recomputes without version bookkeeping (right for cheap, deterministic steps); `code="warn"` (the default) never recomputes on edits, but warns loudly — in the run output, the event log, and `plan()` — whenever it reuses an output whose code has since changed, so recomputing an expensive LLM step stays a deliberate choice. Only the step function's own source is hashed; helper edits are what the version bump is for.

## Layout

- `batchbrain/` — engine (sources, hashing, runner), SQLAlchemy models, object store, FastAPI server
- `web/` — React + Vite dashboard (runs, materializations, lineage, selection-based invalidation)
- `examples/` — runnable demo pipelines
- `tests/` — pytest suite (`uv run pytest`)
