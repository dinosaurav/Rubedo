# Batchit

A local-first batch processing engine that runs DAG pipelines over collections of coordinates — files in a folder, rows in a CSV — with content-addressed caching, durable run history, and surgical invalidation.

Every step output is stored immutably at a deterministic address — `hash(step, code_version, input_hash, config_hash)` — so re-running a pipeline only recomputes what actually changed. A run ledger records what happened to every coordinate in every run (`created`, `reused`, `failed`, `blocked`, `removed`), and lineage edges connect each output to the outputs it was derived from.

## Quickstart

Define a pipeline in `batchbrain_pipelines.py` at the repo root (override the path with the `BATCHBRAIN_PIPELINES` env var):

```python
from batchbrain import ProcessResult, step, pipeline

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

pipeline(id="count-lines", name="Count Lines", folder="examples/input",
         steps=[read_lines, count_lines])
```

Run it programmatically:

```python
import batchbrain

summary = batchbrain.run("count-lines", params={"min_lines": 1})
print(summary.created_count, summary.reused_count)
```

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

pipeline(id="enrich-leads", name="Enrich Leads",
         source=CsvSource("data/leads.csv", key="email"),
         steps=[enrich])
```

`key` names the column(s) that identify a row and is deliberately required: it keeps coordinates stable when rows are edited or inserted, so only changed rows recompute. Pass `key=None` to opt into content-addressed coordinates, where an edited row shows up as removed + created instead.

A step consumes up to three things, each with its own slot in the cache key: **data** (the source payload for root steps, parent outputs for dependent steps — always hashed), **params** (run-level knobs, validated against `params_model` and hashed for exactly the steps that declare a `params` parameter), and **static config** (`@step(config=...)`, fixed at registration). Root steps receive the payload positionally; dependent steps receive parent outputs by parameter name, matching `depends_on`.

State lives in `.batchbrain/` (SQLite database + content-addressed object store), created on first run and gitignored automatically.

## Concepts

See [docs/invariants.md](docs/invariants.md) for the core vocabulary (coordinate, materialization, output address, manifest) and the invariants the engine guarantees — most importantly: a materialization row exists only if its output bytes committed atomically, committed outputs are immutable, and invalidation is a logical tombstone, never a silent delete.

**Caching caveat:** step identity comes from the manual `version` string. If you change a step's code, bump its version or the engine will happily reuse stale outputs.

## Layout

- `batchbrain/` — engine (sources, hashing, runner), SQLAlchemy models, object store, FastAPI server
- `web/` — React + Vite dashboard (runs, materializations, lineage, selection-based invalidation)
- `examples/` — runnable demo pipelines
- `tests/` — pytest suite (`uv run pytest`)
