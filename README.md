# Batchit

A local-first batch processing engine that runs DAG pipelines over folders of files, with content-addressed caching, durable run history, and surgical invalidation.

Every step output is stored immutably at a deterministic address — `hash(step, code_version, input_hash, config_hash)` — so re-running a pipeline only recomputes what actually changed. A run ledger records what happened to every file in every run (`created`, `reused`, `failed`, `blocked`, `removed`), and lineage edges connect each output to the outputs it was derived from.

## Quickstart

Define a pipeline in `batchbrain_processors.py` at the repo root (override the path with the `BATCHBRAIN_PROCESSORS` env var):

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
from batchbrain.processor_runner import run_processor

summary = run_processor("count-lines")
print(summary.created_count, summary.reused_count)
```

Or through the web UI:

```bash
uv run uvicorn batchbrain.server:app --reload   # API on :8000
cd web && npm run dev                            # UI on :5173
```

State lives in `.batchbrain/` (SQLite database + content-addressed object store), created on first run and gitignored automatically.

## Concepts

See [docs/invariants.md](docs/invariants.md) for the core vocabulary (coordinate, materialization, output address, manifest) and the invariants the engine guarantees — most importantly: a materialization row exists only if its output bytes committed atomically, committed outputs are immutable, and invalidation is a logical tombstone, never a silent delete.

**Caching caveat:** step identity comes from the manual `version` string. If you change a step's code, bump its version or the engine will happily reuse stale outputs.

## Layout

- `batchbrain/` — engine (scanner, hashing, runner), SQLAlchemy models, object store, FastAPI server
- `web/` — React + Vite dashboard (runs, materializations, lineage, selection-based invalidation)
- `examples/` — runnable demo pipelines
- `tests/` — pytest suite (`uv run pytest`)
