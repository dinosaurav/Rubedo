# Rubedo

**Content-addressed caching and run history for Python batch pipelines — built for steps you can't afford to re-run.**

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Status: pre-1.0](https://img.shields.io/badge/status-pre--1.0-orange.svg)](#project-status)

Rubedo is a local-first batch engine: you define a DAG of Python steps over a collection of items — files in a folder, rows in a CSV, rows in a SQL table — and Rubedo runs it with **dbt-style state**. Every step output is stored immutably at a deterministic address (`hash(step, code_version, input_hash, pipeline)`), so re-running a pipeline recomputes only what actually changed. The pipeline name is folded into the address, so two pipelines with an identically named/versioned step and identical input never share a cache entry or a liveness row. An append-only run ledger records what happened to every item in every run, and lineage edges connect each output to the outputs it was derived from.

It exists for **non-idempotent, expensive steps** — LLM calls, scraping, paid APIs — where "just re-run the script" means paying for everything again and hoping the results come back the same.

## Why

If you've ever processed a thousand rows through an LLM and then needed to fix the last step, you know the failure modes:

- **Re-running re-pays.** Without durable per-item state, every code tweak or crash means re-running every API call before it.
- **`functools.cache` and pickle files don't know your DAG.** Ad-hoc caches can't tell you *why* something recomputed, can't invalidate downstream when an input changes, and silently go stale when the code does.
- **Orchestrators are the wrong tool.** Airflow/Prefect/Dagster schedule and monitor services; they don't give you row-level, content-addressed incrementality inside a local script. dbt does — but only for SQL.
- **Make/Snakemake track files.** Rubedo tracks *content*, at row granularity, with a queryable history of every run.

Rubedo is a library, not a platform: no daemon, no registry, no magic module. The engine never imports your code — you import the engine. State lives in a `.rubedo/` directory (SQLite control plane + Arrow IPC lane store + content-addressed object store), created on first run and gitignored automatically — and each of those planes can be pointed at a shared Postgres database or an S3-compatible bucket when one machine stops being enough (see [sharing state](#local-by-default-shared-when-you-need-it)).

> **Note:** `.rubedo/` resolves **relative to the current working directory** — pipelines, the CLI, and the server must all run from the same directory (typically your project root) to see the same state. Running from somewhere else silently creates a fresh, empty store there. To run from anywhere, pin the location with the `RUBEDO_HOME` (or `RUBEDO_DB_PATH`) environment variable.

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only FastAPI backend for the web dashboard; the `s3` extra (`pip install "rubedo[s3]"`) adds the S3-compatible cloud store backend. To hack on Rubedo itself (or run the bundled examples), clone the repo and `uv sync`.

## Quickstart

Pipelines are plain Python objects — define them wherever your code lives:

```python
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step(check_cache=False)   # a source watching external state: rescan every run
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

print(p.describe())           # the DAG, before ever running (also: format="mermaid", format="ascii")
print(p.plan())                # dry-run: what would p.run() do to my data, and why
summary = p.run()              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")
```

Nothing is spelled out that the code already says: `scan` is a parentless generator, so it's an `expand`-shaped source; `count_lines`'s parameter names the `scan` step, so that's its dependency; names default to the function names and `version` to `"0"`. The one explicit knob is `check_cache=False`: sources are cached like any step by default, so one that watches external state (a folder, a CSV, a table) must declare that it re-enumerates every run — that's what lets the edit below get noticed.

Run it twice and watch the point of the whole project:

```text
# first run          created=8  reused=0
# second run         created=0  reused=8   ← nothing changed, nothing recomputed
# edit one file...   created=2  reused=6   ← only that file's lanes re-run
```

Each run also snapshots the pipeline's definition (steps, edges, policies) into the ledger, so history and the dashboard can show the DAG of anything that has ever run — no imports of user code required.

Prefer steps defined away from the pipeline that uses them? `pipeline(steps=[...])` takes an explicit list of `@step`-decorated functions, and it's one object either way — no separate builder class; the two forms compose freely:

```python
from rubedo import step, pipeline

@step(check_cache=False)
def scan(): ...

@step
def count_lines(scan): ...

p = pipeline(name="count-lines", steps=[scan, count_lines])
```

## Ingestion is a step

There's no `Source` protocol, no source classes — ingestion is just a parentless generator step (its `out_shape="many"` inferred — the `shape="expand"` alias) that `yield`s a payload per item. Each payload mints its own content-addressed lane. A folder scan is a three-line generator (above); a CSV is a `csv.DictReader` loop; a SQL table is a plain `SELECT` loop — see [docs/concepts/sources.md](docs/concepts/sources.md) for all the recipes, including cloud object storage:

```python
import csv
from rubedo import pipeline

p = pipeline(name="enrich-leads")

@p.step(check_cache=False)   # re-read the CSV every run
def leads():
    with open("data/leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def enrich(leads: dict):
    return {"email": leads["email"], "summary": call_llm(leads["notes"])}
```

Each row is a **content-addressed lane** (`row-<hash>`): identical rows collapse to one lane, and an edited row shows up as removed + created — so incrementality survives row reordering, deduplication, and appends for free. To find or track a row by a human field (email, id), query it — the lane key is never a human key.

A step consumes up to two things, each with its own slot in the cache key: **data** (the source payload for root steps, parent outputs for dependent steps — always hashed) and **params** (run-level knobs, validated against the pipeline's `params_model` and hashed only for steps that declare a `params` parameter — so turning a knob recomputes exactly the steps that read it).

## Built for flaky, expensive work

Steps carry their own execution policies:

```python
def check_price_positive(val: dict):
    if val["price"] < 0: raise ValueError("Negative price")

@step(retries=3, retry_on=(TimeoutError, ConnectionError), retry_delay=1, retry_backoff=2,
      rate_limit="30/min", stale_after="24h", assertions=[check_price_positive])
def enrich(row: dict): ...
```

- **Retries** apply only to exceptions matching `retry_on` (keep it narrow — retrying a deterministic bug on a paid API just multiplies cost). Every attempt lands in the run event log.
- **`rate_limit`** paces the step evenly across all its workers, retries included.
- **`stale_after`** expires outputs: past the TTL the step re-executes — different bytes supersede the old generation (downstream recomputes), identical bytes just refresh the clock.
- **`assertions`** run against the output value before it commits; if any raise, the step fails and bad data never propagates downstream.
- **`executor="process"` or `executor=<factory>`** switches a step from the
  default thread path to a `loky` process pool or any Future-shaped external
  pool returned by a zero-argument factory — [`examples/dask_executor`](examples/dask_executor/)
  and [`examples/ray_executor`](examples/ray_executor/) run real step bodies on
  Dask and Ray with full second-run reuse. Executor choice never changes
  cache identity.
- **`pipeline(..., schedule="broad"|"deep")`** picks the execution order — never the results (cache identity is order-independent, and either mode fully reuses the other's outputs). `"broad"` (default) completes each step across all lanes before the next one starts — natural inspection checkpoints, so you see all of a paid step's output before the next stage spends anything. `"deep"` lets each item race ahead through consecutive 1:1 steps as soon as its own inputs land — first results as early as possible, no stalling at stage boundaries while a slow sibling scrapes. `aggregate`/`join` always synchronize on all lanes either way.

A step can **decline an item** by returning `Filtered(reason=...)`: downstream steps skip it with status `filtered` instead of executing, and the verdict itself is cached like any output — an expensive LLM-based filter runs once per input, not once per run. When the input changes, the decision is made fresh.

`skip_cache=True` marks an inline util — a quick, idempotent helper that keeps other steps readable. It's never materialized or recorded: its identity fuses into its consumers' cache keys, and it executes lazily (memoized per run) only when a consumer actually runs, so fully-cached runs skip it entirely. If a step is expensive, flaky, or non-deterministic, it deserves materialization — don't skip it.

## Shapes

By default a step is `map` — 1:1 per lane. Four more shapes cover fan-in, fan-out, and joins:

- **`aggregate`** (N:1) — fan in over all a parent's surviving lanes: `@step(in_shape="aggregate")` receives `{lane: value}` and returns one output. Add `group_key="field"` to fan in *per group* instead — one output per value of a parent output field. By default it drops failed parent lanes and proceeds with what passed (`on_failed="use_passed"`).
- **`fold`** (N:1) — like `aggregate`, but receives an accumulator (initialized to `fold_init`) and one parent value at a time for incremental processing. Supports `group_key`.
- **`expand`** (1:N) — the step `yield`s a payload per item and each becomes its own content-addressed downstream lane (fetch a feed → a lane per article). The whole expansion is cached against its parent, so a scrape runs once and a re-run re-expands nothing; `stale_after` gives periodic re-scrape.
- **`join`** — an N-way equijoin across multiple `expand` roots, matched on a field of their parent outputs, minting one lane per matched tuple:

```python
p = pipeline(name="enrich")

@p.step
def orders_src():
    with open("orders.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def customers_src():
    with open("customers.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def order(orders_src): return {"oid": orders_src["oid"], "cust": orders_src["cust"]}

@p.step
def customer(customers_src): return {"cid": customers_src["cid"], "name": customers_src["name"]}

@p.step(
        join_on={"order": "cust", "customer": "cid"})
def enrich(order, customer):        # one lane per matched pair
    return {"oid": order["oid"], "name": customer["name"]}
```

Multiple sources are just multiple `expand`-shaped roots in the same pipeline — nothing extra to declare; `join` doesn't care that its parents are expand roots. See [`examples/newsroom`](examples/newsroom/) for join → expand → `group_key` working together.

When a join or union has no interesting body, declare it without one: `p.join(name="pair", join_on={...})` assembles a nested struct from the matched parents, and `p.union(name="all", depends_on=[...])` merges lane sets deduped by content hash — zero per-lane Python calls, cached like any step.

A pipeline doesn't need a source-shaped root at all. A `map` step with no `depends_on` is a **source-less root**: it mints a single lane whose input is its params (or a constant when it takes none), so you can feed a value *into* the head instead of scanning for one — `p.run(params={"pdf": "…"})`. Same params reuse the cached output; a changed param recomputes. It's the everyday counterpart to an `expand` root (which mints N): a `map` root mints one.

```python
@p.step                                    # no parents, not a generator
def load_pdf(params): return split(params["pdf"])   # mints the single '@root' lane
```

See [`examples/pdf_digest`](examples/pdf_digest/) for a source-less head feeding expand → vision-LLM → aggregate → two summaries.

## Search and surgical invalidation

Outputs are **searchable by their content**: a step's output struct fields are the query language's open vocabulary, so you can select by what a step *computed*, regardless of file names or row keys:

```python
from rubedo import Selection, invalidate

invalidate(Selection(index={"company": "acme"}))          # recompute acme's rows next run
Selection.parse("step:extract company:acme live:true")     # query-string form (Python, CLI, and UI)
```

Reserved prefixes (`step:`, `live:`, `version:<2.0`-style ranges, lane-key globs) cover engine facts; any other `field:value` matches a field of the step's output struct. A label is just a field a step returns — non-unique, multi-valued, attachable at any step, never part of cache identity. Invalidation is a logical tombstone, never a delete: history stays intact, and the next run recomputes exactly the invalidated lanes plus their downstream.

`downstream=True` (CLI `--downstream`) widens the tombstone to everything *derived* from the matches — the full downstream closure over the recorded lineage edges, exactly the set `rubedo trace "<same query>"` shows as live seed + downstream, so **trace is the preview of the blast radius**: run it first and read the counts. Be aware that an aggregate or join inside the closure honestly carries everything after it (one bad lane contaminated the fan-in, so the fan-in and its descendants flip too); recovery is never more than re-running the pipeline, which recomputes exactly the invalidated set.

## Code changes and caching

Two independent axes on `@step`:

- **`version`** is the semantic identity — bump it for deliberate behavior changes (also the escape hatch for edits the engine can't see, like helpers your step calls).
- **`code`** decides what a *source edit* means. `code="auto"` folds the function's source hash into the cache identity, so any edit recomputes without version bookkeeping (right for cheap, deterministic steps). `code="warn"` (the default) never recomputes on edits, but warns loudly — in the run output, the event log, and `p.plan()` — whenever it reuses an output whose code has since changed, so recomputing an expensive LLM step stays a deliberate choice.

## Inspecting runs

`p.plan()` is a read-only dry-run: it tells you what `p.run()` would do to every lane and why (reuse, execute, blocked, filtered, stale, code-drift) without writing anything.

Everything a run wrote is queryable through **`Home`** — the handle to one storage root (the `.rubedo/` directory, or wherever `RUBEDO_HOME` points). A `Cell` is one (run, step, lane) outcome with its status and resolved output value:

```python
from rubedo import Home

home = Home.default()
home.current()                              # the latest full run's cells
home.select("step:enrich company:acme")     # same query language as the CLI and UI
home.runs(pipeline="triage", limit=10)      # run history, newest first
```

`RunSummary.cells` gives the same view for the run you just executed, so tests and scripts never hand-roll coordinate lookups.

### Partial runs and sampling

To trial an expensive step on a frozen cohort without paying for the whole batch (and without changing cache identity):

```python
from rubedo import RunScope

scope = RunScope.sample_n(anchor="classify", cells=candidates, n=100, seed="v2")
trial = p.run(scope=scope, targets=["classify"])  # kind='partial'
p.run()  # full run reuses those classify addresses
```

Scope and targets never enter output addresses. Partial runs do not displace `home.current()` (latest full `process` run) or steal retention protection from it. See [trials: sample, diff, roll out](docs/guides/trials.md).

### Run history and run-to-run diff

After a baseline full run and a version-bumped `RunScope` trial, compare
at the anchor (cohort-aware by default) then roll out:

```python
baseline = p.run()
# …bump step version, sample a cohort…
trial = p.run(scope=scope, targets=["classify"])
diff = home.diff(step="classify", before=baseline, after=trial)
print(diff)  # unchanged / changed / added / removed / failed
p.run()      # full rollout reuses the trial's addresses
```

`home.runs(pipeline=..., kind=..., status=..., limit=...)` lists history
(newest first; effective status; includes partials). See
[trials: sample, diff, roll out](docs/guides/trials.md).

`trace()` follows lineage from any selection — upstream to the source items everything came from (roots show their stored payload), downstream to everything derived from it. "This output looks wrong — what produced it, and what did it contaminate?" is one command:

```python
from rubedo import Selection, trace
print(trace(Selection.parse("company:acme")))    # or: rubedo trace "company:acme"
```

By default only live outputs seed a trace; `include_superseded=True` (CLI `--all`) seeds history too. Traversal always follows the real derivation edges either way — superseded generations are marked, never hidden.

`rubedo du` (or `storage_report()` from `rubedo.du`) answers "why is `.rubedo` this big?": total object-store size, a per-pipeline/per-step breakdown, and a reclaimable estimate — a dry-run ref-count audit computed from the ledger. Objects are content-addressed and shared, so an object counts as reclaimable only when *no* live output references it. Purely a report: nothing is ever deleted. `--json` for scripts.

The **CLI** browses and invalidates against the local ledger:

```bash
rubedo ls                          # recent runs
rubedo show <run_id> --failed      # what broke, per lane (--json for scripts)
rubedo invalidate "step:enrich company:acme" --reason "bad prompt"
rubedo check                       # lint declared pipeline(secrets=/env=) against the environment
```

## Retention and garbage collection

The store keeps every generation forever by default — recompute-avoidance is the whole point, and old outputs are cheap insurance. When they stop being worth their bytes, retention prunes by **run recency**: it never touches what recent runs used, so the safety of caching is preserved.

Set a keep-window per pipeline:

```python
pipeline(name="scrape", ..., retention=5)   # keep only the last 5 runs' outputs
```

At the end of each successful run, generations that only older runs referenced are demoted and — once no live output anywhere references the bytes — the object is deleted. It's set-and-forget; a run skips its own prune (never errors) if another run is in flight.

Or reconcile on demand across all pipelines with a global byte budget:

```bash
rubedo gc                       # dry-run: exactly what --delete would prune, deletes nothing
rubedo gc --max-bytes 2GiB      # dry-run against a budget (oldest runs first)
rubedo gc --max-bytes 2GiB --delete   # apply it
```

Retention deletes **bytes, never facts**: a demoted generation keeps its ledger row and lineage, every deletion is logged in an append-only table, and recovery is lazy — if a pruned lane's input reappears, the next run rewrites the bytes and restores the row. `rubedo du` reports GC-reclaimed objects separately from genuinely missing ones. GC refuses to delete while any run is live (a concurrent run could be committing an output that points at bytes GC is about to remove). [notes/retention.md](notes/retention.md) is the full model — policies, the demote/sweep phases, guarantees, and the recompute trade-off.

The **web dashboard** is a read-only browser over runs, materializations, lineage, and current outputs, with search to drill into specific values or errors. (The UI never writes; the API beneath it is read-only except for one endpoint, `POST /api/selection/invalidate`, which is unauthenticated and meant for local use — treat `rubedo serve` as a local tool, not something to expose publicly.)

```bash
rubedo serve                    # API + UI on http://127.0.0.1:8000
```

The built UI is served from the package — no separate dev server needed. To hack on the web UI itself, use `cd web && npm run dev` (Vite proxies `/api` to `:8000`).

Running, recomputing, and invalidation always happen from library code or the CLI; the UI never mutates state.

## Local by default, shared when you need it

All state hangs off a `Home` — one storage root owning three planes: the **ledger** (SQLite by default), the content-addressed **object store**, and the Arrow **lane tables**. The default home is the local `.rubedo/` directory and nothing else exists until you point at it; every knob below is optional.

To share the cache beyond one machine, move the data planes into an S3-compatible bucket — AWS S3, Cloudflare R2, Backblaze B2, and MinIO all use the same backend; the provider is configuration, not an engine concept:

```python
home = Home(".rubedo", store_url="s3://my-bucket/rubedo")   # or RUBEDO_STORE_URL
p = pipeline(name="scrape", home=home)
```

Spilled outputs land under `objects/…`, and lane history is written as immutable Arrow segments under `tables/…`, guarded by a renewable single-writer lease per pipeline (read-only `.plan()` never takes the lease) with threshold compaction keeping segment chains short. A second home against the same bucket and ledger reuses the first's outputs — the run-it-twice payoff, across machines. For truly multi-machine setups the ledger itself moves to a shared database via `db_url=` (any SQLAlchemy URL; Postgres is what the test suite covers).

When steps run in a process pool or an external pool against a cloud store, spilled payloads travel **by reference**: workers receive `objects:<hash>` refs, fetch their inputs from the bucket directly, and put results straight back — the coordinator never relays the bytes (`p.run(payload_refs=False)` forces hub routing if you need it).

Two honest caveats: destructive `rubedo gc --delete` currently refuses cloud stores (dry-run reporting works fine), and the cloud planes are the newest part of the engine. [docs/guides/cloud-storage.md](docs/guides/cloud-storage.md) has the full setup, including R2 endpoint configuration.

## Examples

Every example in [`examples/`](examples/) is a self-contained folder that talks to **real** services (Hacker News, GitHub, Open-Meteo, Project Gutenberg, an LLM via OpenRouter) using only the standard library:

```bash
uv run python examples/count_lines/count_lines.py    # run it twice — watch everything reuse
```

See the [examples README](examples/README.md) for the full table of what each one demonstrates.

## Design

The control plane is an **append-only SQL ledger** (SQLite by default, Postgres for shared deployments — immutability enforced at the ORM layer), while outputs land in **append-only Arrow IPC files**. Committed outputs are immutable, every liveness transition is recorded in the `input_hash_usages` table, and workers can die at any point without corrupting committed state. Planning is read-only and value-free; execution is DB-free; all writes go through one commit path. [notes/invariants.md](notes/invariants.md) is the canonical vocabulary and the promises the engine guarantees; [notes/producer-model.md](notes/producer-model.md) covers the design behind sources, `expand`, and `join`.

## Performance

The data plane is columnar: each step's outputs live in a per-step, append-only **Arrow IPC** file, and the reuse checks that dominate plan time are vectorized Arrow scans rather than per-row SQLite queries. On top of that store:

- **Reuse lookups are O(matches), not O(history).** Each loaded table carries an in-memory `address → row` index; planning probes it and `table.take`s only the matching rows through Arrow's C++ kernels instead of deserializing a step's whole history. In the micro benchmarks this made warm lookups **1.6×** faster and sparse lookups (few matches in a deep table) **2.8×** faster.
- **Liveness is one SQLite query per run.** The set of fulfilled addresses loads once at run start and is consulted as a Python set intersection, replacing a per-step `IN (...)` query — a `.plan()` over a 5K-lane store went from 0.35s to 0.22s when this landed (~31% at 20K lanes).
- **Tables stay in memory while they're needed.** A parent step's table is flushed to disk only once no future segment reads it, and flushing writes *through* the cache — durability never costs a re-read.
- **Data can stay in Arrow end-to-end.** An aggregate step can request its fan-in as a `pa.Table` (`arrow_aggregate=True`) and an expand step can return one, skipping the Python-dict round trip entirely; that's also why any output field is searchable and joinable with no index declaration.

[`benchmarks/`](benchmarks/) is the before/after harness behind these numbers. Scenarios report **work counters** (Arrow rows written, reuse lookups, SQLite statements) alongside timings, so a change can demonstrate it does no extra work — see [`benchmarks/README.md`](benchmarks/README.md).

## Project status

Pre-1.0 and moving fast: the API is unstable and there are **no migrations or backwards-compatibility shims** — schema changes mean deleting `.rubedo/` and re-running. The core model (content-addressed lanes, the five shapes, multi-source, the ledger protocol) is designed and built; hardening and polish are ongoing in [notes/TODO.md](notes/TODO.md).

## Contributing

Small fixes and discussion are welcome; larger features should start as an issue before any code — see [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the verification checklist, and conventions (the short version: small commits, no compat shims, prefer deleting a concept to adding a knob).

## License

[MIT](LICENSE)
