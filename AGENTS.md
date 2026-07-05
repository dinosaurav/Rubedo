# Working on Rubedo (agent instructions — canonical)

Local-first batch engine: DAG pipelines over keyed collections (files, CSV
rows) with content-addressed caching, an append-only run ledger, and
surgical invalidation. Think "dbt state for Python tasks," built for
non-idempotent steps (LLM calls, scraping). Read `README.md` for the user
view and `notes/invariants.md` for the vocabulary and guarantees — both are
accurate and load-bearing; keep them updated when behavior changes.

## Conventions (owner-established, follow exactly)

- **Commit per unit of work, directly to `main`**, with explanatory bodies.
  Granular commits over big ones. Run verification before committing.
- **Dev stage — no migrations, no backwards compatibility.** On any DB
  schema change (new/removed *column* — new tables are fine, create_all
  handles them): `rm -rf .rubedo/rubedo.sqlite .rubedo/objects
  .rubedo/staging`, then repopulate by running
  `uv run python examples/count_lines/count_lines.py` twice (expect Created: 15 then
  Reused: 15). Say so in the commit message.
- **Verification checklist**: `uv run pytest -q` (all green, no new
  warnings), `uv run ruff check src/rubedo/ tests/ examples/`,
  `(cd web && npx tsc -b)` when web changed, plus a live end-to-end of the
  changed behavior (the examples, or a small inline script; for API changes
  start uvicorn on a spare port and curl it).
- **Design-first**: for anything ambiguous or conceptual, propose to the
  owner before building. Start with `notes/TODO.md` for open work — the
  specs there already contain the settled decisions (do not re-litigate
  them, but do flag genuine contradictions). The producer model —
  content-addressed lanes, `expand`, `group_key`, multi-source, and N-way
  `join` — is designed and built; see `notes/producer-model.md`.
- **Ruthless simplification** is a project value: prefer deleting a concept
  to adding a knob.

## Architecture map

- `src/rubedo/spec.py` — `@step` / `pipeline()` build plain
  `StepSpec`/`PipelineSpec` objects. No registry: the engine never imports
  user code. `shape` ∈ `map` (1:1, default) / `reduce` (N:1 fan-in over a
  parent's surviving lanes; `group_key` partitions into one output per
  indexed-field value, else a single `"@all"`) / `expand` (1:N — the fn
  yields `(subkey, value)`, minting `parent/subkey` lanes) / `join` (N-way
  equijoin on `join_on={parent: indexed_field}`, minting `a|b|…` pair lanes).
  `pipeline(sources={name: Source})` declares multiple roots and a root step
  picks one with `@step(source="name")` (`source=`/`folder=` stay
  single-source). `executor` is `"thread"` (default) or `"process"` (a `loky`
  pool serializing via `cloudpickle`, so closures are fine). `describe()`
  renders DAGs (text/Mermaid); `definition()` is the JSON snapshot each run
  records.
- `src/rubedo/sources.py` — `Source` protocol (scan → `SourceItem`s, load →
  payload); `FolderSource`, `CsvSource`, `TableSource` (SQL rows, optional
  `batch_size` streaming mode, `source_id` built without leaking
  credentials). `key=` is optional on Csv/Table: omit it for
  content-addressed `row-<hash>` lanes (identical rows collapse), or declare
  it for stable legible lanes — a non-unique declared key raises in
  `_finalize` (no more content-suffixing). A coordinate is a **lane key**:
  engine-facing dataflow/incrementality key, unique within a scan, stable
  across scans (or content-addressed). Not identity (that's the output
  address), not the search handle (that's `index=`).
- `src/rubedo/planning.py` — read-only plan phase: `_plan_step` emits a
  `StepDecision` (reuse/execute/blocked/pending/filtered) per lane;
  addresses = `hash(step, version, input_hash[, params][, code])`;
  staleness, code-drift, `EphemeralRef` (skip_cache fusion) live here.
  Per shape: reduce → one decision per group (`_group_reduce_lanes`); expand
  → one execute decision per parent lane, reused without re-running the fn via
  a parent-addressed cache anchor (`_plan_expand_reuse`); join → one decision
  per matched tuple (`_plan_join`). `group_key`/`join_on` read `index` rows at
  plan time, so planning stays value-free.
- `src/rubedo/execution.py` — DB-free execute phase: thread or process pool
  (per `step.executor`), retry loop, rate limiter, per-run memo for
  skip_cache utils.
- `src/rubedo/ledger.py` — every DB write: manifests, per-lane statuses,
  events, and `_commit_materialization` (the generations protocol:
  identical bytes reuse/restore, different bytes supersede; every liveness
  transition appends a `materialization_lifecycle` row).
- `src/rubedo/runner.py` — orchestration: `run()`, `plan()` (dry-run,
  writes nothing), `run_pipeline()`.
- `src/rubedo/models.py` — schema + **immutability guards**: ledger tables
  are append-only (ORM update/delete raises `ImmutabilityError`); the only
  mutable columns anywhere are projections (`Run` lifecycle columns,
  `Materialization.is_live`/`refreshed_at`). Tests that must backdate rows
  use raw SQL deliberately. A `before_commit` session guard (the pairing
  guard) additionally enforces invariant 8: every `is_live`/`refreshed_at`
  flip must ship a `materialization_lifecycle` row for that materialization in
  the same transaction. It accumulates across flushes (the supersede path
  flushes a demotion before its lifecycle row exists) and skips savepoint
  releases (`in_nested_transaction()`).
- `src/rubedo/selection.py` — `Selection` + `Selection.parse()` (the query
  language: lane-key globs, indexed fields, `version:<2.0`-style semantic
  version ranges via `packaging.SpecifierSet`) + the materialization query.
- `src/rubedo/server.py` — read-only FastAPI + invalidation endpoint.
  Ledger-derived only; never imports user pipelines.
- `web/` — React/Vite dashboard. `DagView.tsx` renders definition
  snapshots. Dark-theme CSS variables in `index.css`.

## Test conventions

Every engine test file uses the same fixture shape (copy from
`tests/test_index.py`): per-test `.test_<name>_data` (scanned folder) and
`.test_<name>_env` (object store) directories — **never nest the store
inside the scanned folder** — plus a per-test in-memory shared-cache SQLite
with StaticPool. Steps are defined inline with `@step`; hold the
`pipeline(...)` return value and pass it to `run(pipe)` — there are no
string ids. `.test_*/` is gitignored.

## Known sharp edges

- Redefining a step function with the same version in one test triggers the
  code-drift `UserWarning` (by design) — acknowledge with
  `@pytest.mark.filterwarnings`.
- `_commit_materialization`'s supersede path flushes the demotion *before*
  inserting the replacement (one-live-per-address partial unique index).
- Each `examples/<name>/` is a self-contained folder (script + its data);
  the flagship is `examples/count_lines/count_lines.py`. LLM examples read
  `OPENROUTER_API_KEY` from a gitignored `.env` at the repo root.
- The repo lives under `~/Documents` (macOS TCC-protected): if every file
  op suddenly returns EPERM, the app lost its Documents grant — tell the
  owner; nothing in-repo fixes it.
