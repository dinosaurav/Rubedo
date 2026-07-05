# Working on Rubedo (agent instructions — canonical)

Local-first batch engine: DAG pipelines over keyed collections (files, CSV
rows) with content-addressed caching, an append-only run ledger, and
surgical invalidation. Think "dbt state for Python tasks," built for
non-idempotent steps (LLM calls, scraping). Read `README.md` for the user
view and `docs/invariants.md` for the vocabulary and guarantees — both are
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
  warnings), `uv run ruff check rubedo/ tests/ examples/`,
  `(cd web && npx tsc -b)` when web changed, plus a live end-to-end of the
  changed behavior (the examples, or a small inline script; for API changes
  start uvicorn on a spare port and curl it).
- **Design-first**: for anything ambiguous or conceptual, propose to the
  owner before building. Start with `docs/TODO.md`; item 2 (joins)
  must not be built without an owner design session. The specs in `docs/TODO.md` already contain the settled
  decisions — do not re-litigate them, but do flag genuine contradictions.
- **Ruthless simplification** is a project value: prefer deleting a concept
  to adding a knob.

## Architecture map

- `rubedo/spec.py` — `@step` / `pipeline()` build plain
  `StepSpec`/`PipelineSpec` objects. No registry: the engine never imports
  user code. `describe()` renders DAGs (text/Mermaid); `definition()` is the
  JSON snapshot each run records.
- `rubedo/sources.py` — `Source` protocol (scan → `SourceItem`s, load →
  payload); `FolderSource`, `CsvSource`. A coordinate is a **lane key**:
  engine-facing dataflow/incrementality key, unique within a scan (sources
  disambiguate collisions mechanically), stable across scans. Not identity,
  not the search handle.
- `rubedo/planning.py` — read-only plan phase: `_plan_step` emits a
  `StepDecision` (reuse/execute/blocked/pending/filtered) per lane;
  addresses = `hash(step, version, input_hash[, params][, code])`;
  staleness, code-drift, `EphemeralRef` (skip_cache fusion) live here.
- `rubedo/execution.py` — DB-free execute phase: thread pool, retry
  loop, rate limiter, per-run memo for skip_cache utils.
- `rubedo/ledger.py` — every DB write: manifests, per-lane statuses,
  events, and `_commit_materialization` (the generations protocol:
  identical bytes reuse/restore, different bytes supersede; every liveness
  transition appends a `materialization_lifecycle` row).
- `rubedo/runner.py` — orchestration: `run()`, `plan()` (dry-run,
  writes nothing), `run_pipeline()`.
- `rubedo/models.py` — schema + **immutability guards**: ledger tables
  are append-only (ORM update/delete raises `ImmutabilityError`); the only
  mutable columns anywhere are projections (`Run` lifecycle columns,
  `Materialization.is_live`/`refreshed_at`). Tests that must backdate rows
  use raw SQL deliberately. A `before_commit` session guard (the pairing
  guard) additionally enforces invariant 8: every `is_live`/`refreshed_at`
  flip must ship a `materialization_lifecycle` row for that materialization in
  the same transaction. It accumulates across flushes (the supersede path
  flushes a demotion before its lifecycle row exists) and skips savepoint
  releases (`in_nested_transaction()`).
- `rubedo/selection.py` — `Selection` + `Selection.parse()` (the query
  language) + the materialization query.
- `rubedo/server.py` — read-only FastAPI + invalidation endpoint.
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
