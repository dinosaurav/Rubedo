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
  `uv run python examples/count_lines/count_lines.py` twice (expect Created: 22 then
  Reused: 22 — 7 files x 3 steps + 1 reduce; TODO 14 made the source root's
  own per-lane commit count too). Say so in the commit message.
- **Verification checklist**: `uv run pytest -q` (all green, no new
  warnings), `uv run ruff check src/rubedo/ tests/ examples/`,
  `uv run mypy src/rubedo`, `(cd web && npx tsc -b)` when web changed, plus a live end-to-end of the
  changed behavior (the examples, or a small inline script; for API changes
  start uvicorn on a spare port and curl it).
- **Design-first**: for anything ambiguous or conceptual, propose to the
  owner before building. Start with `notes/TODO.md` for open work — the
  specs there already contain the settled decisions (do not re-litigate
  them, but do flag genuine contradictions). The producer model —
  content-addressed lanes, `expand`, `group_key`, multi-source, and N-way
  `join` — is designed and built; see `notes/producer-model.md`.
- **TODO conventions**: items keep their historical numbers (gaps are
  shipped items — see the TODO's Done changelog); every open item's design
  is settled and the spec is buildable as written. Most items carry a
  **Trap:** paragraph that is part of the spec — and items tagged
  **[⚠️ subtle]** or **DANGEROUS** doubly so: read it *and*
  `notes/invariants.md` before coding, satisfy the
  acceptance line verbatim, and never "simplify" the guarded behavior away
  to make a fix easier.
- **Ruthless simplification** is a project value: prefer deleting a concept
  to adding a knob.

## Architecture map

- `src/rubedo/spec.py` — pure data leaf: `StepSpec`/`PipelineSpec`
  dataclasses plus `step()`, `source()`, and `definition()` (the JSON
  snapshot each run records). No registry: the engine never imports user
  code. `shape` ∈ `map` (1:1, default) / `reduce` (N:1 fan-in over a
  parent's surviving lanes; `group_key` partitions into one output per
  indexed-field value, else a single `"@all"`) / `expand` (1:N — the fn
  yields payloads, minting content-addressed `row-<hash>` child lanes; **no
  `depends_on` = a root = a source** that yields the initial lanes and
  re-runs every run, so `pipeline(steps=[...])` needs no separate ingestion
  concept — see `@source`) / `join` (N-way equijoin on
  `join_on={parent: indexed_field}`, minting `a|b|…` pair lanes). A
  **source-less `map` root** (no `depends_on`) mints a single `@root` lane
  whose input is its params (or a constant) — so a pipeline can begin with
  a plain step fed a value instead of scanning for one; same params reuse,
  changed params recompute (`ROOT_LANE` in `planning.py`). A pipeline may
  declare several `@source` roots; `join` doesn't care that its parents are
  roots. `executor` is `"thread"` (default) or `"process"` (a `loky` pool
  serializing via `cloudpickle`, so closures are fine). **`spec.py` never
  imports `pipeline.py`/`runner.py`/`scheduler.py`** — the owner considers
  it a flagship human-readable file; validation and machinery live above it
  (TODO 15's whole point: rotate the dependency so no lazy imports are
  needed).
- `src/rubedo/pipeline.py` — sits *above* the engine (imports `runner.py`):
  `Pipeline` (steps register via `@p.step`/`@p.source` or `steps=[...]`;
  verbs are methods — `.run()`/`.plan()`/`.describe()`/`.definition()`) and
  the `pipeline()` factory that constructs one. `_build_spec` does the
  validation the old free `pipeline()` builder did (at least one root,
  skip_cache/join/group_key consistency) — run lazily on first `.spec`/verb
  access and cached, not at construction (`.build()` is gone). `name` is
  the pipeline's sole identity (no `id=`); `schedule=`/`home=` join
  `retention=`/`params_model=` as construction-time settings.
- `src/rubedo/render.py` — `describe()` (text/Mermaid/ascii DAG rendering)
  and the ascii layout internals (`_AsciiNode`, `_ascii_layers`,
  `_ascii_positions`, `_describe_ascii`). Sits above `spec.py` and
  `planning.py` (both imported at module level — rendering needs
  topological order); `Pipeline.describe()` delegates here.
- `src/rubedo/planning.py` — read-only plan phase: `_plan_step` emits a
  `StepDecision` (reuse/execute/blocked/pending/filtered) per lane;
  addresses = `hash(step, version, input_hash[, params][, code])`;
  staleness, code-drift, `EphemeralRef` (skip_cache fusion) live here.
  Per shape: reduce → one decision per group (`_group_reduce_lanes`); expand
  → one execute decision per parent lane, reused without re-running the fn via
  a parent-addressed cache anchor (`_plan_expand_reuse`); join → one decision
  per matched tuple (`_plan_join`). `group_key`/`join_on` read `index` rows at
  plan time, so planning stays value-free. Untouched by TODO 15.
- `src/rubedo/execution.py` — DB-free execute phase: thread or process pool
  (per `step.executor`), retry loop, rate limiter, data quality assertions (`step.assertions`), per-run memo for
  skip_cache utils.
- `src/rubedo/ledger.py` — every DB write: per-lane statuses,
  events, and `_commit_materialization` (the generations protocol:
  identical bytes reuse/restore, different bytes supersede; every liveness
  transition appends a `materialization_lifecycle` row).
- `src/rubedo/scheduler.py` — the segment machinery: `_partition_segments`
  (topo order → `broad` singleton segments or `deep` runs of consecutive
  ≤1-parent map steps) and `_run_segment`, the one scheduler over (lane,
  step) cells (all ledger writes in the main thread — workers only run step
  functions). reduce/join/expand/multi-parent maps are barrier segments.
  Order only — ledger rows identical either way.
- `src/rubedo/runner.py` — orchestration: internal `run()`/`plan()`
  (`Pipeline.run()`/`Pipeline.plan()` delegate to these — not exported from
  `rubedo.__init__`, see TODO 15) and `run_pipeline()`, which drives every
  segment from `scheduler.py` and records the `Run` row/retention. All
  ledger writes happen in the main thread (restated at the top of this file
  and of `scheduler.py`).
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
- `src/rubedo/gc.py` — retention GC: demote (paired `pruned` lifecycle
  rows) then sweep (delete bytes only when *every* referencing
  materialization across all pipelines is non-live, logged in append-only
  `object_reclamations`). `pipeline(retention=N)` auto-prunes at end of
  run; `gc()` / `rubedo gc [--max-bytes] [--delete]` is dry-run by
  default and refuses while any run's heartbeat is live. Expand anchors
  (live mats with zero `RunCoordinateStatus` refs) are always kept.
- `src/rubedo/selection.py` — `Selection` + `Selection.parse()` (the query
  language: lane-key globs, indexed fields, `version:<2.0`-style semantic
  version ranges via `packaging.SpecifierSet`) + the materialization query.
- `src/rubedo/server.py` — read-only FastAPI + invalidation endpoint.
  Ledger-derived only; never imports user pipelines.
- `web/` — React/Vite dashboard. `DagView.tsx` renders definition
  snapshots. Light-themed ("blueprint") CSS variables in `index.css`. The UI is purely read-only.

## Test conventions

Every engine test file uses the same fixture shape (copy from
`tests/test_index.py`): per-test `.test_<name>_data` (scanned folder) and
`.test_<name>_env` (object store) directories — **never nest the store
inside the scanned folder** — plus a per-test in-memory shared-cache SQLite
with StaticPool. Steps are defined inline with `@step`; hold the
`pipeline(...)` return value (a `Pipeline`) and call `.run()` on it — there
are no string ids, and `name` is the pipeline's sole identity (no `id=`
kwarg — TODO 15). `.test_*/` is gitignored.

Ingestion has no separate concept (TODO 14): there is no `folder=` pipeline
kwarg. A test folder is scanned by a root `@step(shape="expand")` — the
folder recipe from `docs/concepts/sources.md` — that the downstream step
`depends_on`:

```python
@step(name="scan", version="1", shape="expand")
def scan():
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@step(name="extract", version="1", depends_on=["scan"])
def extract(scan: dict):
    text = scan["text"]
    ...

pipe = pipeline(name="ix", steps=[scan, extract])
pipe.run(workers=1)
```

Two consequences worth knowing before writing an assertion: lanes are
content-addressed (`row-<hash>`, not the relative path), so a test that
needs to identify *which* file a lane came from indexes the `path` field
(`@step(index=["path"])`) and looks it up rather than asserting a
`"a.txt"`-shaped coordinate; and every run outcome count (`created_count`/
`reused_count`/…) is one step deeper than before — a single-file fixture
through a one-step chain now reports 2 (the `scan` lane *and* the
downstream lane), not 1.

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
