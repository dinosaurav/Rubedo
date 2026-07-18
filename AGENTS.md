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
  Reused: 22 — 7 files x 3 steps + 1 aggregate; TODO 14 made the source root's
  own per-lane commit count too). Say so in the commit message.
- **Verification checklist**: `uv run pytest -q` (all green, no new
  warnings), `uv run ruff check src/rubedo/ tests/ examples/`,
  `uv run mypy src/rubedo`, `(cd web && npx tsc -b)` when web changed,
  `(cd web && npm run build && npx playwright test)` when web or server
  changed, plus a live end-to-end of the
  changed behavior (the examples, or a small inline script; for API changes
  start uvicorn on a spare port and curl it).
- **Design-first**: for anything ambiguous or conceptual, propose to the
  owner before building. Start with `notes/TODO.md` for open work — the
  specs there already contain the settled decisions (do not re-litigate
  them, but do flag genuine contradictions). The producer model —
  content-addressed lanes, `expand`, `group_key`, multi-source, and N-way
  `join` — is designed and built; see `notes/producer-model.md`.
- **TODO conventions**: items keep their historical numbers (gaps are
  shipped/retired items — archived verbatim, with the full Done
  changelog, in `notes/TODO-obsolete.md`); an open item's spec is
  buildable as written unless tagged **[needs owner decision]** (settled
  problem, unratified fix — propose, don't build) or carrying a **⚠️
  respec** banner (re-verify pointers against current code first). Most
  items carry a **Trap:** paragraph that is part of the spec — and items
  tagged **[⚠️ subtle]** or **DANGEROUS** doubly so: read it *and*
  `notes/invariants.md` before coding, satisfy the
  acceptance line verbatim, and never "simplify" the guarded behavior away
  to make a fix easier.
- **Ruthless simplification** is a project value: prefer deleting a concept
  to adding a knob.

## Release process

Cutting a release is four steps, and **all four must be done before
building/publishing** — PyPI rejects file-name reuse, so a stale version
or a tag pointing at the wrong commit wastes a publish attempt:

1. **Bump `version` in `pyproject.toml`** to the new release number.
2. **Update `CHANGELOG.md`**: move entries from `[Unreleased]` into a
   new `## [X.Y.Z] - YYYY-MM-DD` section. Every shipped tag needs a
   changelog entry — no skipping.
3. **Commit both, push to `main`.**
4. **Tag the version-bump commit** (not an earlier one): `git tag vX.Y.Z
   <sha> && git push origin vX.Y.Z`. The tag must point at a commit where
   `pyproject.toml` already has the new version — otherwise a build from
   the tag produces the old wheel name and PyPI rejects it. If you tagged
   too early, `git tag -d vX.Y.Z && git tag vX.Y.Z <correct-sha> && git
   push origin vX.Y.Z --force` to retag.

## Architecture map

- `src/rubedo/spec.py` — pure data leaf: `StepSpec`/`PipelineSpec`
  dataclasses plus `step()` and `definition()` (the JSON
  snapshot each run records). No registry: the engine never imports user
  code. `StepSpec` carries `in_shape`/`out_shape` as the primary fields;
  the legacy `shape=` kwarg on `step()` is translated to the pair and
  never stored. The four conceptual shapes: `map`
  (`in_shape="one", out_shape="one"`, 1:1, default) / **`aggregate`**
  (`in_shape="aggregate", out_shape="one"` — N:1 fan-in over a parent's
  surviving lanes; was called "reduce"; `group_key` partitions into one
  output per field value read from the parent's output dict, else a single
  `"@all"`) / `expand` (`in_shape="one", out_shape="many"` — 1:N; the fn
  yields payloads, minting content-addressed `row-<hash>` child lanes; **no
  `depends_on` = a root = a source** that yields the initial lanes and
  is anchor-cached by default — sources that watch external state declare
  `check_cache=False` to re-enumerate each run — so `pipeline(steps=[...])`
  needs no separate ingestion
  concept — a parentless generator `@step` infers this shape automatically)
  / `join` (`in_shape="join", out_shape="many"` — N-way equijoin on
  `join_on={parent: field}`, minting `a|b|…` pair lanes; the field is
  read from the parent's output dict). A
  **source-less `map` root** (no `depends_on`) mints a single `@root` lane
  whose input is its params (or a constant) — so a pipeline can begin with
  a plain step fed a value instead of scanning for one; same params reuse,
  changed params recompute (`ROOT_LANE` in `planning.py`). A pipeline may
  declare several source-shaped roots; `join` doesn't care that its parents
  are roots. `executor` is `"thread"` (default) or `"process"` (a `loky` pool
  serializing via `cloudpickle`, so closures are fine). **`spec.py` never
  imports `pipeline.py`/`runner.py`/`scheduler.py`** — the owner considers
  it a flagship human-readable file; validation and machinery live above it
  (TODO 15's whole point: rotate the dependency so no lazy imports are
  needed).
- `src/rubedo/pipeline.py` — sits *above* the engine (imports `runner.py`):
  `Pipeline` (steps register via `@p.step` or `steps=[...]`;
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
  Reuse checks consult `input_hash_usages.fulfilled` (liveness gate) +
  `lane_store.find_latest_filled_by_address` (content retrieval) via
  `batch_lookup_by_address`. Per shape: aggregate → one decision per group
  (`_group_reduce_lanes`, reads `group_key` field from the parent's
  output dict); expand → one execute decision per parent lane,
  reused without re-running the fn via a parent-addressed cache anchor;
  join → one decision per matched tuple (`_plan_join`, reads `join_on`
  fields from parent output dicts).
- `src/rubedo/execution.py` — DB-free execute phase: thread or process pool
  (per `step.executor`), retry loop, rate limiter, data quality assertions (`step.assertions`), per-run memo for
  skip_cache utils.
- `src/rubedo/lane_store.py` — per-step Arrow IPC files under
  `.rubedo/tables/`: append-only rows of lane metadata (row_id, lane_key,
  address, input_hash, code_version, output, output_identity, content_type,
  code_hash, ts, run_id, filtered). `output` holds the value
  itself in native Arrow type (struct for dicts, int64 for ints, string) when
  all lanes in a step are inline; falls back to `string` (JSON-serialized
  inline + `"objects:<hash>"` ref strings) when any value spills.
  `output_identity` is the content identity hash (for downstream
  `input_hash` computation), computed once at commit time from the original
  output value and stored directly — plan time reads it from the column
  instead of recomputing from the Arrow-read-back value, so the union struct
  null-fill (heterogeneous dict key sets) doesn't shift the identity.
  `content_type` distinguishes `"text"` (native string return), `"json"`
  (inline or JSON-serialized), and `"bytes"`/`"arrow-ipc:<kind>"` (spilled).
  Pure data — no tombstones, no liveness. The
  `batch_lookup_by_address` function is the planning phase's reuse lookup
  (SQLite `input_hash_usages` for liveness, Arrow for content).
- `src/rubedo/ledger.py` — every DB write: per-lane statuses,
  events, the commit path (Arrow row via `lane_store.append_filled` +
  `input_hash_usages.fulfilled=True` + address-based `MaterializationEdge`;
  `_commit_materialization` is **deleted**), and the `input_hash_usages`
  claim (plan time, records `last_run_id` only — does NOT flip
  `fulfilled=False`) / fulfill (commit time, `fulfilled=True`) lifecycle.
  `mat_action` is determined by checking if the address was already
  fulfilled with matching `output_identity` (→ "reused"/"refreshed") or not
  (→ "created"). `output_identity` is computed once at commit time via
  `_identity_of` and stored in the Arrow column — no recompute at plan time.
  Arrow row only written for created/superseded/refreshed
  — pure reuse is a no-op.
- `src/rubedo/scheduler.py` — the segment machinery: `_partition_segments`
  (topo order → `broad` singleton segments or `deep` runs of consecutive
  ≤1-parent map steps) and `_run_segment`, the one scheduler over (lane,
  step) cells (all ledger writes in the main thread — workers only run step
  functions). aggregate/join/expand/multi-parent maps are barrier segments.
  Order only — ledger rows identical either way.
- `src/rubedo/runner.py` — orchestration: internal `run()`/`plan()`
  (`Pipeline.run()`/`Pipeline.plan()` delegate to these — not exported from
  `rubedo.__init__`, see TODO 15) and `run_pipeline()`, which drives every
  segment from `scheduler.py` and records the `Run` row/retention. All
  ledger writes happen in the main thread (restated at the top of this file
  and of `scheduler.py`).
- `src/rubedo/models.py` — schema + **immutability guards**: ledger tables
  are append-only (ORM update/delete raises `ImmutabilityError`); the only
  mutable columns anywhere are projections (`Run` lifecycle columns)
  and the `InputHashUsage` liveness columns (`fulfilled`, `last_run_id`,
  `claimed_at` — the one intentionally mutable ledger table:
  claim/fulfill/tombstone/demote are in-place updates). The
  `Materialization` model is **deleted** — no `materializations` table, no
  `is_live`, no `uq_live_output_address` index. `MaterializationEdge` is
  address-based (`parent_address`/`child_address`, no integer FKs).
  `RunCoordinateStatus` has no `materialization_id` column —
  `output_address` is the join key. See `notes/arrow-storage.md` for the
  full design and `notes/invariants.md` for the updated guarantees.
- `src/rubedo/gc.py` — retention GC: demote (set IHU `fulfilled=False`)
  then sweep (delete bytes only when *every* referencing materialization
  across all pipelines is non-live, logged in append-only
  `object_reclamations`). `pipeline(retention=N)` auto-prunes at end of
  run; `gc()` / `rubedo gc [--max-bytes] [--delete]` is dry-run by
  default and refuses while any run's heartbeat is live. Expand anchors
  (live mats with zero `RunCoordinateStatus` refs) are always kept.
  Identity is `Set[str]` addresses; content hashes from
  `lane_store.all_filled_rows()` — no `Materialization` import.
- `src/rubedo/selection.py` — `Selection` + `Selection.parse()` (the query
  language: lane-key globs, output fields, `version:<2.0`-style semantic
  version ranges via `packaging.SpecifierSet`) + the materialization query.
  Output-field selection scans the Arrow `output` struct column directly
  (no SQLite `materialization_index` table).
- `src/rubedo/server.py` — FastAPI server: read-only API + UI plus one
  write endpoint (`POST /api/selection/invalidate` — unauthenticated,
  meant for local use). Ledger-derived only; never imports user pipelines. Serves the built web
  UI from `web_static/` (SPA fallback to `index.html`); `rubedo serve`
  wraps uvicorn so one command gives the full dashboard. The web app's
  `api.ts` uses a relative `/api` URL — same-origin in production, proxied
  to `:8000` by Vite in dev.
- `web/` — React/Vite dashboard. `DagView.tsx` renders definition
  snapshots. Light-themed ("blueprint") CSS variables in `index.css`. The UI is purely read-only.
  `vite.config.ts` builds to `src/rubedo/web_static/` and proxies `/api` in
  dev. Playwright e2e specs in `web/tests/` spawn a backend with a temp
  `RUBEDO_HOME` and verify the SPA renders real ledger data.

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
kwarg. A test folder is scanned by a bare-`@step` root — a parentless
generator infers `out_shape="many"` (a `shape="expand"` alias — the folder recipe from
`docs/concepts/sources.md`) — and the downstream step's parameter name is
its dependency declaration. Tests use this terse form throughout: no
`name=`/`version=`/`shape=`/`depends_on=` unless the kwarg is the test's
subject (version bumps, drift, validation errors), the name genuinely
differs from the function's, or the shape can't be inferred — a plain
`@all` aggregate keeps `shape="reduce"` (alias) or `in_shape="aggregate"`,
and aggregate/join steps keep an
explicit `depends_on=` (parent counts validate at decoration time, before
build-time inference runs):

```python
@step
def scan():
    for name in sorted(os.listdir(TEST_FOLDER)):
        path = os.path.join(TEST_FOLDER, name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@step
def extract(scan: dict):
    text = scan["text"]
    ...

pipe = pipeline(name="ix", steps=[scan, extract])
pipe.run(workers=1)
```

Two consequences worth knowing before writing an assertion: lanes are
content-addressed (`row-<hash>`, not the relative path), so a test that
needs to identify *which* file a lane came from reads the `path` field
from the output dict and looks it up rather than asserting a
`"a.txt"`-shaped coordinate; and every run outcome count (`created_count`/
`reused_count`/…) is one step deeper than before — a single-file fixture
through a one-step chain now reports 2 (the `scan` lane *and* the
downstream lane), not 1.

## Performance changes

`benchmarks/` is the before/after harness (not part of pytest; docs in
`benchmarks/README.md`). For any perf-motivated change:

- **Measure both sides**: `uv run python benchmarks/bench.py run --label
  before` on the baseline commit, `--label after` on your change, then
  `compare before after` (same `--scale` both sides; results are
  gitignored JSON tagged with the git sha, so labels survive checkouts).
  Quote the relevant compare lines in the commit body.
- **Scenario families**: `micro_*` isolate lane_store + SQLite hot paths
  with synthetic history; `run_*` drive a real pipeline end-to-end;
  `plan_deep_*` measure a pure `.plan()` on the map-root →
  dependent-expand shape (the one shape where a dry run resolves every
  lane — an expand *source* reports `pending` downstream);
  `shape_*` pit two pipeline shapes against each other (e.g. the
  skip_cache quartet). Scenarios report **work counters** alongside
  times — Arrow rows written per step, reuse lookups, disk-table cache
  misses, SQL statements (`sqlite_stmts`), `util_fn_calls`. Counters,
  not timing, are how you show a shape or
  change "does no extra work"; `compare` prints any counter that drifted.
- **If your hot path isn't covered, add a scenario** — it's ~15 lines:
  `@scenario("name", repeats=N)` taking `(params, repeats)` and returning
  `times` or `(times, counters)`; build state with `fresh_env()` /
  `make_files()` / `seed_step_history()`, time with `timed()` or
  `timed_counted()`, and use `drop_table_cache()` to model a fresh
  process on a warm store. Copy the `make_util_pipeline` +
  `_bench_util_shape` pattern for shape pairs; count step-fn executions
  with a closure list. Keep scenario names stable — `compare` aligns by
  name.
- Counters are visibility, not assertions: when a guarantee is
  load-bearing ("skip_cache never materializes"), pin it in pytest too
  (`tests/test_skip_cache.py` shows the closure-counting trick).
- Harness caveat: `WorkCounters` wraps `lane_store` **module
  attributes** — it sees all current call sites because they resolve at
  call time (`lane_store.append_filled(...)` or function-local `from
  .lane_store import ...`). A module-top `from .lane_store import X` in
  engine code would silently bypass the counters; keep the existing
  import style.

## Known sharp edges

- Redefining a step function with the same version in one test triggers the
  code-drift `UserWarning` (by design) — acknowledge with
  `@pytest.mark.filterwarnings`.
- Each `examples/<name>/` is a self-contained folder (script + its data);
  the flagship is `examples/count_lines/count_lines.py`. LLM examples read
  `OPENROUTER_API_KEY` from a gitignored `.env` at the repo root.
- The repo lives under `~/Documents` (macOS TCC-protected): if every file
  op suddenly returns EPERM, the app lost its Documents grant — tell the
  owner; nothing in-repo fixes it.
