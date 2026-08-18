# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] - 2026-08-18

Breaking step-interface rewrite (TODO 39–41). No compat shim. Dev-stage:
`rm -rf .rubedo` — old `definition()` snapshots still have `in_shape` /
`out_shape` / `skip_cache` / `check_cache`.

### Added
- **`shape="join_table"` / `p.join_table(...)`.** Same `join_on` /
  `join_mode` / null-key / duplicate-key rules as pair-lane `join`, one
  table-valued `@all` coordinate. Parents must be `as_table=True`.
  `join_on=` still infers pair-lane `join`.
- **`as_table=True`.** A DataFrame / `pa.Table` is one cache entry in the
  step's existing coordinate(s) — not an implicit explode. Returning a
  frame without the flag errors.
- **`row_key=` on expand.** Mint dict lanes from a table parent; identity
  is that column (missing/duplicate keys raise), not full-payload hash
  collapse.
- **`pipeline(cache_default=...)`.** Maps that omit `use_cache=` inherit
  it. Library default `True` (every map stores). Compilers that emit
  cheap formulas pass `False` and mark loads / expands / LLM steps
  `use_cache=True`. expand / join / aggregate / fold always store.
- **Fused row kernel.** A `use_cache=False` map whose one parent is a
  table and whose parent parameter is annotated `dict` runs per inner
  row and stacks the result (scalar → column named after the step; dict
  → table of those rows). Annotate a DataFrame/Table for one vectorized
  call. Dict-lane parents still zip.

### Changed
- **One `shape=`.** `StepSpec.shape` is `map` / `expand` / `aggregate` /
  `fold` / `join` / `join_table` instead of `in_shape`/`out_shape`.
  Inference is unchanged in spirit: a generator is `expand`, `join_on=`
  is `join`, `group_key=` is `aggregate`, `fold_init=` is `fold`; a
  plain `@all` aggregate still needs `shape="aggregate"`. Maps zip
  parent coordinates and never mint. Only expand, pair `join`, and
  grouped aggregate/fold mint lanes. `definition()` snapshots `shape`
  (if not map), `as_table`, `table_input`, `row_key`.
- **Expand minting.** The fn must `yield` or `return` a list/tuple. A
  dict or str no longer fans out keys/characters. Expand + DataFrame
  errors unless `row_key=` is set.
- **Aggregate table input.** Inferred from a parent-parameter annotation
  (`pa.Table` / `pl.DataFrame` / `pd.DataFrame`). `arrow_aggregate=` is
  gone.
- **`use_cache=False` (was `skip_cache=True`).** Fused map: never
  materialized, identity folds into consumers, never mints. Maps only;
  a fused step still needs a consumer. Join / `group_key` parents cannot
  be fused (plan reads committed output fields).
- **`force=True` (was `check_cache=False`).** Per-step form of
  `run(force=True)`: skip reuse, still commit. Right for a folder scan
  or API poll. This is not `use_cache=False`.
- **Docs.** README, How it works, shapes, sources, enrichment, `/llms.txt`,
  and the landing snippets match `shape=` / `use_cache` / `force` /
  `cache_default`.

### Removed
- `in_shape=` / `out_shape=` (`one`/`many`) and the `shape=` alias that
  translated into that pair.
- `skip_cache=` / `check_cache=`.
- `arrow_aggregate=`.

## [0.5.1] - 2026-08-15

### Changed
- **Per-lane census events dropped:** `run_events` no longer writes
  `step_cache_hit`, `materialization_{created,reused,refreshed}`,
  `step_filtered`, `step_blocked`, or `step_processing_started`. Those
  duplicated `RunCoordinateStatus` (the one-row-per-cell census) with no
  extra data. The event log stays for sparse audit: run lifecycle,
  retries, failures, code-drift, `partial_fan_in`, retention. Existing
  homes keep historical rows; new runs do not write them.

- **Batched ledger commits on plan (`RUBEDO_LEDGER_BATCH`):** `plan_cells`
  used to `session.commit()` after every lane — a fsync storm under
  `schedule="deep"` reuse. Planned rows now commit in batches of 128
  (set `RUBEDO_LEDGER_BATCH=1` to restore the old cadence). Execute
  decisions still flush immediately so nested execute transactions
  cannot deadlock the SQLite writer. `progress_cb` for reuse/skip
  fires after that commit so nested ledger reads (ai-table's
  mid-run `_resolve`) see the rows. Reuse also passes the Arrow
  ``output`` already loaded during plan as optional 4th/5th callback
  args so product engines can skip that nested read entirely.

- **Identity everywhere:** PyPI `description`, `/llms.txt`, and `AGENTS.md`
  now open with the same line as the landing and README (a Python library
  for batch pipelines that remember every step). Mechanism nouns stay in
  the At a glance / 10-line contract.

- **One cheap example, one paid proof.** Docs home uses the same
  no-key folder `count_lines` snippet as the README; the tutorial stays
  the copy-paste classifier; the landing keeps `inbox` → `decide` as the
  LLM walkthrough. README later sections point at docs instead of
  re-teaching. How-to pages lead with the job; the dashboard is on the
  Inspect a run opening and in the tutorial Next. Off-nav long-form
  (shapes, sources, versioning, producer-model, Arrow, retention-design)
  is excluded from MkDocs search. Getting-started and retention stubs
  are clearer jump pages.

- **Landing:** the 8 / 0 / 1 LLM-call stats sit under the hero CTA so
  the reuse proof is above the fold; code + walkthrough stay below.

- **Docs slash:** public nav is Home → Tutorial → Examples → How it works
  (one page) → How to (six jobs) → Reference → Invariants. First-run folded
  into the tutorial; retention folded into Inspect a run. Shapes, sources,
  versioning, producer-model, Arrow, and retention-design stay as long-form
  URLs (banners point back) but are off the sidebar. Getting-started and
  the retention guide are stubs so old links still resolve.

- **Docs IA:** nav is now Start → How it works → How to → Reference →
  Internals instead of a flat encyclopedia. Home matches the landing/README
  dual-audience pass (identity first, At a glance for crawlers). Getting
  started is a short first-run page; concept and guide titles are jobs
  ("What Rubedo remembers", "Find and invalidate a row") rather than
  mechanism nouns. File paths are unchanged so existing URLs still resolve.

- **README:** same dual-audience pass as the landing page. Opens with what
  Rubedo is (a Python library for batch pipelines) and the last-step /
  don't-re-pay job; a compact "At a glance" block keeps the mechanism
  nouns (content-addressed caching, ledger, surgical invalidation,
  dbt-style state, address formula) on the first screen for crawlers.
  Quickstart and the LLM/CSV example walk through what the code is doing;
  `created=2` is explained as two steps for one file.

- **Marketing landing (rubedo.run):** identity-first rewrite. Hero now
  says what Rubedo is (a Python library for batch pipelines) and walks
  through the example instead of captioning a DAG. Human outcomes lead
  (don't re-pay the last-step fix); mechanism language (content-addressed
  caching, ledger, dbt-style incrementality, surgical invalidation) stays
  in bodies, FAQ, meta, and a compact "At a glance" aside for crawlers.
  Proof (dashboard + reuse numbers) sits above Try it; Compare is demoted.

## [0.5.0] - 2026-08-12

### Added
- **Symmetric outer join (`join_mode`, TODO 38):** `join_mode="intersect"`
  (default, today's inner join) or `"union"` on `@step(..., join_on=...)`
  and declarative `p.join(...)`. Union emits the ∪ of per-side join keys
  with absent sides bound as Python `None` (pair coordinates use the
  reserved `@missing` segment). All sides are equal — no left/right.
  Join `input_hash` always reserves a slot per `depends_on` parent
  (absent → `@missing` sentinel) so extending `join_on` cannot collide
  with older addresses; `join_mode` itself is not part of cache identity,
  so flipping intersect↔union reuses matched pairs. Null join-*field*
  values still raise (they never share a null bucket). Under union with
  `on_failed="use_passed"`, failed parent lanes look like unmatched
  (plan warns). See `docs/concepts/shapes.md` and
  `docs/guides/data-enrichment.md`.
- **Join duplicate-key warning:** plan emits a `UserWarning` when any
  participating join key has duplicate lanes on a side (cartesian
  fan-out), so messy lookup tables are visible instead of silently
  multiplying enrich rows.
- **Data enrichment guide:** normalize → dedupe (`group_key`) →
  intersect/union/anti-join practices in
  `docs/guides/data-enrichment.md`.

## [0.4.3] - 2026-08-04

### Fixed
- **Same quadratic-scan bug on the commit path (`ledger.py`):**
  `_commit_execution_result`'s reuse-identity check called the same
  uncached `address_row_index()` from 0.4.2's fix, once per committed
  lane, on the write path for every `check_cache=False` source re-run —
  a documented, encouraged pattern, not an edge case. Swapped in
  `lane_store.rows_by_address`, the same per-step cached, anchor-aware
  address lookup the planning phase already uses for reuse checks.
- **Broadcast/singleton dependencies raised "disjoint lane sets"
  (TODO 37):** `_plan_step` required every named dependency to have a
  materialization at the exact same coordinate as the step being
  planned. That broke whenever one dependency was a true singleton (a
  source-less map root, or an aggregate/fold with no `group_key` —
  always exactly one coordinate for the whole run) mixed with a real
  per-row dependency. `planning.singleton_coordinate_steps` now
  statically classifies such steps; `_plan_step` resolves a singleton
  dependency via its one materialization and broadcasts it to every
  real per-row target instead of requiring exact-coordinate equality.
  Two genuinely unrelated multi-lane producers still raise the same
  error — that strictness is unchanged. See
  `docs/concepts/shapes.md#broadcasting-a-single-value-into-per-row-steps`.

## [0.4.2] - 2026-08-04

### Fixed
- **Quadratic hang in `GET /api/runs/{id}/view`:** `build_run_view`'s
  per-coordinate preview lookup called the uncached
  `home.lanes.address_row_index()` once per coordinate row instead of
  once per request; each call rescans and re-parses every step's Arrow
  table in the lane store. A run with thousands of coordinates could
  turn the endpoint into an effective hang. Fixed by computing the
  address index once per `build_run_view` call, matching every other
  call site's pattern.

## [0.4.1] - 2026-08-03

### Added
- **Grouped Run View:** `GET /api/runs/{id}/view` projects a run into
  definition-driven sections (branch / join / child / summary / fold).
  The dashboard Run Detail page defaults to this layout, with created /
  reused / failed tinting and click-to-expand cell and summary previews.

### Changed
- Docs accuracy: expand-root caching is anchor-cached on `@root` unless
  `check_cache=False` (shapes, inspecting-runs, getting-started, tutorial);
  coordinates include `@root` / `@all` / join keys, not only `row-<hash>`;
  dashboard UI is read-only while `POST /api/selection/invalidate` exists;
  Home docs mention the Arrow `tables/` plane; join parents need not be
  expand roots; Run View documented in inspecting-runs and README.
- Marketing site: prerender the landing page so it is indexable without JS.

## [0.4.0] - 2026-07-21

### Added
- **Pass-by-reference spilled payloads (TODO 13):** when the object store
  is remote and a step uses `"process"` or a factory executor, spilled
  parents are shipped as `objects:<hash>` refs. Workers rebuild an
  `S3Store` from picklable `store_config`, fetch inputs, run assertions,
  and PUT spill-worthy results. `run(payload_refs=False)` forces hub
  routing; a failed worker probe warns once and degrades by value.
  Inline-only pipelines never engage the shim.
- **Bring-your-own execution pools (TODO 8):** `executor=` accepts a
  zero-argument factory returning any Future-shaped pool alongside
  `"thread"` and `"process"`. Factories are called once per step segment,
  returned pools are shut down by Rubedo, and definition snapshots record
  stable `external:<module>.<qualname>` markers without changing cache
  identity. Includes fake-pool parity tests and an optional Dask
  `LocalCluster` example.
- **Postgres ledger coverage (TODO 7b):** real env-gated and CI service
  tests cover schema creation, IHU claim/fulfill, concurrent collision
  upserts, lineage-edge dedupe, ORM immutability, and query/selection
  behavior. Ledger claim/fulfill and edge writes now use atomic
  SQLite/Postgres `ON CONFLICT` statements instead of check-then-insert
  races. Development installs include psycopg 3.
- **S3-compatible object store (TODO 7, object plane):** `ObjectStore`
  protocol with `LocalStore` + `S3Store` (AWS S3 / R2 / B2 / MinIO via
  endpoint URL). Configure with `Home(store=...)`, `Home(store_url=...)`,
  or `RUBEDO_STORE_URL` (`s3://bucket/prefix`). Optional extra:
  `pip install 'rubedo[s3]'`. `rubedo du` / GC sizing use sized inventory
  (zero per-object HEAD/GET). Destructive `gc(delete=True)` hard-refuses
  cloud stores until versioned-bucket gating; server object download
  streams from the store.
- **Shared cloud lane store (TODO 7, content plane):** an S3-backed Home
  automatically writes immutable Arrow segments under `tables/`, with
  `row_id` dedupe, LIST-etag cache invalidation, threshold compaction, and
  renewable conditional-put writer leases per pipeline. A second Home
  against the same ledger and bucket reuses the first's lanes. Read-only
  plans remain lease-free. Postgres correctness coverage remains item 7b.
- **Public read/query surface (TODO 35):** `Cell` is one (run, step, lane)
  outcome. `Home.cells` / `Home.current` / `Home.select` (and
  `RunSummary.cells`) share an implementation with `/api/current-outputs`.
  `home.select("step:scan path:a.txt")` replaces hand-rolled
  coord-for-path lookups. `RunSummary.output_for` stays the payload map,
  now via resolved cells.
- **`Home.ephemeral(...)`** — unshared Home (not interned); `fresh=` is
  the public constructor knob (replaces private `_fresh=`).

### Changed
- Docs swept for staleness against the 0.3.0 storage rewrite: dropped
  lingering references to the deleted `Materialization` model / `is_live`
  / `materialization_lifecycle` (search-and-invalidation, retention,
  tutorial guides now describe `input_hash_usages.fulfilled` instead);
  added `check_cache=False` to the getting-started/tutorial folder-scan
  snippets (missing it meant the documented "edit a file, only that lane
  recomputes" walkthrough no longer matched actual behavior, since root
  `expand` steps are anchor-cached by default since 0.3.0); added `fold`
  to every shape-count mention, `dask_executor`/`ray_executor`/
  `paper_scout` to `docs/examples.md`, and dropped the last `index=`/
  `reduce` mentions from `examples/README.md`.
- Docs reorganized by reader intent: Concepts keeps only the stable model
  pages; Cloud Storage moved to Guides; the Partial Runs and Run Diff
  pages merged into one "Trials: sample, diff, roll out" guide;
  Development split into a top-level Contributing entry and a Design
  Notes section that now also publishes `notes/arrow-storage.md`.
  `notes/` archives closed material under `notes/archive/`
  (`TODO-obsolete.md`, `lookup-performance.md`), drops the stale
  `framework_analysis.md`, and the Pages deploy now serves the canonical
  `notes/llms.txt` at `/llms.txt`.
- **Breaking:** drop the `shape="reduce"` and `arrow_reduce=` aliases.
  Write `in_shape="aggregate"` (or let `group_key=` imply it) and
  `arrow_aggregate=True`. The true sequential accumulator remains
  `in_shape="fold"`.
- **Breaking:** `pipeline(home=...)` takes a `Home` instance, not a path
  string (`TypeError` otherwise). `Home` owns the ledger (`Database`),
  object store (`ObjectStore` — local or S3-compatible), and lane tables
  (`LaneStore`) for one root — process-global `_init_home` and the
  one-home-per-process guard are gone. Concurrent runs against different
  homes in one process are supported. Construct with `Home("/path")` (or
  `home=None` for the ambient `RUBEDO_HOME`/`.rubedo` default); same
  absolute path interns to the same instance. `trace`/`gc`/
  `storage_report`/`invalidate` and `create_app(home=)` take `Home` the
  same way. (TODO 34 end-state)

### Fixed
- Mypy's analysis target is now Python 3.12 so numpy≥2.5's PEP 695 stub
  syntax parses under either a 3.11 or 3.12 interpreter. Runtime floor
  stays `requires-python = ">=3.11"`.
- Docs API pages now render like typical Material/mkdocstrings Python
  references: Google-style `Args`/`Returns` become parameter tables
  (`docstring_style: google`), the `step()` shape table is real Markdown
  (and the `tables` extension is enabled), source dumps are off, and the
  custom CSS no longer paints every heading/code block in red (kept the
  brand hairline + link accent only).
- Docs navigation: drop Material `navigation.tabs` (top tabs hid the API
  unless you were already on that section) and put the full tree —
  including a top-level **API Reference** — in the left sidebar, expanded
  by default. Page TOC stays on the right.

## [0.3.0] - 2026-07-18

### Added
- `in_shape="fold"` — a streaming accumulator shape for aggregate-style
  steps: `fn(acc, value)` is called once per parent lane (sorted by
  coordinate, so order never changes results) starting from a deep copy
  of `fold_init` per group. Same plan/address/reuse/ledger semantics as
  `in_shape="aggregate"`; only execution differs. Requires exactly one
  parent and a JSON-serializable `fold_init`.
- `p.join(name=, join_on=)` / `p.union(name=, depends_on=)` — declarative
  steps with no function body: `join` assembles a nested struct from
  matched parents, `union` merges lane sets deduped by content hash. Both
  run with zero per-lane Python calls; caching is automatic.
- Expand and reduce/aggregate steps can now produce/consume `pa.Table`
  (Arrow tables) directly instead of a dict-of-lanes: `arrow_reduce=True`
  (renamed `arrow_aggregate`) hands a reduce/aggregate step a `pa.Table`,
  and an expand step returning a table mints one lane per row without a
  Python dict round trip.
- `check_cache` step field (default `True`) — per-step cache bypass that
  still commits results, the per-step equivalent of `--force`. Root
  (source) steps that must notice new/changed external state on every
  run should set `check_cache=False`; `count_lines`'s scan step is the
  reference example.
- `join`/`group_key` read their fields directly from the parent's output
  struct — `index=` is gone, every output field is searchable without it.

### Changed
- **Storage rewrite**: the `materializations` / `materialization_index` /
  `MaterializationLifecycle` SQLite tables are deleted. Step outputs now
  live in a per-step Arrow IPC lane store (`lane_store.py`) as native
  Arrow types (structs for dicts, int64/string for scalars) with
  automatic spill to the object store for large values; liveness is
  tracked by the existing `input_hash_usages` table plus an
  address-based `MaterializationEdge`. GC, selection, trace, and the
  server all read Arrow instead of the old SQLite tables. See
  `notes/arrow-storage.md`.
- `StepSpec` carries `in_shape`/`out_shape` as its primary fields instead
  of a single `shape`: `map` (one/one), `aggregate` (aggregate/one — the
  step formerly called `reduce`), `expand` (one/many), `join` (join/many).
  `reduce` → `aggregate` throughout, including `arrow_reduce` →
  `arrow_aggregate` (old `shape=` kwarg still accepted, translated
  internally).
- Root expand (source) steps now reuse from cache across runs instead of
  always re-executing — the expand anchor is keyed on a constant root
  lane, so a second run with an unchanged generator emits `reuse` for
  every child lane instead of re-scanning. Sources that need to detect
  new or changed external state (folders, CSV/SQL/S3 scans) must opt in
  with `check_cache=False`; docs (`sources.md`, README) updated to add it
  to every external-state recipe.
- Perf: cached fulfilled-address set (one SQLite query per run instead of
  per step), O(matches) Arrow lookups via a cached address index, an LRU
  cache for on-disk Arrow tables, parent tables kept in memory across
  segments instead of re-read, and independent root expands now run
  concurrently under `schedule="deep"`.

### Fixed
- Expand steps that return a `pa.Table` now record the creating run's id
  on every child row — previously those rows landed with an empty
  `run_id` and the server's "created by run" provenance came back blank
  for table-returned expand lanes.
- Output identity is canonicalized so Arrow's union null-fill
  (heterogeneous dict key sets across lanes) can no longer shift a
  downstream step's `input_hash`.
- Dict outputs with differing key sets across lanes now evolve schema
  correctly (union of fields, nullable for missing) instead of erroring.
- Cache eviction on invalidation: a plan run immediately after an
  invalidate now correctly sees the lane as needing recompute.

## [0.2.6] - 2026-07-15

### Fixed
- Web UI assets now actually ship in the published wheel. The publish
  workflow was running `uv build` without first building the web assets
  (which are gitignored), so every PyPI wheel had an empty
  `web_static/` and `rubedo serve` showed "web UI not built." The
  workflow now runs `npm ci && npm run build` before `uv build`.

## [0.2.5] - 2026-07-15

### Fixed
- Static assets now served with correct MIME types via a single
  `FileResponse` handler instead of a `StaticFiles` mount that didn't
  resolve correctly in installed-package environments (caused "Expected
  a JavaScript-or-Wasm module script but the server responded with
  text/html" errors in the browser).

## [0.2.4] - 2026-07-15

### Fixed
- Web UI assets now build during `pip install` via a `setup.py` hook and
  ship in the wheel. Previously `rubedo serve` showed "web UI not built"
  because `web_static/` was gitignored and never included in the package.
  End users installing from PyPI get the dashboard out of the box — no
  npm required.

## [0.2.3] - 2026-07-15

### Added
- `rubedo serve` — one command starts the read-only FastAPI server with
  the built web UI served at `/` (SPA fallback for client-side routes).
  The web assets are bundled as package-data, so `pip install
  "rubedo[server]"` ships the dashboard. Vite builds to
  `src/rubedo/web_static/` and proxies `/api` to `:8000` in dev.
- `Pipeline.declare()` — writes a `kind="declaration"` Run with the full
  definition snapshot (including step source code) to the ledger without
  executing. The pipeline appears in the dashboard and `rubedo ls` before
  any run.
- Live run progress UI: per-step completion states (waiting/active/done)
  with progress bars and `finished/total` labels, animated topology on
  the Runs page. Live run cards expand/collapse and stay visible after
  completion (dismissible). The Runs page always polls (2s live, 5s idle)
  so new runs appear without a manual refresh.
- Clickable step detail panel in DagView: click any step node to see all
  specs (name, version, shape, depends_on, workers, retries, rate_limit,
  stale_after, executor, group_key, join_on, etc.) plus syntax-highlighted
  source code (open by default). A "View materializations →" link appears
  when a pipelineId is available.
- Click a pipeline name in the runs table to expand its DAG inline.
- `definition()` snapshot now includes a `source` field per step with the
  raw `inspect.getsource()` text.
- Playwright e2e specs (4 tests) spawning a backend with a temp
  `RUBEDO_HOME`, verifying the SPA renders real ledger data. Added to CI.
- `private/demo_live.py` — 7-step DAG with parallel branches and
  `stale_after="3s"` for observing live progress (`--force` and
  `--declare` flags).

### Changed
- SSE stream interval 1.0s → 0.3s for smoother live progress animation.
- `/api/pipelines` now includes `kind="declaration"` runs, not just
  `kind="process"`.
- `web/src/api.ts` uses relative `/api` URL (same-origin in prod, proxied
  by Vite in dev) instead of hardcoded `http://localhost:8000/api`.

### Fixed
- Playwright e2e: use `uv run python` in CI (bare `python` lacked venv
  dependencies like pydantic).

## [0.2.2] - 2026-07-14

### Changed
- `depends_on=` is now inferred for `reduce` and `join` steps too: a
  reduce step's parameter names its parent (like any map step), and a
  join's `join_on` keys ARE the parents. The parent-count validation for
  reduce moved from decoration time to build time so signature inference
  runs first. Explicit `depends_on=` still works and disables inference.
- `@p.step` (bare, no parens) now registers correctly — previously it
  silently did nothing (the decorator was returned uncalled).
- Swept all examples, docs, tests, and marketing to the terse step style:
  bare `@step`/`@p.step` with inference instead of explicit `name=`/
  `version=`/`shape=`/`depends_on=` that restate what the code already
  says.

### Added
- `test_depends_on_dict_alias_on_join` and
  `test_depends_on_dict_alias_on_reduce` — coverage for the `depends_on`
  dict alias form (`{"param": "step"}`) on join and reduce steps.

### Removed
- `docs/llms.txt` — stale duplicate of the canonical `notes/llms.txt`.

## [0.2.1] - 2026-07-13

### Changed
- The Pipeline rotation (TODO 15): one `Pipeline` object with verbs as
  methods (`.run()`/`.plan()`/`.describe()`/`.definition()`); `name` is
  the pipeline's sole identity (no `id=`); `pipeline()` is the sole
  constructor. `@p.step` registers steps on it; `pipeline(steps=[...])`
  takes an explicit list. `.build()` is gone — the spec is built lazily
  on first verb access.
- Step ergonomics (TODO 16): `@step` auto-names from the function name,
  defaults `version` to `"0"`, and works bare (`@step`) or called
  (`@step()`, `@step(version="2")`).
- Ingestion is a step, not a class (TODO 14): no `Source` protocol or
  `sources=` kwarg — a parentless generator `@step` infers `shape="expand"`
  and yields the initial lanes. A source-less `map` root mints a single
  `@root` lane from `params`.
- `describe(format="ascii")` — hand-rolled terminal DAG rendering; TTY
  autodetect picks ascii in a real terminal, text otherwise (TODO 20/24).
- Rewrote `notes/invariants.md` values-first (TODO 17); swept
  invariant-number references from docs/notes.
- Comment cleanup: process-notes out of source, constraints stay
  (TODO 19).
- Marketing landing page: spacing, syntax highlighting, hover tooltips,
  diamond-join rewrite.

### Added
- `pipeline(secrets=/env=)` declarations + `rubedo check` env lint
  (TODO 20/21).
- GitHub Pages workflow for the marketing site + docs.
- `StepSpec` is callable — `s(params)` runs a step in isolation for
  unit tests (TODO 24).

### Fixed
- `pipeline(retention=)` validated eagerly, not lazily.
- Marketing preview 404.

## [0.2.0] - 2026-07-12

### Added
- Retention GC (TODO 10b): `pipeline(retention=N)` auto-prunes a
  pipeline's last N terminal runs; `rubedo gc [--max-bytes] [--delete]`
  is a dry-run-by-default sweeper that demotes (paired `pruned`
  lifecycle rows) then deletes bytes only when no live materialization
  references them. `object_reclamations` table records every swept
  object.
- `schedule="broad"|"deep"` (TODO 9): broad completes each step across
  all lanes before the next; deep lets each lane race ahead through
  consecutive 1:1 map steps. Reduce/join/expand/multi-parent maps
  synchronize either way.
- Lane-level (downstream) invalidation — invalidating a lane
  propagates to its descendants.
- Source-less `map` root: a pipeline can begin with a plain step that
  mints a single `@root` lane from `params` instead of scanning for one.
- `examples/pdf_digest` — source-less map root feeding a vision→text DAG.

### Fixed
- `dist/*.gitignore` no longer leaks into GitHub release assets.

## [0.1.1] - 2026-07-09

### Added
- `trace()` / `rubedo trace` — lane-following lineage queries: seed on any
  selection and walk the recorded derivation edges upstream (what an output
  was derived from) and downstream (everything it contaminated), read-only;
  superseded generations are marked, never hidden.
- `storage_report()` / `rubedo du` — read-only storage observability:
  object-store size and live/reclaimable breakdown per pipeline and step,
  computed from the ledger, with a `--json` output for scripting.

## [0.1.0] - 2026-07-08

Initial public release.

### Added
- DAG pipelines over keyed collections — files in a folder, CSV rows, SQL
  table rows — with content-addressed caching: re-runs recompute only what
  changed (`hash(step, version, input_hash[, params][, code])`).
- Step shapes: `map` (default), `reduce` with optional `group_key`,
  `expand` (1:N lane minting), and N-way `join`; multi-source pipelines
  (`sources={name: Source}`).
- Step policies for flaky, expensive work: `retries`/`retry_on`,
  `rate_limit`, `stale_after` TTLs, data-quality `assertions`, cached
  `Filtered` verdicts, and `skip_cache` inline utils.
- Append-only run ledger with immutability guards, output generations
  (supersede/restore/refresh), lineage edges, and surgical invalidation via
  the `Selection` query language (`step:`, `version:<2.0`, indexed fields).
- Heartbeat-derived run liveness: stored status is terminal-only; readers
  derive `running`/`interrupted` from heartbeat freshness — a killed or
  slept run can never wedge as "running".
- Code-drift handling (`code="warn"|"auto"`), pipeline-level `params_model`
  validation, thread and process (`loky`) executors, terminal progress.
- Read-only ops CLI (`rubedo ls` / `show` / `invalidate`) and a read-only
  web dashboard (FastAPI + React) with live run streaming, lineage, and
  output search.
- MkDocs documentation, marketing site structure, community health files
  (issue/PR templates, CODEOWNERS), and the PyPI publishing workflow.
