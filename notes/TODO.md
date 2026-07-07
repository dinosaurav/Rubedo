# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine contradictions),
with file pointers, gotchas, and acceptance criteria. Read `CLAUDE.md` first
for conventions, and `notes/invariants.md` for vocabulary. One item = one (or
a few) commits.

The **producer model is done** (content-addressed lanes → `expand` →
`group_key` → multi-source → N-way `join`); see the Done changelog and
`notes/producer-model.md`. What's left is grouped into four tiers below, and
items are numbered sequentially (1..12) across tiers so cross-references stay
stable.

## Priority snapshot (recommended order — owner may reshuffle)

- **Tier 1 · Product shape & packaging** — the producer model is a natural
  "feature complete" moment; before a `pip install rubedo` push, keep the
  public surface trustworthy and the install lean: **1** dependency & packaging
  hygiene · **2** read-only ops CLI (build early — a terminal view of ledger
  state is the fastest way to eyeball the output of everything else while
  building it).
- **Tier 2 · DX, Observability & UI** — make it delightful to watch and drive:
  **3** live run view animations (backend + wiring already shipped) · **4**
  pipelines-page enhancements · **5** rich output visualization.
- **Tier 3 · Scale & cloud** — a dependency chain, build when multi-machine
  demand is real: **6** cloud sources → **7** cloud ledger+store → **8**
  distributed execution; **9** lane-pipelined execution (independent).
- **Tier 4 · Deferred / careful** — **10** storage GC (**dangerous**) · **11**
  `expand` child-views (storage optimization) · **12** lane tooling.

══════════════════════════════════════════════════════════════════════
# Tier 1 · Product shape & packaging
══════════════════════════════════════════════════════════════════════

## 1. Dependency & packaging hygiene

Keep `pip install rubedo` lean and honest about what the engine actually needs.

- **Done:** `litellm` moved out of core `dependencies` into the `dev`
  dependency-group — it was never imported under `src/`, only by the
  `graphify` example (alongside its `networkx`/`tree-sitter`, already in `dev`).
  Core install no longer drags in litellm's dependency tree.
- **Remaining:** a smoke-install check (build the wheel, install it into a
  clean venv, `import rubedo` + run a trivial pipeline) so the packaged
  artifact — not just the editable checkout — is known-good. When cloud sources
  land (items 6–7), give them **optional extras** (`rubedo[s3]`, `rubedo[gcs]`,
  `rubedo[postgres]`) rather than core deps, same pattern. `py.typed` already
  ships. Acceptance: a clean-venv wheel install runs `examples/count_lines`
  end-to-end with only core deps present.

## 2. Read-only ops CLI (`rich`)

A terminal window into ledger state — the fastest way to inspect what a run
produced while building the rest of the roadmap (it's a dev tool for verifying
other commands' output as much as a user feature). Add a `rubedo` console entry
point (`[project.scripts]` in `pyproject.toml`). **Scope is deliberately the
read/ops surface only** — the terminal twin of the read-only web dashboard:

- `rubedo ls` — recent runs (id, pipeline, status, created/reused/failed
  counts, timing).
- `rubedo show <run>` — one run's steps, per-step counts, coordinate statuses,
  events; `--json` for scripting.
- `rubedo invalidate <selection>` — surgical invalidation from the terminal
  (the invalidation UI was removed from the dashboard, so the CLI + code are
  now the home for this; see item 12).

**Reuse, don't duplicate — this is the point of doing it now:**
- `invalidate` is already a standalone public API (`invalidation.invalidate`)
  taking a `Selection`; `Selection.parse()` builds one from a query string. The
  CLI command is a thin wrapper over `invalidate(Selection.parse(arg))` — zero
  new query logic.
- `ls`/`show` read the same ledger the server does, but those queries are
  currently **inlined** in `server.py`'s FastAPI handlers (`get_runs`,
  `get_run`, and the summary-JSON unpacking). Factor them into a shared
  read-query layer (plain functions returning dicts/dataclasses, no FastAPI
  types) that **both** `server.py` and the CLI call — so the HTTP API and the
  CLI can never drift. `rich` renders the tables. Reads `RUBEDO_HOME` like
  everything else; imports **zero** user pipeline code.

**Explicitly out of scope: `rubedo run` / `rubedo plan` as first-class
commands.** Pipelines are Python and already have a natural entry point
(`python my_pipeline.py` calling `run(pipe)`); a `module:factory` string would
reintroduce exactly the registry/discovery indirection the engine deliberately
rejects ("no registry; the engine never imports user code"), while being weaker
than the code path it replaces (stringly-typed, no args, no type-checking). If
a `rubedo run` ever appears it is at most **syntactic sugar** — e.g. exec a file
path that itself calls `run()` — never a discovery mechanism, and never the
recommended way to run a pipeline. Design-first if even the sugar is wanted.

══════════════════════════════════════════════════════════════════════
# Tier 2 · DX, Observability & UI
══════════════════════════════════════════════════════════════════════

## 3. Live run view (streaming progress) — animation polish

**Backend and wiring already shipped** (commit "feat(ui): Live run view and
lineage search"): `server.py` exposes `GET /api/runs/{run_id}/stream` as an SSE
endpoint that polls `run_coordinate_statuses` on a 1s interval and pushes
per-step + total created/reused/failed/blocked/filtered counts; `RunDetail.tsx`
consumes it via `EventSource`, updating counts live and closing the stream when
the run leaves `running`. The polling design is correct — SQLite has no
`LISTEN/NOTIFY`, so cross-process (run in one process, server in another) means
tailing the ledger on a short interval.

**What's actually left is the visual layer:** the DAG should *light up* as a
run executes — animate node state transitions (e.g. pulsating running states),
smooth counter increments rather than snapping, per-step created/reused/failed
badges ticking. Make watching a run feel dynamic. **Also fix here:**
`RunDetail.tsx` hardcodes `http://localhost:8000` in the `EventSource` URL —
route it through the same base-URL helper the rest of `api.ts` uses.

## 4. Pipelines Page Enhancements

The pipelines page should act as a richer entry point into a pipeline's state.
- **Last Run Details:** Surface more comprehensive information about the most recent run for each pipeline (status, duration, coordinate counts).
- **Step Drill-Down:** Allow clicking on a specific step from the pipeline page to get deeper information about that step.
- **Direct Materialization View:** Provide a way to view and browse the latest materializations specifically produced by that selected step.

## 5. Rich Output Visualization

Improve how materializations and outputs are displayed across the UI. Go beyond simple metadata and raw JSON/text previews to show more useful information, such as the actual calculated content for a step in a cleaner, more readable format.

══════════════════════════════════════════════════════════════════════
# Tier 3 · Scale & cloud
══════════════════════════════════════════════════════════════════════

## 6. Cloud object storage sources (`S3Source` / `GCSSource`)

Local folders and SQL are great starts, but modern data lives in buckets. Add
`Source`s that scan and pull from S3/GCS (`src/rubedo/sources.py`): `scan()`
lists objects under a prefix → coordinates = keys relative to the prefix;
`load()` downloads the object bytes; `source_id` = `s3://bucket/prefix` (no
credentials — use the ambient boto3 / google-cloud-storage client). **The load-
bearing gotcha:** hashing an object means *downloading* it, so `scan()` must
**not** content-hash eagerly. Use the object's **ETag/size/mtime as the change
token** instead of a true content hash (S3 ETag is the MD5 for single-part
uploads but not for multipart — fall back to size+mtime or a stored checksum
there). This is exactly the producer-model insight that "scan produces a
content hash eagerly" is the *folder* assumption; cloud sources need a change
token that isn't the content hash. Ship boto3/gcs as optional extras
(`rubedo[s3]`, `rubedo[gcs]`; see item 1). Acceptance: scan a bucket prefix →
coordinates; a step reads object bytes; a re-run reuses untouched objects
without re-downloading to hash them.

## 7. Configurable cloud ledger + object store (Postgres / S3-GCS)

Distinct from item 6 (input data) — this is the *internal* materialization
store (`src/rubedo/store.py`) and ledger DB (`src/rubedo/db.py`) that back every
run. `db.py` is already SQLAlchemy-based, so pointing it at a Postgres URL is
comparatively mechanical (the WAL/`busy_timeout` pragma hook is SQLite-specific
and must become conditional). `store.py`'s content-addressed layout
(`hash[:2]/hash[2:4]/hash`) maps directly onto an S3 key prefix, but every
`os.path`/`open()`/`os.replace()` in it assumes a local filesystem and needs an
abstraction swapped in behind the same interface (atomic `replace` becomes a
conditional-put). This — **not** the execution backend — is the real
prerequisite for genuine multi-machine/cloud execution (item 8): a distributed
worker can't write to a purely local SQLite file + local objects dir. The
`RUBEDO_HOME` root (shipped) is a natural stepping stone since it already
isolates where these paths resolve. Acceptance: a run whose `store`/`db` point
at Postgres + a bucket produces identical ledger/reuse behavior to the local
default.

## 8. Pluggable distributed execution backend (Dask / Ray)  **[depends on item 7]**

Today `execution.py` offers `executor="thread"|"process"`, both single-machine.
`_execute_step`'s `call()` already treats "the pool" as anything satisfying
`.submit(fn, *args, **kwargs) -> Future-with-.result()` (the same shape
`dask.distributed.Client` and a thin `ray` wrapper expose), so a third
`executor="dask"`/`"ray"` value is a small change to *that call site*. The real
cost is architectural, not mechanical: it needs a running scheduler/cluster —
which cuts against the "zero-daemon" positioning (`notes/framework_analysis.md`)
— and it **depends on item 7** (a distributed worker can't reach a local
SQLite + objects dir). **Owner design session before building:** add it as a
third `executor=` value alongside `"process"`, or *replace* `"process"` (a
Dask/Ray `LocalCluster` subsumes the local-multi-process case; `loky` already
solved the picklability pain far more cheaply). Acceptance: an
`executor="dask"` step runs on a `LocalCluster` and reuses across runs via the
cloud store (item 7).

## 9. Non-topological (lane-pipelined) execution

Today the runner is **staged**: `for step in topo: plan → execute all lanes →
commit` (`src/rubedo/runner.py`), so every step waits for *all* lanes of the
previous step before any lane advances — max parallelism within a step, zero
pipelining across steps. A lane can't race ahead through `scrape → parse →
classify` while a sibling is still scraping. Goal: let a lane flow through
consecutive **per-lane** steps (`map`/`filter`/`expand`) as far as it can,
independent of its siblings. Wins: first results land far sooner (latency),
long-running/streaming pipelines make continuous progress, workers don't stall
at stage boundaries. **Not** for the collective steps — `reduce`/`join` are
true barriers (they need the whole input set), so pipelining applies only to
the per-lane runs *between* barriers (the owner flagged this is about the
non-join-heavy cases). Cost is a real execution-model rework: the interleaved
plan→execute and `coord_step_mats` both assume whole-step staging; a
lane-pipelined engine needs a scheduler over `(lane, step)` tasks with
dependency edges that stops a lane at the next barrier and synchronizes there.
Interacts with the `expand` cache anchor (per-parent) and reduce/join barriers.
Design-first.

══════════════════════════════════════════════════════════════════════
# Tier 4 · Deferred / careful
══════════════════════════════════════════════════════════════════════

## 10. Storage sprawl management + garbage collection  **[⚠️ DANGEROUS]**

Content-addressed stores keep everything; without cleanup the `.rubedo`
directory balloons. Useful *safe* features first: disk-usage warnings, storage
limits, age-out policies. Actual byte-deleting GC is genuinely hazardous and
must not be built casually — the orphan-retention decision
(`producer-model.md` Q2) is *keep orphans* for good reasons, and any GC that
deletes bytes fights that and can corrupt live state. **Four traps:**
**(1) Shared objects** — the store dedupes identical bytes
(`hash[:2]/hash[2:4]/hash`), so one physical object can back *many*
materializations across different addresses; "this materialization is orphaned"
does **not** mean "its bytes are unreferenced." A sweep MUST ref-count physical
objects against *all* live materializations before deleting a byte, or it
silently guts live outputs (violates invariants 1 & 3). **(2) Direction of
truth** — sweep by walking the ledger and ref-counting; **never** the store;
never delete ledger rows (append-only). **(3) Concurrency** — a commit on
another machine can *restore* (re-reference) bytes a sweep is mid-delete on.
**(4) Cloud irreversibility** — S3/GCS deletes are permanent (no trash); a
buggy pass against a bucket is catastrophic. Gate any real GC behind dry-run +
a ref-count audit + object-versioned buckets before it *ever* points at remote
storage.

## 11. `expand` child views (dedup storage) — post-launch optimization

Today `shape="expand"` uses option (a) from `notes/producer-model.md` — the
step stores its full yielded list as a cache anchor *and* extracts each item
into its own child materialization, so scraped data is stored twice. Option
(b): make each child lane a lightweight **view** into the anchor
(`(anchor-address, subkey)` + the item's content hash) instead of a separate
materialization, so downstream resolves the item out of the anchor and nothing
is duplicated. Wins most for large scraped payloads. Needs a new view-ref type
in `coord_step_mats` + resolution in `_resolve_parent_value` + edge/`input_hash`
handling; downstream per-item caching stays keyed on the item's content hash.
Correctness is identical to (a) — purely a storage optimization, so only worth
it once double-storage actually bites.

## 12. Lane tooling — following & invalidation

Two utilities that ride on machinery that already exists (`MaterializationEdge`
lineage, `MaterializationIndexEntry` labels); now that lanes can go
content-addressed/minted, they're the load-bearing navigation surface.

- **Lane-following (lineage queries).** "Find the results connected to a label
  at a certain step": index-lookup (`MaterializationIndexEntry`) to seed
  materializations carrying the label, then BFS up/down `MaterializationEdge`
  to reach connected outputs at other steps. Pure query over existing tables —
  a recursive CTE, no new bookkeeping. Survives reduce/expand/join because it
  is a materialization graph, not coordinate-equality. This is the "follow the
  path of a lane" utility that replaces a legible coordinate once lanes are
  opaque. Root-of-lineage → source row is answered by indexing source metadata
  at the root (decide: always index it).
- **Lane-level invalidation.** Today `invalidate(selection)` flips `is_live` on
  the selected materializations only, and the settled core semantics are
  lazy-via-recompute (invalidate a specific bad case, let the next run
  recompute — no eager descendant cascade; `producer-model.md` Q1). The
  deferred tooling is broader selection-driven invalidation over a *lane* (e.g.
  "invalidate everything this label touched, all steps"), built on the same
  lineage traversal above. Note: since the invalidation UI was removed from the web dashboard, this invalidation tooling must be robust for CLI and code-first use cases. Design-first; the core stays minimal.

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above)

Dependency hygiene: `litellm` moved from core `dependencies` to the `dev`
group (only the `graphify` example used it; core install no longer pulls it) ·
Pipeline Run Search & Step Inspection UI (RunInspector, deep value search) ·
Live run view backend + wiring (SSE `GET /api/runs/{id}/stream` + `RunDetail`
`EventSource`; animation polish still open, item 3) ·
`PipelineBuilder` helper · data quality assertions (`assertions=[]`) ·
Source protocol (Folder/Csv, lane-key semantics, duplicate handling) ·
type checking pass (mypy configured, py.typed shipped, public API typed) ·
content-addressed store + generations (supersede/restore/refresh) ·
append-only lifecycle ledger with ORM immutability guards · params/code in
cache identity (`code="auto"|"warn"` drift warnings) · single
`run()`/`plan()` entry points, no registry, definition snapshots on runs ·
plan/execute/ledger module split · step policies: retries, rate_limit,
stale_after, skip_cache (fusion) · filters (`Filtered` verdicts, cached) ·
`@step(index=[...])` + `Selection(index=...)` + selection language
(`Selection.parse`, `{"query": ...}` API, UI query box) · DAG rendering
(describe/Mermaid + DagView on Pipelines/RunDetail with per-step counts) ·
trim pass removing v1 residue (`config=`/`config_hash`,
`Selection.coordinates`/`output_content_hash`, `Manifest.manifest_hash`,
manifest size/mtime columns, `previous_output_address`/
`previous_materialization_id`, `SelectionPreviewItem.coordinate`/
`coordinate_count`) and the metadata-filter query path (storage/display
kept, only filtering removed) · Dashboard page and redundant examples
removed · fan-in/reduce steps (`shape="reduce"`, full N→1 fan-in,
`tests/test_reduce.py`) · `TableSource` (SQL rows as lanes,
credential-free `source_id`, `tests/test_table_source.py`) ·
cross-process concurrency safety (SQLite WAL + busy_timeout,
IntegrityError retry-once on commit collision,
`tests/test_concurrency_safety.py`) · pairing-rule guard mechanically
enforcing invariant 8 (`before_commit` session listener,
`tests/test_pairing_guard.py`) · semantic version ordering + range
selection (`version:<2.0` etc. via `packaging.SpecifierSet`, version-aware
sort in DataTable.tsx) · UI polish cluster (API error states via
`fetchJson`, filtered lanes shown in Current Outputs, reduce badge in
DagView) · examples + positioning (`hn_digest` — real HN + LLM
filter→classify→reduce, the flagship non-idempotent-LLM demo;
`github_health`/`weather_advisory` — chained retried/rate-limited APIs
with `stale_after`; `gutenberg_stats` — `skip_cache` util +
`executor="process"`; `orders_rollup` — `TableSource` streaming
`batch_size`; `notes/llms.txt` LLM-authoring guide; README pitch
paragraph) · project rename (Batchit/batchbrain -> Rubedo) · configurable
`RUBEDO_HOME` root (env var, resolved by both `db.py` and `store.py`;
explicit `home=` param on `run()`/`plan()` takes precedence over env vars,
same precedence `db.py`'s `db_path` param already had; `RUBEDO_DB_PATH`
still wins over `RUBEDO_HOME` for the DB specifically when no explicit
param is given; `server.py` needed no code changes — it already picks up
the same env var transitively) · codebase typing pass (`_RunMemo._values`
typed as `Dict[Tuple[str, str], Tuple[Literal["ok", "err"], Any]]`;
`store.py`'s `read_materialization_output` param was fully unannotated,
now a `HasOutputContentHash` Protocol satisfied structurally by both
`Materialization` and `MatRef`; `ObjectMetadataOut`/
`MaterializationIndexEntryOut` schemas replace `get_object_metadata`'s
untyped dict return, `download_object` got an explicit `-> FileResponse`
return type; `_serialize`/`stage_and_commit`'s `result: Any` stayed `Any`
deliberately — step return values are genuinely heterogeneous, no
narrower type is honest there) · CPU-bound parallelism migrated to `loky` +
`cloudpickle` (`executor="process"`), allowing closures in process-executed
steps (`tests/test_process_executor.py` updated to verify local functions) ·
**producer model** (`notes/producer-model.md` — the owner design session and
build): content-addressed lanes (`key=` optional, `_disambiguate` gone,
`tests/test_sources.py`) · `expand` (`shape="expand"`, 1:N coordinate-minting,
cached via a parent-addressed list-anchor so a scrape runs once,
`tests/test_expand.py`) · `group_key` reduce (partition by an indexed field;
reduce now folds in minted lanes, `tests/test_group_key.py`) · multi-source
pipelines (`sources={name: Source}`, root `@step(source=...)`, per-step
`step_sources` threaded through execution, `tests/test_multisource.py`) ·
N-way `join` (`shape="join"`, equijoin on indexed fields, `left|right` pair
coordinates, 4-way star supported, `tests/test_join.py`) — every shape is now
a producer · Runner rework resolved by going vertical (no big-bang
Source→Producer refactor; `expand`/`join` reuse the interleaved plan→execute
runner) · resolved-won't-do: arbitrary-rules plugin surface
(wrapper-or-built-in rule); plan()-in-UI (server never imports user code — use
plan() in Python); per-producer census (removal is a low-value report, minted
lanes orphan silently); behavior-preserving Source→Producer refactor (went
vertical instead).
