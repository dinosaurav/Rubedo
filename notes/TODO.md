# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine contradictions),
with file pointers, gotchas, and acceptance criteria. Read `CLAUDE.md` first
for conventions, and `notes/invariants.md` for vocabulary. One item = one (or
a few) commits.

The **producer model is done** (content-addressed lanes → `expand` →
`group_key` → multi-source → N-way `join`); see the Done changelog and
`notes/producer-model.md`. **Tier 0 and Tier 1 are also done** — the only open
work is Tier 3/4, all design-first. Items keep their original sequential
numbers (1..12) across tiers so cross-references stay stable, so open items
below start at 6.

## Priority snapshot (recommended order — owner may reshuffle)

Everything still open is **design-first**, and most of it is gated on real
demand — but two half-items serve today's single-machine user and are worth
building ahead of any demand signal:

- **Ready ahead of demand** — **12** lane-following (the lineage-BFS half
  only; best value-per-risk on the list, pure read-only queries) · **10a**
  storage observability (`rubedo du` + ref-count audit as a dry-run report).
- **Tier 3 · Scale & cloud** — a dependency chain, build when multi-machine
  demand is real: **6** cloud sources → **7** cloud ledger+store → **8**
  distributed execution; **9** lane-pipelined execution (independent — and
  really a single-machine *latency* feature, so it's the likeliest to earn
  organic demand first).
- **Tier 4 · Deferred / careful** — **10b** byte-deleting GC (**dangerous** —
  four traps; build on 10a only) · **11** `expand` child-views (storage
  optimization) · **12** lane-level invalidation (the second half of lane
  tooling).

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
Design-first. **Candidate for the design session (not a settled decision):**
since barriers are only `reduce`/`join`, consider *fusing chains of
consecutive per-lane steps into one per-lane task* instead of a general
`(lane, step)` scheduler — most of the latency win for a fraction of the
rework, and it may sidestep the `coord_step_mats` staging assumption
entirely. Note also that although this item is filed under scale, the payoff
is single-machine latency (LLM pipelines stalling at stage boundaries), so
expect demand to arrive from ordinary local users, not cluster users.

══════════════════════════════════════════════════════════════════════
# Tier 4 · Deferred / careful
══════════════════════════════════════════════════════════════════════

## 10a. Storage observability (the safe half — promoted, demand-independent)

Content-addressed stores keep everything; without visibility the `.rubedo`
directory balloons silently, and "why is `.rubedo` 2 GB?" is the first
question every real user asks. Ship the *read-only* half first: a
`rubedo du` CLI report — total store size, a per-pipeline/per-step
breakdown, and a **ref-count audit as a dry-run report** ("N objects /
M bytes would be reclaimable"), computed by walking the ledger (never the
store) and ref-counting physical objects against *all* live
materializations. Deliberately no deletes and no enforcement: this
answers the user question today *and* exercises the exact ref-count logic
10b would depend on, in production, long before any delete exists. Rides
the ops-CLI machinery (item 2). Acceptance: `rubedo du` on a populated
store reports sizes + reclaimable estimate, and the audit agrees with a
hand-count on a small fixture.

## 10b. Byte-deleting garbage collection  **[⚠️ DANGEROUS — build on 10a only]**

Storage limits and age-out policies imply enforcement, and enforcement means
deleting bytes. Actual byte-deleting GC is genuinely hazardous and
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
content-addressed/minted, they're the load-bearing navigation surface. The
two halves are **separably shippable**: lane-following is promoted (ready
ahead of demand — read-only, no invariants at risk, and it's the debugging
story that sells the ledger: "this output is wrong, show me everything it
touched"); lane-level invalidation stays deferred.

- **Lane-following (lineage queries) — promoted.** "Find the results connected to a label
  at a certain step": index-lookup (`MaterializationIndexEntry`) to seed
  materializations carrying the label, then BFS up/down `MaterializationEdge`
  to reach connected outputs at other steps. Pure query over existing tables —
  a recursive CTE, no new bookkeeping. Survives reduce/expand/join because it
  is a materialization graph, not coordinate-equality. This is the "follow the
  path of a lane" utility that replaces a legible coordinate once lanes are
  opaque. Root-of-lineage → source row is answered by indexing source metadata
  at the root (decide: always index it).
- **Lane-level invalidation — deferred.** Today `invalidate(selection)` flips `is_live` on
  the selected materializations only, and the settled core semantics are
  lazy-via-recompute (invalidate a specific bad case, let the next run
  recompute — no eager descendant cascade; `producer-model.md` Q1). The
  deferred tooling is broader selection-driven invalidation over a *lane* (e.g.
  "invalidate everything this label touched, all steps"), built on the same
  lineage traversal above. Note: since the invalidation UI was removed from the web dashboard, this invalidation tooling must be robust for CLI and code-first use cases. Design-first; the core stays minimal.

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above)

**Tier 0 — Open Bugs & Hardening (H4–H7):** H4 `stream_run` no longer blocks
the event loop (SSE is a sync generator Starlette threads) · H5 CORS pinned to
the Vite dev origins with `allow_credentials=False` · H6 packaging leanness
(`fastapi`/`uvicorn` moved to a `rubedo[server]` extra; setuptools find
directive replaces the hardcoded package list) · H7 DRY/N+1 leftovers
(`_ensure_gitignore` deduped into `util.py`; `get_pipelines_api` uses one
grouped query). **Tier 1 — item 1 (packaging hygiene):** `litellm` out of core
deps; `scripts/smoke_test.sh` builds the wheel, installs into a clean venv, and
runs `examples/count_lines` end-to-end with only core deps. **Tier 1 — item 2
(read-only ops CLI):** `rubedo` console entry point (`ls`/`show`/`invalidate`,
`--json`, `--failed`) over a shared read-query layer (`queries.py`) both the CLI
and `server.py` call so they can't drift; `pipeline:` selection term (+ B4 fix
in the same selection query); failure introspection (`get_run_failures`
read-query + `RunSummary.failures()` accessor).

**2026-07-08 — heartbeat-derived run liveness:** stored `Run.status` is now
terminal-only (`completed`/`completed_with_failures`/`failed`; NULL while in
flight) — "running" is never stored, because a durable row can't truthfully
make a present-tense claim (a killed process left it lying forever, animating
the live view and holding its SSE stream open). A daemon thread bumps
`Run.last_heartbeat_at` every 60s (timer, not bump-on-commit: one slow LLM
call can go minutes without a ledger write) and readers derive
`running`/`interrupted` via `effective_run_status()` (applied in `queries.py`
for CLI + API and in the SSE stop condition). No reaper, no reconcile:
sleep/wake self-heals — a resumed process starts beating again and the run
flips back to "running" on its own. `last_heartbeat_at` is a Run projection
column but an *ephemeral presence signal* exempt from event pairing
(invariants.md updated; `tests/test_run_liveness.py`). Same restructure fixed
`run(progress=True)`'s `TerminalProgress` scoping (it exited before execution
began) · `count_lines` example fixed for pipeline-level `params_model`
(steps receive the validated dict, not a model instance — it had been failing
every lane on a fresh store since 829dc3e).

Bugfixes from 2026-07-07 code review (B1-B7, H1-H3): fixed multi-parent map crash, invalidation partial commits on failure, duplicate IDs in selection query, skip_cache crash on join/reduce, hash bytes in expand, batch ledger planning (H2), remove mypy ignore overrides (H3), per-key locking for `_RunMemo` skip_cache utils (H1) · UI enhancements (live run view animations, pipelines page drill-down and last-run details, rich JSON viewer for materialization payloads) · Terminal progress feedback (`run(progress=True)`) · pipeline-level `params_model` validation · partial fan-in policy (`on_failed="use_passed"|"block"`) · Dependency hygiene: `litellm` moved from core `dependencies` to the `dev`
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
