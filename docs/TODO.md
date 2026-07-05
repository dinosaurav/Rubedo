# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine
contradictions), with file pointers, gotchas, and acceptance criteria.
Read `CLAUDE.md` first for conventions, and `docs/invariants.md` for
vocabulary. One item = one (or a few) commits.

──────────────────────────────────────────────────────────────────────



## 1. Joins  **[now designed in `docs/producer-model.md` — build per that doc's sequencing]**

The owner design session happened: joins are no longer a standalone item but
the last step of the **producer model** (`docs/producer-model.md`), where a
join is the binary, collective member of the `expand` family — it matches two
independent roots on a declared equijoin key (hashed) and mints `left|right`
pair coordinates. Pair-explosion control is resolved (equijoin key, not
arbitrary predicate; predicate is a post-join `filter`). Do not build in
isolation — it lands after content-addressed lanes → producer refactor →
`expand` → `group_key`, per the doc's sequencing.

──────────────────────────────────────────────────────────────────────

## 2. Future Product Directions (Recommended Next Steps)

These are strategic feature recommendations to expand the engine's capabilities for real-world, large-scale workflows:

- **Cloud Object Storage Sources (`S3Source` / `GCSSource`)**: Local folders and SQL are great starts, but modern data engineering lives in cloud buckets. Adding native sources for scanning and pulling from S3 or GCS is critical for adoption.
- **Configurable cloud ledger + object store (Postgres / S3-GCS)**: distinct from the Source item above, which is about *input* data — this is about the *internal* materialization store (`store.py`) and ledger DB (`db.py`) that back every run. `db.py` is already SQLAlchemy-based, so pointing it at a Postgres URL is comparatively mechanical (the WAL/busy_timeout pragma hook is SQLite-specific and would need to become conditional); `store.py`'s content-addressed layout (`hash[:2]/hash[2:4]/hash`) maps directly onto an S3 key prefix, but every `os.path`/`open()`/`os.replace()` call in it assumes a local filesystem and would need an abstraction swapped in behind the same interface. This — not the execution backend — is the real prerequisite for genuine multi-machine/cloud execution (see the executor="process" and Dask discussion in item 0's history); the configurable `RUBEDO_HOME` root (now shipped — see Done below) is a natural stepping stone since it already isolates where these paths get resolved.
- **Pluggable distributed execution backend (Dask / Ray / cloud)**: today `execution.py` only offers `executor="thread"|"process"`, both single-machine. `execution._execute_step`'s `call()` already treats "the pool" as anything satisfying `.submit(fn, *args, **kwargs) -> Future-with-.result()` (see `pool.submit(step.fn, *args, **kwargs).result()`), which is the same shape `dask.distributed.Client` and `ray` (via a thin wrapper) expose — so a third `executor="dask"`/`executor="ray"` value is a comparatively small change to *this specific call site*. The real cost is architectural, not mechanical: it requires a running scheduler/cluster, which cuts directly against this project's "zero-daemon" positioning (`docs/framework_analysis.md`), and it depends on the cloud ledger/object-store item above (a distributed worker can't write to a purely local SQLite file + local objects dir). Needs an owner design session before building: whether to add this as a third `executor=` value alongside `"process"`, or have it *replace* `"process"` outright (a Dask/Ray `LocalCluster` subsumes the same local-multi-process case — see item 0's loky/cloudpickle note for a lower-cost alternative that solves the picklability pain without any of this).
- **Incremental Source Scanning (High Watermarks)**: Currently, sources scan their entire domain on every run (relying on cache identity to skip work). For massive tables or buckets, a source should support an `updated_at > last_run` watermark to skip scanning untouched coordinates entirely, drastically speeding up the planning phase.
- **Dynamic Lane Expansion (`flat_map` shape)** — *now designed in `docs/producer-model.md` as the `expand` producer (1:N coordinate-minting); build per that doc, not this bullet.* Currently we have `map` (1:1) and `reduce` (N:1). Adding an `expand` shape (1:N) lets a step `yield` multiple outputs from a single lane (e.g., fetching an RSS feed and yielding an output lane for each article).
- **Robust CLI & Terminal UI**: The Web UI is excellent, but local-first developers love the terminal. A `rubedo` CLI with rich terminal output (using a library like `rich`) to show live DAG execution, progress bars for lanes, and interactive plan confirmations would greatly enhance the core DX.
- **Data Quality Assertions**: Similar to dbt tests, allowing users to define lightweight assertions or schemas on step outputs to automatically fail/block lanes if the data is malformed (e.g., an LLM returns invalid JSON that parses but misses required fields).
- **Storage Sprawl Management**: Include useful features to prevent storage sprawl, such as disk usage warnings, storage limits, automated policies to reduce/expire old data, and mark-and-sweep garbage collection to clean up unlinked or orphaned data.
  - **⚠️ DANGER — GC is genuinely hazardous; do not build casually.** The
    orphan-retention decision (`producer-model.md` open Q2) is *keep orphans*
    for good reasons; any GC that deletes bytes fights that and can corrupt
    live state. Four traps: **(1) Shared objects.** The store dedupes identical
    bytes (`hash[:2]/hash[2:4]/hash`), so one physical object can back *many*
    materializations across different addresses — "this materialization is
    orphaned" does **not** mean "its bytes are unreferenced." A sweep MUST
    ref-count physical objects against *all* live materializations before
    deleting a single byte, or it silently guts live outputs (violates
    invariants 1 & 3). **(2) Direction of truth.** Sweep by walking the ledger
    (truth) and ref-counting; **never** by walking the store (derived); and
    never delete ledger rows (append-only). **(3) Concurrency.** A commit on
    another machine can *restore* (re-reference) bytes a sweep is mid-way
    through deleting — GC racing the restore path deletes live data. **(4)
    Cloud irreversibility.** On S3/GCS deletes are permanent and unrecoverable
    (no filesystem trash) — a buggy pass against a random cloud bucket is
    catastrophic and unrollbackable. Gate behind dry-run + a ref-count audit +
    object-versioned buckets before it is *ever* pointed at remote storage.
- **Source API Simplification**: Remake how `Source` works so that average consumers don't feel compelled to write a full class that implements the `Source` protocol. A simpler functional or generator-based API (e.g., a `@source` decorator) would significantly reduce boilerplate for custom data sources.
- **`expand` child views (dedup storage) — post-launch**: today `shape="expand"` uses option (a) from `docs/producer-model.md` — the step stores its full yielded list as a cache anchor *and* extracts each item into its own child materialization, so scraped data is stored twice. Option (b): make each child lane a lightweight **view** into the anchor (`(anchor-address, subkey)` + the item's content hash) instead of a separate materialization, so downstream resolves the item out of the anchor and nothing is duplicated. Wins most for large scraped payloads. Needs a new view-ref type in `coord_step_mats` + resolution in `_resolve_parent_value` + edge/`input_hash` handling; downstream per-item caching stays keyed on the item's content hash. Correctness is identical to (a) — purely a storage optimization.

──────────────────────────────────────────────────────────────────────

## 3. Runner rework for the producer model  **[part of `docs/producer-model.md`]**

The orchestration layer (`runner.py`: `run()` / `plan()` / `run_pipeline()`)
hardcodes the source-privileged flow today: scan source → `_snapshot_source`
manifest → plan each step in topo order → execute → commit. Under the producer
model it must drive every producer uniformly — the source is just the nullary
root — threading the emitted-lane namespace from one producer to the next,
minting coordinates for `expand`/`join`, honoring `group_key` on `reduce`, and
running a **per-producer census** instead of one source snapshot. This is where
the `spec` / `planning` / `ledger` changes converge, so sequence it after those
per the doc. Non-negotiable: the map-a-folder common path stays a one-liner —
the general core must not leak into the simple case.

──────────────────────────────────────────────────────────────────────

## 4. Continue / resume an interrupted run

Crash-safety is already implicit: a died run's committed materializations
persist and the *next* run reuses them by address (invariant 3). What's missing
is an explicit affordance to **continue a specific interrupted run** — re-plan
only its pending/failed/blocked lanes and keep the run identity for reporting —
so a long, expensive LLM run that dies at lane 900/1000 resumes without reading
as a conceptually fresh run. Largely a UX/reporting layer over the existing
content-addressed reconcile, so keep it thin. Open: same `run_id` vs a new run
linked to the old; re-scan vs reuse the prior manifest; interaction with
selection-scoped/partial runs. Design-first.

──────────────────────────────────────────────────────────────────────

## 5. Lane tooling — following & invalidation  **[deferred; post-`docs/producer-model.md`]**

Two families of utilities that ride on machinery that already exists
(`MaterializationEdge` lineage, `MaterializationIndexEntry` labels) — deferred
until the producer-model refactor lands, since lanes become the load-bearing
navigation surface then. Not to be built yet; captured so the direction isn't
lost.

- **Lane-following (lineage queries).** "Find the results connected to a label
  at a certain step": index-lookup (`MaterializationIndexEntry`) to seed
  materializations carrying the label, then BFS up/down `MaterializationEdge`
  to reach connected outputs at other steps. Pure query over existing tables —
  a recursive CTE, no new bookkeeping. Survives reduce/expand/join because it
  is a materialization graph, not coordinate-equality. This is the "follow the
  path of a lane" utility that replaces a legible coordinate once lanes go
  opaque/content-addressed. Root-of-lineage → source row is answered by
  indexing source metadata at the root (decide: always index it).
- **Lane-level invalidation.** Today `invalidate(selection)` flips `is_live`
  on the selected materializations only, and the settled core semantics are
  lazy-via-recompute (invalidate a specific bad case, let the next run
  recompute — no eager descendant cascade; see `producer-model.md` open
  question 1). The deferred tooling is broader selection-driven invalidation
  over a *lane* (e.g. "invalidate everything this label touched, all steps"),
  built on the same lineage traversal above. Design-first; the core stays
  minimal.

──────────────────────────────────────────────────────────────────────

## 6. Live run view (streaming progress)

A run already writes `run_events` and `run_coordinate_statuses` as it
progresses, and `server.py` is read-only and ledger-derived. Add a streaming
endpoint so the web UI can watch a run execute live — the DAG lighting up,
per-step created/reused/failed/blocked counts ticking as lanes complete.
Mechanism: prefer **SSE** (Server-Sent Events) over raw WebSockets — it is
one-way (all that "view a run as it goes" needs), plain HTTP, and pulls in no
new deps; the server tails new ledger rows for a `run_id` and pushes them. The
tailing constraint to design around: SQLite has no `LISTEN/NOTIFY`, so
cross-process (run in one process, server in another) means polling the ledger
on a short interval; an in-process event bus is only possible if run and server
share a process. Stays consistent with "server never imports user code" — the
feed is purely ledger-derived. Pairs naturally with the `rich` CLI/TUI
live-DAG item in section 2.

──────────────────────────────────────────────────────────────────────

## 7. Non-topological (lane-pipelined) execution

Today the runner is **staged**: `for step in topo: plan → execute all lanes →
commit` (`runner.py:294`), so every step waits for *all* lanes of the previous
step before any lane advances — max parallelism within a step, zero pipelining
across steps. A lane can't race ahead through `scrape → parse → classify` while
a sibling is still scraping.

Goal: let a lane flow through consecutive **per-lane** steps (map/filter/expand)
as far as it can, independently of its siblings, instead of stopping at every
stage boundary. Wins: first results land far sooner (latency), long-running /
streaming pipelines make continuous progress, and workers don't stall at stage
boundaries. Explicitly **not** for the collective steps — `reduce`/`join` are
true barriers (they need the whole input set), so pipelining only applies to the
map/expand/filter runs *between* barriers; the owner already flagged this is
about the non-join-heavy cases.

Cost is a real execution-model rework: the current interleaved plan→execute and
`coord_step_mats` both assume whole-step staging. A lane-pipelined engine needs
a scheduler over `(lane, step)` tasks with dependency edges, that stops a lane's
advance at the next barrier and synchronizes there. Interacts with the expand
cache anchor (per-parent) and reduce/join barriers. Design-first; likely lands
after the producer-model line (`docs/producer-model.md`) settles, since barriers
(reduce/join) define exactly where pipelining must stop.

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above)

Source protocol (Folder/Csv, lane-key semantics, duplicate handling) ·
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
`batch_size`; `docs/llms.txt` LLM-authoring guide; README pitch
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
resolved-won't-do: arbitrary-rules plugin surface (wrapper-or-built-in
rule); plan()-in-UI (server never imports user code — use plan() in
Python).
