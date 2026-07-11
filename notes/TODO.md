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

- **Ready ahead of demand** — both halves shipped 2026-07-09: **10a** storage
  observability as `storage_report()` / `rubedo du`, and **12**'s
  lane-following half as `trace()` / `rubedo trace` — see Done. Nothing else
  is worth building ahead of a demand signal.
- **Tier 3 · Scale & cloud** — a dependency chain, build when multi-machine
  demand is real: **6** cloud sources (design settled 2026-07-10, spec is
  buildable) → **7** cloud ledger+store (design settled 2026-07-11, spec is
  buildable; **7b** PG test coverage follows it) → **8** distributed
  execution. (**9** lane-pipelined execution shipped 2026-07-10 as
  `schedule="deep"` — v1; see Done.)
- **Tier 4 · Deferred / careful** — **10b** retention GC (**dangerous** —
  five traps; design session settled 2026-07-10, spec below is buildable).
  (**11** `expand` child-views turned out already resolved — the `2850e74`
  hash-list anchor ended the double storage; see the item. **12** lane-level
  invalidation shipped 2026-07-09 — item 12 is now fully done; see the Done
  changelog.)

══════════════════════════════════════════════════════════════════════
# Tier 3 · Scale & cloud
══════════════════════════════════════════════════════════════════════

## 6. Cloud object storage sources (`S3Source` / `GCSSource`)  **[design settled 2026-07-10]**

Local folders and SQL are great starts, but modern data lives in buckets. Add
`Source`s that scan and pull from S3/GCS (`src/rubedo/sources.py`). **The
load-bearing gotcha:** hashing an object means *downloading* it, so `scan()`
must **not** content-hash eagerly — it makes LIST calls only, zero
GetObject. This is exactly the producer-model insight that "scan produces a
content hash eagerly" is the *folder* assumption; cloud sources use a change
token that isn't the content hash. Note the containment property that makes
a cheap token safe: `SourceItem.content_hash` feeds only the *root step's*
`input_hash` (`planning.py:723`); consumers key on the root's output bytes,
so a token that churns without a real content change costs one re-download +
root re-run per object and nothing downstream.

**Settled decisions (owner design session 2026-07-10 — do not re-litigate):**

- **Change token = `hash(etag, size)`, always** — one token shape, no
  multipart sniffing, **never mtime**. ETag is a stable, content-derived
  change token for single-part *and* multipart uploads (it changes when
  content changes; identical re-uploads keep it — always for single-part,
  for multipart when the part size matches); mtime is strictly worse (bumps
  on identical re-upload). GCS: `hash(md5Hash or crc32c, size)` — GCS always
  supplies a real hash. The "fall back to size+mtime for multipart" idea
  from the original spec is dead.
- **`load()` returns the object bytes.** No local download cache (which
  would invent a directory whose lifecycle 10b would have to learn). This
  knowingly diverges from `FolderSource`'s hand-a-path idiom; a
  streaming/large-object story waits for demand. No `mode=` knob.
- **Client hook = `client_factory=`**, an optional zero-arg callable
  returning a client; default factory = the ambient session
  (`boto3.client("s3")` / `storage.Client()`). A factory (closure)
  cloudpickles cleanly under `executor="process"`; the live client is
  created lazily per process and dropped from pickling (`__getstate__`).
  **Never a live `client=` kwarg** — boto3 clients don't pickle. The
  factory also covers MinIO/localstack endpoints and test fakes.
- **Sequencing: two commits.** `S3Source` first (moto-tested, proves the
  token/payload/factory shape), `GCSSource` second with the identical
  shape (no moto equivalent — tests inject a fake via `client_factory`).

**Mechanics:** `S3Source(bucket, prefix="", client_factory=None)`.
`id` = `s3://bucket/prefix` — credential-free **and endpoint-free** (an
injected MinIO endpoint must not leak into `source_id`; same rule as
`TableSource`). `scan()` = paginated ListObjectsV2 → coordinate = key
relative to the prefix (forward slashes, mirroring `FolderSource`),
`content_hash` = the token above, `ref` = the full key, `metadata` =
size/mtime for display. `load()` = GetObject → bytes. Ship boto3/gcs as
optional extras (`rubedo[s3]`, `rubedo[gcs]`); moto goes in the dev group;
core install stays boto3-free (`scripts/smoke_test.sh` must stay green).

**Trap (part of the spec):** (1) scan must stay LIST-only — any per-object
GetObject/HEAD at scan time reintroduces the download-to-hash cost the
token exists to avoid; (2) S3 returns `ETag` wrapped in literal double
quotes — strip before hashing or the token silently differs between code
paths; (3) paginate ListObjectsV2 (>1000 keys) from day one; (4) verify
`executor="process"` end-to-end — the Source travels to the loky worker,
and a lazily-created client must survive the trip.

Acceptance: scan a moto bucket prefix → coordinates; a root step receives
the object bytes; a re-run reuses untouched objects with **zero GetObject
calls** (assert by counting calls on the fake/moto client); an object
overwritten with identical bytes (single-part) stays reused; a changed
object recomputes exactly its lane; the same pipeline runs under
`executor="process"` with a `client_factory`; tests follow the standard
fixture shape (`tests/test_index.py`); `rubedo[s3]` extra installs boto3,
core install does not.

## 7. Configurable cloud ledger + object store (Postgres / S3-GCS)  **[design settled 2026-07-11]**

Distinct from item 6 (input data) — this is the *internal* materialization
store (`src/rubedo/store.py`) and ledger DB (`src/rubedo/db.py`) that back
every run. This — **not** the execution backend — is the real prerequisite
for genuine multi-machine/cloud execution (item 8): a distributed worker
can't write to a purely local SQLite file + local objects dir.

**Settled decisions (owner design session 2026-07-11 — do not re-litigate):**

- **`ObjectStore` protocol** (exists / read / write / delete, write
  carrying conditional-put semantics) with `LocalStore` + `S3Store`
  implementations; `store.py`'s module functions become thin delegates to
  a process-global store instance, so every call site (ledger, planning,
  execution, du, server) stays untouched. GCS rides the same protocol
  later. Reuse item 6's `client_factory=` pattern for endpoints/tests
  (fsspec was rejected: a heavyweight layer for four methods that fights
  item 6's hand-rolled-boto3 choice).
- **Staging is a local concept.** `LocalStore` keeps the staging dir +
  fsync + atomic `os.replace`; `S3Store` uploads directly with a
  conditional put (`If-None-Match: "*"`; GCS `ifGenerationMatch=0`) —
  a bucket upload is already atomic, so no staging keys exist and
  `cleanup_staged` is a cloud no-op.
- **Config = two URLs, no new concepts.** `RUBEDO_DB_PATH` already takes
  a URL (fix the mangling first — `db.py:48-51` wraps any non-sqlite
  string in `sqlite:///`; anything containing `://` must pass through
  verbatim, and the makedirs/`_ensure_gitignore` logic must skip URL
  targets). Add `RUBEDO_STORE_URL` (`s3://bucket/prefix`) + a `store=`
  param on `run()`/`plan()` with the same explicit-param-over-env
  precedence `home=` has. `home=` itself stays local-only (a real cloud
  deployment points DB and store at different systems, so one root URL
  can't express it). WAL/`busy_timeout` pragmas stay SQLite-only
  (already conditional).
- **New `size_bytes` column on `Materialization`, recorded at commit**
  (schema change — dev-stage reset ritual per CLAUDE.md, say so in the
  commit). `rubedo du` becomes a pure ledger query (delete the
  per-object `getsize` walk); the server's download endpoint switches
  `FileResponse` → `StreamingResponse` over `store.read` so it works on
  both backends; and 10b's warn-threshold gets its cheap size check
  (one SUM) for free.
- **Tests: SQLite + moto only for now** (owner call). `S3Store` is
  moto-tested in the always-run suite; real-Postgres correctness is
  deferred to **item 7b** — which makes trap (1) below extra
  load-bearing, because nothing automated exercises it until 7b lands.

**Trap (part of the spec):** **(1) The partial-index dialect trap** —
one-live-per-address is declared with `sqlite_where=text("is_live")`
*only* (`models.py:142-146`). Postgres ignores `sqlite_where`, so the
index silently becomes an **unconditional** unique index and the
supersede path (`_commit_materialization`'s demote-then-insert) breaks
the moment a second generation lands. Add `postgresql_where=` alongside
it in the same `Index`; untested until 7b, so do not "clean it up" away.
**(2) 412 is success** — a conditional put that fails with
PreconditionFailed means the object already exists: map it to the same
idempotent early-return the local exists-check takes, never an error.
**(3) Missing reads return None** — `read_materialization_output`
returns `None` for absent objects (du counts them as missing);
`S3Store.read` must map `NoSuchKey` to `None`, never raise. **(4) Path
stragglers** — `_get_object_path` leaks local paths to `du.py:141` and
`server.py:378/458` today; both call sites move behind the protocol
(du: sizes from the ledger; server: streaming read). Grep for any other
direct path use before calling it done.

Acceptance: `examples/count_lines` run twice with `RUBEDO_STORE_URL`
pointed at a moto/MinIO bucket → Created: 15 then Reused: 15, with
statuses, addresses, and lifecycle rows identical to the local run; a
Postgres `RUBEDO_DB_PATH` engine-creates cleanly (live behavior verified
manually until 7b); `rubedo du` against the cloud store makes zero
per-object API calls; the server payload/download endpoints stream from
the bucket; the `size_bytes` schema change ships with the store-reset
ritual in the commit message.

## 7b. Postgres ledger test coverage  **[follows item 7]**

The suite stays SQLite-only through item 7 (owner call, 2026-07-11);
this item pays that debt. Env-gated live tests: a pytest fixture keyed
on `RUBEDO_TEST_PG_URL` that cleanly skips when unset (suite stays green
offline, no docker requirement for local dev), plus a CI job with a
postgres service container so the gap is covered on every push. Cover
exactly the dialect-sensitive machinery: the generations protocol
(create → supersede → restore proving the partial unique index behaves
under `postgresql_where` — item 7 trap 1); the pairing guard
(`before_commit` listener) firing identically; the IntegrityError
retry-once commit-collision path; ORM immutability guards raising on
update/delete; a `queries.py`/selection smoke pass. Also add the
verification note to `AGENTS.md`: touching `db.py`/`models.py` ⇒ run the
PG suite (`docker run postgres` + `RUBEDO_TEST_PG_URL=...`).
Acceptance: full suite green both with and without `RUBEDO_TEST_PG_URL`
set; the CI postgres job green on real Postgres.

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

## 9. Non-topological (lane-pipelined) execution — [DONE 2026-07-10 — v1]

Shipped as `run(pipe, schedule="broad"|"deep")`; see the Done changelog
entry. Settled decisions from the design session: **one scheduler + barrier
policy, not two code paths** — the run is (lane, step) cells, the topo order
is partitioned into segments, and one segment executor
(`_run_segment` in `src/rubedo/runner.py`) drives every segment; **broad is
the default** (each step a singleton segment, degenerating to the classic
staged loop — the old loop is deleted, not flag-guarded); the knob is
**run-level** (`run()`/`run_pipeline()`, ValueError otherwise; `plan()`
untouched). Deep-eligible = `map` with ≤1 parent (skip_cache fusion
preserved); reduce/join are barriers by definition. **Unlocked later:**
expand interiors and multi-parent maps are barriers in v1 — both could join
deep segments with per-parent anchor / readiness handling. Scheduling
changes order only: statuses, addresses, and lifecycle rows are
byte-identical across modes (`tests/test_schedule.py`).

══════════════════════════════════════════════════════════════════════
# Tier 4 · Deferred / careful
══════════════════════════════════════════════════════════════════════

## 10a. Storage observability (the safe half) — [DONE 2026-07-09]

Shipped as `storage_report()` / `rubedo du [--json]` (`src/rubedo/du.py`);
see the Done changelog entry. Original spec, for context:

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

## 10b. Retention GC (byte-deleting)  **[⚠️ subtle — DANGEROUS; design settled 2026-07-10]**

Owner design session 2026-07-10 reframed this from mark-and-sweep to
**retention**. In steady state nearly everything is live (current
generations are live; orphans stay live by `producer-model.md` Q2), so
10a's reclaimable set ("every reference non-live") is tiny and sweeping it
buys little. The real storage hog is **old runs**: superseded generations
and orphaned-live materializations that only historical runs reference.
GC v1 therefore prunes by run recency, never touching what recent runs
used.

**Settled decisions (do not re-litigate):**

- **Two policies ship, no others:** per-pipeline **keep-last-N-runs** and
  a **global byte budget**. No age-based knob. Per-pipeline *byte* budgets
  were explicitly rejected as ill-defined — the store dedupes identical
  bytes across pipelines (`du.py`'s "per-pipeline bytes can sum to more
  than total"), so bytes are only globally meaningful; run count is the
  crisp per-pipeline unit.
- **Setting home:** `pipeline(..., retention=N)` (`None` default = keep
  everything; validate `N >= 1`). It rides the `definition()` snapshot
  each run records, and the ops path reads each pipeline's policy from its
  **latest run's `definition_json`** — `rubedo gc` never imports user code
  (same rule as `server.py`). No engine config file (rejected: new
  concept).
- **Triggers:** (a) end of a successful `run()` auto-prunes *that
  pipeline* when its `retention=` is set — set-and-forget is the point of
  a persisted setting; it **skips with a note** (never errors) if another
  run is in flight. (b) `gc(delete=False, max_bytes=None, home=None)` /
  `rubedo gc [--max-bytes SIZE] [--delete]` applies every recorded
  retention policy, then if the store still exceeds `max_bytes` prunes
  oldest-first across pipelines until it fits. **Dry-run is the default**
  for `gc()`/CLI: print exactly what would be demoted/deleted (riding
  `storage_report` machinery in `src/rubedo/du.py`), touch nothing without
  `delete=True` / `--delete`.
- **Default when unconfigured: keep everything**, plus a **warn-only
  threshold**: at end of run, if the store exceeds a constant (~1 GiB) and
  the pipeline has no `retention=`, print one line pointing at
  `retention=` / `rubedo gc`. Keep the check cheap — don't pay a full
  per-object `getsize` walk on every run of a huge store (e.g. reuse the
  sizes the run already touched, or sample/cache; implementer's choice,
  but the acceptance is "no O(store) stat storm per run").
- **Q2 softening, accepted with eyes open:** pruning demotes orphaned-live
  materializations outside the keep-set, so if pruned data *reappears* (a
  file comes back, a row's content reverts) the step **recomputes** —
  non-idempotent cost re-paid. The ledger heals lazily and safely:
  `stage_and_commit` re-writes the missing bytes (its exists-check fails
  post-delete) and the pruned row restores. Q2's keep-orphans default
  stands whenever `retention` is unset.

**Mechanics — two phases, both on existing machinery
(new module `src/rubedo/gc.py`, CLI verb in `src/rubedo/cli.py`):**

1. **Demote.** Keep-set = materializations referenced by the pipeline's
   last N *terminal* runs via `RunCoordinateStatus.materialization_id`
   (the latest terminal run always survives). Flip every still-live
   materialization of that pipeline outside the keep-set to
   `is_live=False` with a paired lifecycle row, `action="pruned"` — the
   pairing guard (invariant 8) enforces the pairing for free. Ledger rows
   are never deleted.
2. **Sweep.** Delete object files where **every** referencing
   materialization across **all** pipelines is now non-live — exactly
   `du.py`'s reclaimable rule — and append one row per deleted object to a
   new append-only **`object_reclamations`** table (`content_hash`,
   `bytes`, `created_at`, trigger/run id). New table ⇒ `create_all`
   handles it, no store reset. `rubedo du` must then report *reclaimed*
   separately from *missing* (deliberate deletion ≠ corruption).
3. **Global budget** (`max_bytes`): candidates = live materializations
   ordered by their most recent referencing run, oldest first, excluding
   anything referenced by a pipeline's latest terminal run; demote until
   the projected reclaimable bytes (computed under the shared-object rule)
   bring the store under budget, then sweep once.

**Trap (part of the spec):** **(1) Shared objects** — one physical object
(`hash[:2]/hash[2:4]/hash`) can back many materializations across
addresses, steps, and pipelines; "this materialization is prunable" does
**not** mean "its bytes are unreferenced." The sweep MUST ref-count
against *all* materializations before deleting a byte (one live reference
anywhere keeps it), or it silently guts live outputs (invariants 1 & 3).
`tests/test_du.py` already pins the one-live-one-dead shape. **(2)
Direction of truth** — demote and sweep by walking the ledger; **never**
enumerate the store; never delete ledger rows. **(3) The restore race** —
`stage_and_commit` early-returns without writing when the object file
already exists (`store.py:86`), and the ledger commit happens later in
the main thread: a concurrent run can pass the exists-check, GC deletes
the file, the run commits a **live** materialization pointing at nothing.
Guard: sweeping refuses while any run's `effective_run_status()` says
"running" (heartbeat machinery, shipped 2026-07-08); the end-of-run
auto-prune *skips* instead of erroring. **(4) Cloud irreversibility** —
the store is local-only today; when item 7 lands, `gc` must hard-refuse
non-local stores until dry-run + ref-count audit + object-versioned
buckets gate it. **(5) Expand anchors** — expand reuse hangs off a
parent-addressed cache-anchor materialization (`_plan_expand_reuse` in
`planning.py`); verify the keep-set query actually reaches anchors (they
may not appear in `RunCoordinateStatus.materialization_id`) and widen it
if not (e.g. `MaterializationEdge` closure from kept materializations).
Pruning a live anchor silently re-runs the scrape/LLM next run — the
exact cost this engine exists to prevent.

Acceptance: `retention=2` over three input generations → the next run (or
`rubedo gc --delete`) demotes exactly the generation only run 1
referenced, with paired `pruned` lifecycle rows, the freed object deleted
from disk and logged in `object_reclamations`, and the latest outputs
byte-identical; a shared object with one live reference in another
pipeline survives a prune that demotes its other referents; a pruned lane
whose input reappears recomputes and restores (lazy heal); `gc` and the
auto-prune refuse/skip while another run's heartbeat is live; dry-run
output lists exactly what a subsequent `--delete` does and deletes
nothing; an expand chain keeps its anchor through a prune and still
reuses; a store over `--max-bytes` prunes oldest runs first, never a
pipeline's latest terminal run, until under budget; `rubedo du`
distinguishes reclaimed from missing. Update `notes/invariants.md`
(pruned/reclaimed vocabulary; note under invariant 7 that retention
deletes *bytes*, never facts) and the README (retention + gc docs).

## 11. `expand` child views (dedup storage) — [RESOLVED 2026-07-10: already fixed by `2850e74`]

Retired without building anything: the premise died on 2026-07-06.
The original spec targeted option (a) from `notes/producer-model.md` —
"the step stores its full yielded list as a cache anchor *and* extracts
each item into its own child materialization, so scraped data is stored
twice" — and proposed option (b), children as lightweight views into the
anchor. But the source≡root-expand unification Phase 1 (`2850e74`,
`notes/unification-plan.md`) changed the anchor to store the child
**content hashes only** (`execution._expand_outcomes`,
`_plan_expand_reuse` replays hashes), which ended the double storage as a
side effect: payloads live once in the child materializations, the anchor
is a tiny JSON hash list, and the store's global content-addressing
already collapses identical payloads everywhere else. Verified live
2026-07-10: 3×100 KB expand children → store holds exactly three
100 KB objects + a 202 B anchor, full reuse on re-run. Option (b)'s
view-ref machinery would now *add* a concept to save ~0 bytes — the
ruthless-simplification answer is to not build it. (The stale "full
yielded list" comments in `planning.py`/`execution.py` and
`producer-model.md`'s "Known cost" note were fixed in the same pass.)

## 12. Lane tooling — following & invalidation

Two utilities that ride on machinery that already exists (`MaterializationEdge`
lineage, `MaterializationIndexEntry` labels); now that lanes can go
content-addressed/minted, they're the load-bearing navigation surface. The
two halves were separably shippable, and **both have now shipped**
(2026-07-09): lane-following as `trace()`, lane-level invalidation as
`invalidate(..., downstream=True)`.

- **Lane-following (lineage queries) — [DONE 2026-07-09].** Shipped as
  `trace(selection)` / `rubedo trace "<query>"` (`src/rubedo/trace.py`):
  selection-seeded BFS over `MaterializationEdge` both directions, live-only
  seeding by default (`include_superseded=True`/`--all` widens), traversal
  follows real edges regardless of liveness (marked, never hidden), and
  lineage roots resolve their stored payload at display time — the
  "always index source metadata" option was **decided against** (owner call
  2026-07-09): reading the object at display deletes the bookkeeping concept.
  Original spec, for context: "Find the results connected to a label
  at a certain step": index-lookup (`MaterializationIndexEntry`) to seed
  materializations carrying the label, then BFS up/down `MaterializationEdge`
  to reach connected outputs at other steps. Pure query over existing tables —
  a recursive CTE, no new bookkeeping. Survives reduce/expand/join because it
  is a materialization graph, not coordinate-equality. This is the "follow the
  path of a lane" utility that replaces a legible coordinate once lanes are
  opaque. Root-of-lineage → source row is answered by indexing source metadata
  at the root (decide: always index it).
- **Lane-level invalidation — [DONE 2026-07-09].** Shipped as a flag on the
  existing verb: `invalidate(selection, reason, downstream=True)` /
  `rubedo invalidate "<query>" --downstream` — seeds on the selection's live
  matches, walks trace's `_bfs` downstream, flips every live materialization
  in the closure (paired lifecycle rows; non-live nodes passed through, never
  re-flipped; upstream untouched; lazy heal on next run). Settled decisions:
  flag-on-invalidate (no new function, no selection-language change);
  **trace-as-preview** (same seeding rule + same BFS, correspondence
  guaranteed by test); **no blast-radius guardrail** — loud docs instead.
  Original context: today `invalidate(selection)` flips `is_live` on
  the selected materializations only, and the settled core semantics are
  lazy-via-recompute (invalidate a specific bad case, let the next run
  recompute — no eager descendant cascade; `producer-model.md` Q1). Note: since the invalidation UI was removed from the web dashboard, this invalidation tooling must be robust for CLI and code-first use cases.

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above)

**2026-07-10 — lane-pipelined execution (item 9, v1):**
`run(pipe, schedule="broad"|"deep")` — one scheduler, barrier policy. The
runner partitions the topo order into segments and drives every segment
through one segment executor (`_run_segment`): segment heads are planned
whole, executes go to per-step pools (thread, or loky process per
`executor=`), and every completion is committed in the main thread
(execution stays DB-free), immediately planning the lane's in-segment
consumers. Broad (default): every step a singleton segment — the executor
degenerates to plan-all → execute-all → commit-each, the old staged loop
(now deleted). Deep: maximal runs of consecutive `map`-with-≤1-parent steps
share a segment, so a lane races ahead through the chain the moment its own
inputs commit; reduce/join/expand/multi-parent maps are singleton barriers
(expand interiors + multi-parent maps unlockable later). Rate limiter is one
instance per step per run shared across all task submissions;
retries/assertions/_RunMemo semantics unchanged; failure and Filtered
cascades flow per lane. Scheduling changes order only — statuses,
addresses, content hashes, and lifecycle rows are identical across modes,
and either mode fully reuses a store the other wrote
(`tests/test_schedule.py`; per-lane planning via `_plan_step(..., lanes=)`,
per-cell execution via `execution._process_decision`).

**2026-07-09 — lane-level invalidation (item 12, second half — item 12 fully
done):** `invalidate(selection, reason, downstream=True)` / `rubedo invalidate
"<query>" --downstream` flips the selection's live matches plus their full
downstream closure (trace's `_bfs` over `MaterializationEdge`; live-only
seeding mirrors trace, so `rubedo trace` *is* the preview of the blast
radius — correspondence pinned by test). Every flip pairs a lifecycle row
(invariant 8); non-live nodes pass through untraversed-but-unflipped;
upstream never touched; no eager recompute — the next run heals exactly the
invalidated set. Run records `params_json={"downstream": true}`; result adds
`seed_count`/`downstream_count`. No guardrail on blast radius — loud docs
instead (`tests/test_invalidate_downstream.py`).

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

**2026-07-09 — storage observability (item 10a):** `storage_report()` +
`rubedo du [--json]` (`src/rubedo/du.py`) — total object-store size and
object count, a per-pipeline/per-step breakdown (bytes, materialization
counts, live vs not), and the **ref-count audit as a dry-run report**
("N objects / M bytes have zero live references"), computed by walking the
ledger and grouping physical objects by `output_content_hash` across *all*
materializations — never by enumerating the store directory. An object is
reclaimable only when every referencing materialization is non-live; one
live reference anywhere keeps it. Objects the ledger names but disk lacks
are counted as missing (never a crash) and excluded from the reclaimable
estimate. Nothing deletes — this is exactly the audit 10b would build on.
One finding for 10b: cache identity is coordinate-free, so identical input
bytes collapse to a single materialization; real object sharing arises from
*different* inputs whose outputs normalize to identical bytes
(`tests/test_du.py` covers the one-live-one-dead shared-object trap).

**2026-07-09 — lane-following (item 12, first half):** `trace(selection)` +
`rubedo trace "<query>" [--all] [--json]` — lineage BFS over
`MaterializationEdge` from any selection's materializations, upstream and
downstream, with root payload resolution at display time (no auto-indexing —
owner decision), live-only seeding by default, and superseded nodes marked
rather than hidden. Verified against newsroom's join-minted pair lanes and
expand-minted children (`tests/test_trace.py`). Also: v0.1.0 published to
PyPI via trusted publishing; CI on push/PR; `RunSummary.output_for` fixed to
include freshly created lanes.

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
