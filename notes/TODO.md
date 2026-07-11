# TODO

Each open item below is a self-contained spec: the design decisions are
settled (owner design sessions 2026-07-10/11 — do not re-litigate; flag
genuine contradictions), with file pointers, gotchas, and acceptance
criteria. Read `CLAUDE.md` first for conventions, and `notes/invariants.md`
for vocabulary. One item = one (or a few) commits.

Items keep their historical numbers for stable cross-references (gaps are
shipped/retired items — see the Done changelog). Order below is the
recommended build order: **10b** serves today's single-machine user and is
imminent; the cloud chain (**6** → **7**+**7b** → **8** → **13**) builds
when multi-machine demand is real — though **8** is independently buildable
(workers never touch the ledger/store; item 7 is its throughput story, not
a prerequisite).

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

## 8. Pluggable execution pools (bring-your-own cluster)  **[design settled 2026-07-11]**

Today `execution.py` offers `executor="thread"|"process"`, both single-machine.
`_execute_step`'s `call()` already treats "the pool" as anything satisfying
`.submit(fn, *args, **kwargs) -> Future-with-.result()` (`execution.py:278`) —
the same shape `dask.distributed` and a thin `ray` wrapper expose.

**Settled decisions (owner design session 2026-07-11 — do not re-litigate):**

- **No named backends.** `executor=` accepts `"thread"` | `"process"` | a
  **zero-arg factory returning a pool-like** (`.submit(fn, *args, **kwargs)`
  → Future with `.result()`). The engine never imports dask or ray; no
  `rubedo[dask]` extra exists; the zero-daemon positioning
  (`notes/framework_analysis.md`) survives because Rubedo itself never
  requires a cluster — a user who has one hands over a factory. The
  original add-vs-replace-`"process"` question dissolves: `"process"`
  (loky) stays, and no third *named* value is ever added. Documented
  recipe: `executor=lambda: Client("tcp://…").get_executor()` — dask's
  `ClientExecutor` already satisfies the shape, including `shutdown()`.
- **Attach point: per-step `executor=`**, exactly where `"process"` builds
  its loky pool today (`runner.py:262-265`); a factory-built pool slots
  into the same per-step `process_pools` dict, so mixed pipelines (LLM
  steps on threads, CPU steps on the cluster) fall out for free. Update
  the validation at `spec.py:239-240` to accept callables.
- **Item-7 dependency softened — buildable now.** Workers never touch the
  ledger or the store: parent payloads are resolved in the main process,
  only `fn` + args ship to the pool, results return over the wire, and
  staging/commit stay in the main thread. v1 is correct against the local
  SQLite + objects dir; item 7 is the *throughput* story (workers reading
  a shared store instead of routing payloads through the scheduler) and
  stays a later optimization, not a prerequisite — see item 13.
- **Testing: fake pool in the suite, live dask as an example.** The
  always-run suite proves the seam with a trivial in-repo `.submit()`
  fake (statuses/addresses identical to `"thread"`); a self-contained
  `examples/` script demonstrates a real `dask.distributed.LocalCluster`
  and serves as the manual acceptance run. Dask never enters the dev
  deps.

**Mechanics/notes:** pool lifecycle — the engine created it (via the
factory), so the engine shuts it down where loky pools are shut down today
(`runner.py:351`): duck-typed `shutdown(wait=True)` if present, else
`close()`. `step.workers` still bounds in-flight submissions (the per-step
thread pool wraps `call()`), independent of the external pool's own
parallelism. Retries/rate-limit/assertions/`_RunMemo` run main-side,
unchanged. `definition()` must stay JSON: serialize a factory executor as a
marker (e.g. `"external:<qualname>"`), never the object.

Acceptance: a step with `executor=<fake factory>` runs in the suite with
statuses, addresses, and lifecycle rows identical to `"thread"`; a bogus
`executor=` string still raises the `ValueError`; `definition()` of a
factory-executor pipeline serializes; the dask example runs a step on a
`LocalCluster` and fully reuses on a second run against the plain local
store — no item-7 machinery involved.

## 13. Pass-by-reference payloads (workers talk to the store directly)  **[depends on items 7 + 8; design settled 2026-07-11]**

With a cloud store and an out-of-process pool, the runner is a *byte hub*:
GET parent from the bucket → ship to the worker → full result back → PUT to
the bucket — four network transits per step hop, and reduce fan-in routes
all N parent payloads through one process. Refs make bytes flow
worker↔store directly; the runner handles only hashes and metadata.

**Settled decisions (owner design session 2026-07-11 — do not re-litigate):**

- **Activation is automatic — no per-step knob.** Refs engage when the
  store is non-local AND the step's executor crosses a process boundary
  (`"process"` *or* an item-8 factory pool — a local loky worker with a
  cloud store benefits too; `"thread"` shares the runner's memory and
  gains nothing). One escape hatch: `run(payload_refs=False)` forces hub
  routing for the whole run.
- **Credential-less workers degrade, never fail.** Before the first ref
  submission per (pool, run), the runner submits a cheap probe task (the
  shim attempts a store access check worker-side). On failure: warn once
  (run event + `UserWarning`: grant workers store access, or silence with
  `payload_refs=False`) and route that pool by value for the rest of the
  run. Don't probe per lane; don't cache across runs (credentials
  change).
- **Mechanism = a shim wrapping the fn.** The engine submits
  `_ref_call(store_config, refs, fn, …)` instead of `fn`; worker-side the
  shim GETs and deserializes inputs, calls `fn`, serializes + hashes +
  conditional-PUTs the result, and returns only
  `(content_hash, content_type, size_bytes, …)`. The pool contract stays
  plain `.submit()` — item 8's seam untouched; store config travels via
  the picklable `client_factory` pattern from items 6/7.
- **Both directions in v1** (reads and writes). The **ledger commit stays
  main-thread**: the runner commits from the returned metadata via a
  `stage_and_commit` variant that skips byte staging (the object is
  already in the store) but runs the full `_commit_materialization`
  generations/pairing machinery unchanged. Invariant 3 survives: a worker
  dying mid-PUT leaves at most an unreferenced object at a
  content-addressed key and no ledger row; a retry lands idempotently on
  the same key (item 7's 412-is-success).
- **Shapes: `map`/`reduce`/`join`; `expand` stays by-value.** Reduce
  fan-in is the biggest win (N payloads fetched in parallel by the
  worker). `expand` is deferred: `_expand_outcomes` mints coordinates and
  the anchor from child hashes main-side, so ref-ifying it moves
  coordinate minting into the shim — wait until it demonstrably bites.
- **Ephemeral parents stay by value.** `EphemeralRef`/skip_cache outputs
  aren't in the store by definition; refs are per-parent, so a mixed
  submission (some parents as refs, ephemeral ones by value) is the
  normal case, not an error.

**Trap (part of the spec):** **(1) The main-side value consumers.** Today
the runner holds every result value between `call()` and commit, and
several things quietly depend on that: output validation
(`_validate_output`), data-quality `assertions`, `Filtered` verdict
detection, and `@step(index=[...])` extraction at commit. Under refs the
runner never sees the bytes, so **each of these moves into the shim**
(index specs and assertion callables travel with the submission; the shim
returns index entries and verdicts in its metadata) — grep everything
that touches `result` between call and commit and account for every
consumer before shipping. **(2) One hasher.** The worker computes the
content hash the ledger will trust: the shim must call the *same*
`_serialize`/`hash_bytes` code the runner uses (import, never copy), or
identical values could land at different addresses and break dedup.
**(3) Missing objects worker-side** are a normal step failure with a
clear error ("parent object <hash> not in store"), never a silent
None payload. **(4) `size_bytes`** (item 7's column) comes from the
shim's metadata — it must equal what the store reports, since du now
sums the ledger.

Acceptance: with a moto/MinIO store and the suite's fake factory pool, a
chained map pipeline completes with **zero payload GET/PUT calls by the
runner** for ref-routed steps (assert by instrumenting the runner-side
store client), and its ledger rows, statuses, and addresses are
byte-identical to the same pipeline run with `payload_refs=False`; a
credential-less pool warns once and completes correctly by value; a
reduce over N parents fetches all N worker-side; an indexed, asserted,
filtering step behaves identically under refs and hub routing; `expand`
pipelines are untouched; a worker killed mid-PUT leaves no ledger row and
the re-run heals.

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above; git log has the detail)

**2026-07-10/11 — design-session sweep:** every open item's spec settled
(6, 7 + new 7b, 8, 10b reframed as retention GC, 13 added from the
byte-hub finding). **Item 11 (`expand` child views) retired** — its
double-storage premise had already died with `2850e74` (the anchor stores
child *content hashes*, not payloads; verified live: 3×100 KB children →
three 100 KB objects + a 202 B anchor, full reuse); option (b)'s view-ref
machinery would add a concept to save ~0 bytes. Item 8's `[depends on
item 7]` gate dropped (workers never touch ledger/store).

**2026-07-10 — lane-pipelined execution (item 9, v1):**
`run(pipe, schedule="broad"|"deep")` — one scheduler + barrier policy, not
two code paths: the topo order is partitioned into segments and one
segment executor (`_run_segment`, `src/rubedo/runner.py`) drives them all;
ledger writes stay in the main thread. Broad (default) = singleton
segments (the old staged loop is deleted, not flag-guarded); deep =
maximal runs of consecutive ≤1-parent `map` steps share a segment so a
lane races ahead as soon as its inputs commit; reduce/join/expand/
multi-parent maps are barriers (expand interiors + multi-parent maps
unlockable later). Scheduling changes order only — statuses, addresses,
and lifecycle rows are byte-identical across modes and either mode reuses
a store the other wrote (`tests/test_schedule.py`).

**2026-07-09 — lane tooling (item 12, both halves) + storage
observability (item 10a) + v0.1.0:** `trace(selection)` / `rubedo trace`
(`src/rubedo/trace.py`) — lineage BFS over `MaterializationEdge`, both
directions, live-only seeding by default, superseded nodes marked not
hidden; root payloads resolve at display time (auto-indexing source
metadata **decided against**). `invalidate(selection, reason,
downstream=True)` / `--downstream` — flips the selection's live matches
plus the downstream closure via trace's `_bfs`, so **trace is the
preview** (correspondence pinned by test); paired lifecycle rows; upstream
untouched; lazy heal; **no blast-radius guardrail** — loud docs (the web
UI's invalidation surface was removed, so this tooling must stay robust
for CLI/code-first use). `storage_report()` / `rubedo du [--json]`
(`src/rubedo/du.py`) — sizes per pipeline/step + the ref-count audit as a
dry-run report (an object is reclaimable only when *every* referencing
materialization is non-live; missing-from-disk counted, never a crash) —
exactly the audit 10b builds on; `tests/test_du.py` pins the
one-live-one-dead shared-object trap. Also: v0.1.0 on PyPI (trusted
publishing), CI on push/PR.

**2026-07-08 — heartbeat-derived run liveness:** stored `Run.status` is
terminal-only (NULL in flight; "running" is never stored — a durable row
can't keep a present-tense claim). A daemon thread bumps
`Run.last_heartbeat_at` every 60s; readers derive `running`/`interrupted`
via `effective_run_status()`. No reaper: sleep/wake self-heals. The
heartbeat is an ephemeral presence signal exempt from event pairing
(`invariants.md`, `tests/test_run_liveness.py`). Same pass fixed
`run(progress=True)` scoping and the `count_lines` params regression.

**2026-07-07 and earlier — bugs/hardening + foundation:** Tier 0 fixes
B1–B7/H1–H7 (multi-parent map crash, invalidation partial commits,
skip_cache on join/reduce, batch ledger planning, per-key `_RunMemo`
locking, SSE event-loop blocking, CORS pinning, packaging leanness, DRY/
N+1) · item 1 packaging hygiene (`litellm` out of core;
`scripts/smoke_test.sh` proves a clean-venv wheel install) · item 2
read-only ops CLI (`rubedo ls/show/invalidate` over `queries.py`, shared
with `server.py` so they can't drift; `pipeline:` selection term; failure
introspection). Foundation, in one breath: the **producer model**
(content-addressed lanes, `expand` with parent-addressed anchors,
`group_key` reduce, multi-source, N-way `join` — see
`notes/producer-model.md`) · content-addressed store + generations
(supersede/restore/refresh) · append-only ledger with ORM immutability
guards + the invariant-8 pairing guard · single `run()`/`plan()` entry
points, no registry, definition snapshots · step policies (retries,
rate_limit, stale_after, skip_cache fusion, assertions, filters,
`on_failed`) · `index=` + selection language with semver ranges ·
Folder/Csv/Table sources (streaming `batch_size`) · loky/cloudpickle
process executor · `RUBEDO_HOME` · mypy/py.typed pass · React dashboard
(DAG view, run inspector, SSE live view) · examples suite (`count_lines`
flagship, `hn_digest`, `pdf_digest`, …) · rename to Rubedo.
**Resolved won't-do** (don't re-propose): arbitrary-rules plugin surface;
plan()-in-UI (server never imports user code); per-producer census
(minted lanes orphan silently by design); behavior-preserving
Source→Producer refactor (went vertical instead).
