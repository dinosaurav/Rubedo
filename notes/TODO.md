# TODO

Each open item below is a self-contained spec: settled design decisions
(do not re-litigate; flag genuine contradictions), file pointers, gotchas,
and acceptance criteria. Read `CLAUDE.md` first for conventions and
`notes/invariants.md` for vocabulary. One item = one (or a few) commits.

Items keep their historical numbers for stable cross-references. The
pre-restructure TODO (with the full Done changelog and every shipped
item's spec) is archived verbatim at `notes/TODO-obsolete.md` — gaps in
the numbering below are shipped or retired items, documented there. New
items continue from **29**. Items tagged **[needs owner decision]** have
a settled problem statement but an unratified fix — propose, don't build.
Unsettled ideas live in **Parked** at the bottom — do not build those
without a design session.

Items 29–34 come from the 2026-07-18 full-codebase review (external
agent, findings re-verified against source before being written down).

──────────────────────────────────────────────────────────────────────

## 29. Expand-table Arrow rows permanently record `run_id=""`

The optimized table-expand path builds its Arrow batch with
`"run_id": pa.array([""] * len(children))` and a comment claiming the
ledger fills it in (`src/rubedo/execution.py:497`). It doesn't: the
`arrow_batched` branch in `_commit_execution_result`
(`src/rubedo/ledger.py:346`) only *reads* the buffered row back for the
MatRef — nothing patches `run_id` before `append_arrow_batch` flushes.
Every child row from this path lands on disk with an empty `run_id`,
so lane-level provenance (which run created this output?) is broken for
the optimized path, and the server's object endpoint reports an empty
`created_by_run_id`.

**Fix:** thread the run id into `_expand_table_outcomes` (it's on the
`ctx` the caller holds) and write it into the batch, then delete the
stale "filled by ledger" comment. No schema change — the column exists
and is simply mis-filled.

**Trap:** the non-table expand path and ordinary map commits already
record `run_id` correctly — don't touch them; assert equality between
the two expand paths in the test rather than pinning a literal.

Acceptance: a pipeline whose root expand takes the table path produces
Arrow rows whose `run_id` equals the creating run's id (test reads rows
back via `lane_store` and compares against the run row); the object/API
metadata for such a lane reports the real `created_by_run_id`;
`uv run pytest -q` green; e2e Created:22 → Reused:22.

## 30. `/api/current-outputs` silently drops steps

`get_current_outputs` (`src/rubedo/server.py:257`) groups
`RunCoordinateStatus` by only `(source_id, coordinate)` and keeps
`max(id)` per group. In a normal chain every step writes a status for
the same `(source_id, coordinate)`, so the endpoint keeps only the
deepest step's row and drops the rest; across pipelines the same pair
can also collide. The endpoint's claim ("the latest run's live lanes")
is only true for single-step pipelines.

**Fix:** group by `(pipeline_id, step_name, source_id, coordinate)` (or
whatever narrower key the dashboard's "Current Outputs" view actually
intends — check `web/src/` usage first and say which in the commit).

Acceptance: an API regression test drives a two-step chain and asserts
one current-output row per (step, lane), not one per lane; two
pipelines with overlapping coordinates don't collide; Playwright suite
still green.

## 31. Declarative `join()`/`union()` bypass construction-time validation

`Pipeline.join` and `Pipeline.union` (`src/rubedo/pipeline.py:296,323`)
construct `StepSpec` directly, skipping the validation `step()` gives
decorated steps. An empty or one-parent `join_on`, or a `union` with no
parents, is accepted at declaration and fails later inside planning
with an internal error instead of a clear `ValueError` at build time.

**Fix:** apply the same rules `step()` enforces — `join_on` needs ≥2
parents, `union` needs ≥1 — either by routing the declarative
constructors through shared validation helpers or by adding the checks
inline (keep `spec.py` readable per the flagship rule; the helpers can
live in `pipeline.py`).

Acceptance: `p.join(name="j", join_on={})` and
`p.join(name="j", join_on={"a": "x"})` and
`p.union(name="u", depends_on=[])` each raise `ValueError` naming the
step and the rule at declaration (or at `_build_spec`, matching where
equivalent `@step` errors fire); messages match the existing error
style; full suite green.

## 32. Docs reconciliation: invariants.md + README vs shipped code

Three places where the written guarantees drifted from the code:

- `notes/invariants.md:41` still lists the pre-rewrite Arrow schema
  (`content_hash`, `output_path`) — the shipped schema is `output` +
  `output_identity` + `content_type` (see `lane_store.py`'s module
  docstring, which is current).
- `notes/invariants.md:46` says `input_hash_usages` is one row per
  `(address, step, pipeline)` — the schema's primary key is `address`
  alone (`src/rubedo/models.py`, `InputHashUsage`). Make the doc match
  the code for now; item 33 is where the *key itself* may change.
- `README.md:247` calls the web dashboard "read-only" without
  qualification, but the server exposes
  `POST /api/selection/invalidate` (`src/rubedo/server.py:621`). The
  *UI* is read-only; the API is not. Say exactly that, in README and
  in `AGENTS.md`'s server bullet, and note the endpoint is unauthenticated
  and intended for local use.

Acceptance: the three passages match the code; `mkdocs build --strict`
green; no engine changes in the commit.

## 33. Cross-pipeline liveness coupling  **[needs owner decision]**

Output addresses (`compute_output_address`, `src/rubedo/hashing.py:34`)
hash `(step, version, input_hash[, params][, code])` — no pipeline
identity. `input_hash_usages` is keyed by address alone
(`src/rubedo/models.py`). But Arrow content *is* pipeline-scoped
(`tables/<pipeline>/<step>.arrow`). Consequence: two pipelines with an
identically named+versioned step and identical inputs share one
liveness row. Invalidating or retention-pruning in pipeline A flips
`fulfilled=False` for pipeline B (needless recompute); conversely B can
see `fulfilled=True` from A's commit, miss in its own Arrow file, and
recompute anyway. Everything degrades to "recompute", never corruption
— the two-mechanism reuse check (IHU **and** Arrow row) self-heals —
but liveness semantics silently cross pipeline boundaries.

**Options (pick one, then build):**
(a) Scope liveness: add `pipeline_id` to the IHU primary key (schema
change → dev-stage reset ritual). Liveness then matches content
scoping. Recommended: it makes the two mechanisms congruent and the
`models.py` docstring's "the caller already knows pipeline_id" argument
already assumes per-pipeline reads.
(b) Make reuse deliberately global: keep the shared key and make the
Arrow lookup consult other pipelines' files too (a shared cache across
pipelines). Bigger semantic change; interacts with GC and retention.

**Trap:** GC (`src/rubedo/gc.py`) sweeps by address across *all*
pipelines today — whichever option wins, re-derive the demote/sweep
logic against it, and add cross-pipeline invalidation + retention tests
(two pipelines, same step name/version/input) that pin the chosen
semantics.

Acceptance (option a): the cross-pipeline test shows invalidating A
leaves B's reuse intact; schema-change commit follows the dev-stage
reset ritual; GC test covers two pipelines sharing content hashes.

## 34. `home=` is process-global  **[needs owner decision on end-state; small guard buildable now]**

`_init_home` (`src/rubedo/runner.py:43`) repoints module-global DB,
object-store, and lane-table state. Two concurrent runs in one process
targeting different homes will silently switch each other's backing
store mid-run. Single-home processes (the normal case, and every test)
are unaffected.

**Minimal buildable slice:** document one-home-per-process, and make
concurrent conflicting `_init_home` calls raise (a module-level "active
home + live-run refcount" check) instead of corrupting. **Eventual
fix** (needs design): a per-run context object carrying db/store/tables
handles — decide whether that's worth the plumbing before building it.

Acceptance (slice): two threads running pipelines with different
`home=` values → the second raises a clear error naming both homes;
same-home concurrency and the no-home default are untouched; docs state
the constraint.

──────────────────────────────────────────────────────────────────────

## 25. Did-you-mean suggestions  **[DEFERRED — owner 2026-07-14: queued, do not build until asked]**

`difflib.get_close_matches` on the loud errors: item 22's unmatched
parameter names, unknown `depends_on`/`join_on` step names, unknown
`Selection` fields, CLI step/pipeline arguments. Small, self-contained;
waits for the owner's go.

## 6. Cloud object storage sources (`S3Source` / `GCSSource`)  **[design settled 2026-07-10; ⚠️ respec after item 14]**

> **2026-07-12:** item 14 deletes the `Source` protocol this spec subclasses.
> The settled *decisions* survive — etag-based change token, LIST-only
> enumeration, `client_factory=` — but the shape becomes an `@source` recipe:
> the source generator LISTs and yields `{key, etag, size}` (cheap tokens,
> zero GetObject), and a downstream cached `map` step GETs the bytes. The
> containment property is preserved by lane structure instead of by
> `scan()`/`load()`: a churned token recomputes one lane (one re-download),
> nothing else. Do not build as written; respec against the post-14 world.

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

## 7. Configurable cloud ledger + object store (Postgres / S3-GCS)  **[design settled 2026-07-11; ⚠️ pre-Arrow-rewrite spec — re-verify pointers]**

> **2026-07-18:** this spec predates the Arrow storage rewrite: the
> `materializations` table, `is_live` partial index, and
> `_commit_materialization` it references are deleted, and the lane
> store's Arrow files under `tables/` are a third storage surface the
> protocol must now cover (or explicitly exclude). The *decisions*
> (ObjectStore protocol, two-URL config, staging-is-local, conditional
> puts) stand; re-verify every file/line pointer and re-derive the trap
> list against current `models.py`/`ledger.py`/`lane_store.py` before
> building.

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
  a URL (fix the mangling first — `db.py` wraps any non-sqlite
  string in `sqlite:///`; anything containing `://` must pass through
  verbatim, and the makedirs/`_ensure_gitignore` logic must skip URL
  targets — the module docstring currently overpromises here). Add
  `RUBEDO_STORE_URL` (`s3://bucket/prefix`) + a `store=`
  param on `run()`/`plan()` with the same explicit-param-over-env
  precedence `home=` has. `home=` itself stays local-only (a real cloud
  deployment points DB and store at different systems, so one root URL
  can't express it). WAL/`busy_timeout` pragmas stay SQLite-only
  (already conditional).
- **Tests: SQLite + moto only for now** (owner call). `S3Store` is
  moto-tested in the always-run suite; real-Postgres correctness is
  deferred to **item 7b**.

**Trap (part of the spec):** **(1) Dialect-sensitive DDL** — any
`sqlite_where`/SQLite-only index or constraint silently degrades on
Postgres; audit current `models.py` for dialect-conditional DDL and add
the `postgresql_*` twin in the same declaration; untested until 7b, so
do not "clean it up" away. **(2) 412 is success** — a conditional put
that fails with PreconditionFailed means the object already exists: map
it to the same idempotent early-return the local exists-check takes,
never an error. **(3) Missing reads return None** — absent objects must
read as `None` (du counts them as missing); `S3Store.read` must map
`NoSuchKey` to `None`, never raise. **(4) Path stragglers** — grep for
direct `objects/` path construction (du, server download endpoints)
and move every call site behind the protocol before calling it done.
**(5) The Arrow lane store** (`tables/`) is local-file I/O via pyarrow —
decide explicitly whether item 7 covers it or scopes it out, and write
that decision into the commit.

Acceptance: `examples/count_lines` run twice with `RUBEDO_STORE_URL`
pointed at a moto/MinIO bucket → identical Created/Reused counts,
statuses, addresses, and lifecycle rows to the local run; a Postgres
`RUBEDO_DB_PATH` engine-creates cleanly (live behavior verified
manually until 7b); `rubedo du` against the cloud store makes zero
per-object API calls; the server payload/download endpoints stream from
the bucket.

## 7b. Postgres ledger test coverage  **[follows item 7]**

The suite stays SQLite-only through item 7 (owner call, 2026-07-11);
this item pays that debt. Env-gated live tests: a pytest fixture keyed
on `RUBEDO_TEST_PG_URL` that cleanly skips when unset (suite stays green
offline, no docker requirement for local dev), plus a CI job with a
postgres service container so the gap is covered on every push. Cover
exactly the dialect-sensitive machinery: the IHU claim/fulfill lifecycle
under concurrent runs; any dialect-conditional index from item 7 trap 1;
the IntegrityError retry-once commit-collision path; ORM immutability
guards raising on update/delete; a `queries.py`/selection smoke pass.
Also add the verification note to `AGENTS.md`: touching
`db.py`/`models.py` ⇒ run the PG suite (`docker run postgres` +
`RUBEDO_TEST_PG_URL=...`).
Acceptance: full suite green both with and without `RUBEDO_TEST_PG_URL`
set; the CI postgres job green on real Postgres.

## 8. Pluggable execution pools (bring-your-own cluster)  **[design settled 2026-07-11]**

Today `execution.py` offers `executor="thread"|"process"`, both single-machine.
The execute path already treats "the pool" as anything satisfying
`.submit(fn, *args, **kwargs) -> Future-with-.result()` —
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
  its loky pool today; a factory-built pool slots
  into the same per-step `process_pools` dict, so mixed pipelines (LLM
  steps on threads, CPU steps on the cluster) fall out for free. Update
  the executor validation in `spec.py` to accept callables.
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
factory), so the engine shuts it down where loky pools are shut down
today: duck-typed `shutdown(wait=True)` if present, else
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

## 13. Pass-by-reference payloads (workers talk to the store directly)  **[depends on items 7 + 8; design settled 2026-07-11; ⚠️ pre-Arrow-rewrite spec — re-verify against inline outputs]**

> **2026-07-18:** this spec predates inline Arrow outputs: most small
> outputs never touch the object store anymore, so "every payload is a
> store object" no longer holds — refs only make sense for *spilled*
> values (`"objects:<hash>"`). Re-derive the activation rule and the
> shim's read path against `lane_store.py` before building; the
> decisions below otherwise stand.

With a cloud store and an out-of-process pool, the runner is a *byte hub*:
GET parent from the bucket → ship to the worker → full result back → PUT to
the bucket — four network transits per step hop, and aggregate fan-in routes
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
  commit variant that skips byte staging (the object is
  already in the store) but runs the full commit machinery unchanged.
  The crash-safety guarantee survives (`notes/invariants.md`): a worker
  dying mid-PUT leaves at most an unreferenced object at a
  content-addressed key and no ledger row; a retry lands idempotently on
  the same key (item 7's 412-is-success).
- **Shapes: `map`/`aggregate`/`join`; `expand` stays by-value.** Aggregate
  fan-in is the biggest win (N payloads fetched in parallel by the
  worker). `expand` is deferred: the expand path mints coordinates and
  the anchor from child hashes main-side, so ref-ifying it moves
  coordinate minting into the shim — wait until it demonstrably bites.
- **Ephemeral parents stay by value.** `EphemeralRef`/skip_cache outputs
  aren't in the store by definition; refs are per-parent, so a mixed
  submission (some parents as refs, ephemeral ones by value) is the
  normal case, not an error.

**Trap (part of the spec):** **(1) The main-side value consumers.** Today
the runner holds every result value between `call()` and commit, and
several things quietly depend on that: output validation, data-quality
`assertions`, and `Filtered` verdict detection. Under refs the
runner never sees the bytes, so **each of these moves into the shim**
(assertion callables travel with the submission; the shim returns
verdicts in its metadata) — grep everything
that touches `result` between call and commit and account for every
consumer before shipping. **(2) One hasher.** The worker computes the
content hash the ledger will trust: the shim must call the *same*
serialization/hash code the runner uses (import, never copy), or
identical values could land at different addresses and break dedup.
**(3) Missing objects worker-side** are a normal step failure with a
clear error ("parent object <hash> not in store"), never a silent
None payload. **(4) `size_bytes`** comes from the
shim's metadata — it must equal what the store reports, since du
sums the ledger.

Acceptance: with a moto/MinIO store and the suite's fake factory pool, a
chained map pipeline completes with **zero payload GET/PUT calls by the
runner** for ref-routed steps (assert by instrumenting the runner-side
store client), and its ledger rows, statuses, and addresses are
byte-identical to the same pipeline run with `payload_refs=False`; a
credential-less pool warns once and completes correctly by value; an
aggregate over N parents fetches all N worker-side; an indexed, asserted,
filtering step behaves identically under refs and hub routing; `expand`
pipelines are untouched; a worker killed mid-PUT leaves no ledger row and
the re-run heals.

──────────────────────────────────────────────────────────────────────

## Parked (ideas, deliberately unspecced — design session required before building)

- **Cloud control plane** — hosted execution, deploy/build service,
  scheduler, secrets vault, shared team cache, dashboard write surfaces.
  Spine ratified 2026-07-13; full design in
  `notes/private/cloud-control-plane.md` (gitignored, owner-local —
  services live *outside* `src/rubedo/`). Gated on items 7, 8, 13; the
  engine-side slice (item 21, `pipeline(secrets=, env=)` + `rubedo check`)
  shipped 2026-07-14. Remaining sessions before building: vault
  build-vs-buy, build-sandbox isolation tech, tenant-scale ceiling — see
  the doc's open-questions section.

- **Sinks / reverse ETL** (the return leg of the refinement loop:
  CSV/Sheet in → refined batch back out; Sheets via gspread, Excel via
  openpyxl as extras, CSV/Parquet trivially). **Owner re-raised
  2026-07-18 ("reverse ETL") — this is the next design session to
  schedule.** Belongs **in code, in the pipeline
  file** — settled. The open fork is **step vs verb**, and it's the
  real design session. Owner leans *step* for simplicity
  (2026-07-13): a terminal aggregate that writes the target gets
  change-detection free from the planner (inputs unchanged → reuse →
  no write — the incremental-sync diff with zero new concepts), shows
  delivery in `describe()`/lineage, and is in fact writable today
  with no new machinery. The tension to resolve before blessing it:
  the ledger is trustworthy because it describes a store the engine
  owns; a Sheet is mutable external state, so a *cached* "delivered"
  can silently go false (hand-edited/replaced target won't re-write
  without a version bump), delivery failures conflate with refinement
  failures in run outcomes, and the sink's materialization is a
  receipt, not data — entering GC/retention/lineage machinery built
  for data. Candidate synthesis: declared in the pipeline and drawn
  in the DAG like a step, but diffs against the ledger's own record
  (not assumed target state) and logs delivery as events rather than
  materializations. Verb alternative (`p.export(select=..., to=...)`
  as a ledger projection at the server's altitude) stays on the table
  as the re-assertable/repair-friendly shape.

- **Bucketed aggregation / `allocate`** (batching lanes into ~N-sized
  groups — owner re-raised 2026-07-18 as an "allocate" shape). The naive
  "first 50 to finish" is nondeterministic and breaks order-independent
  cache identity; sorted-chunks shift every boundary on any insertion
  (near-total recompute). **Correction to the earlier sketch
  (2026-07-18):** `hash(lane_key) % ceil(n/50)` is *also* unstable —
  the divisor changes whenever n crosses a multiple of 50 and reshuffles
  nearly every lane. A viable design needs a membership rule independent
  of total lane count: fixed hash-prefix buckets, rendezvous hashing, or
  a declared fixed bucket count. With `fold` shipped (2026-07-18),
  tree-reduce falls out once bucketing exists (fold per bucket, then
  fold the bucket outputs). Owner: useful for some flows, not near-term
  (2026-07-12); membership rule is the design session.
- **skip_cache expansion** (owner note 2026-07-18: "skip_cache needs
  some work"). Needs a concrete problem statement before any design.
  The current contract is intentionally narrow — lazy, per-run memoized,
  never materialized, fused identity, incompatible with collective/
  fan-out shapes — and any expansion must preserve those guarantees or
  be a separate feature. (Note: "always rerun" is already shipped as
  `check_cache=False` — force execution while still materializing; do
  not add a synonym without a semantic distinction.)
- **Per-step spill override** (`@step(spills=[...])`) — the one piece of
  item 27 that didn't ship: force named fields to the object store,
  overriding the size/type rules. Waits for a real payload that needs
  it.
- **`plan --why` / recompute-blame.** Itemize which identity slot changed
  for an `execute` decision (input vs params vs code vs version vs stale)
  against the last live generation; the "blame" extension walks lineage
  upstream to the *first* changed thing and shows its value diff. Later.
- **Streaming expand** — commit each yielded child as it arrives instead
  of buffering the full expansion. Multiple independent payoffs: bounded
  memory on huge fan-outs, a crash mid-expansion keeps the
  already-committed children, and under `schedule="deep"` downstream
  lanes could start before the expansion finishes (barrier relaxation).
  **The trap that makes it non-trivial:** the expand *anchor* must commit
  strictly last, after every child — an early anchor + a mid-expansion
  crash reads as a complete, reusable expansion on the next run. Unrelated
  to item 14/scan; parked on demand, not on design doubt.
- **Step-version diff.** The ledger already holds *both generations*
  across a version bump — a `diff("step", "v1", "v2")` showing per-lane
  output changes is prompt A/B testing as a read-only ledger query
  (run v2 on a sample, compare, then commit to the batch). Data model
  needs nothing; pairs with the parked run-diff/code-diff ideas
  (2026-07-13).
- **Per-lane cost tracking / $-saved.** Steps that call paid APIs
  record cost per lane; run summary reports "reused $N of prior work."
  The product's value prop as a number, printed every run. Rides the
  existing ledger (2026-07-13).
- **Human-in-the-loop overrides** — accept/correct individual lane
  outputs (LLM refinement always needs a human pass on some rows).
  Natural fit: an override is a new generation with provenance
  `human` instead of a step run, so append-only survives — but this
  touches the commit path and the liveness lifecycle
  (`notes/invariants.md`), and would be the
  dashboard's first write surface (**DANGEROUS** — full design
  session required, do not sketch in code) (2026-07-13).
- **Failure triage view.** Blocked/failed lanes already accumulate in
  the ledger; a first-class "these 14 rows failed, retry just these"
  surface (CLI + dashboard) turns an engine fact into a refinement
  workflow (2026-07-13).

## Done

The full pre-restructure changelog lives in `notes/TODO-obsolete.md`
(and git log has the detail). Since the restructure:

- **2026-07-18 — `fold` (item 28 Phase 2):** `in_shape="fold"` shipped —
  streaming accumulator, aggregate-identical caching/planning/ledger,
  coordinate-sorted execution, `fold_init` required + JSON-validated +
  snapshotted, unary (one parent), `arrow_aggregate` rejected.
  `tests/test_fold.py`. Item 28 is fully shipped (Phase 1 landed
  2026-07-17 as `in_shape`/`out_shape` + `aggregate` rename).
- **2026-07-18 — TODO restructure:** old TODO archived verbatim to
  `notes/TODO-obsolete.md`; items 26 (retired) and 27/28 (shipped —
  27 minus the `spills=` valve, now Parked) dropped from the live list;
  items 29–34 added from the re-verified codebase review; items 6/7/13
  carry fresh ⚠️ respec banners where the Arrow rewrite invalidated
  their file pointers.
