# TODO

Each open item below is a self-contained spec: the design decisions are
settled (owner design sessions 2026-07-10/11/12 — do not re-litigate; flag
genuine contradictions), with file pointers, gotchas, and acceptance
criteria. Read `CLAUDE.md` first for conventions, and `notes/invariants.md`
for vocabulary. One item = one (or a few) commits.

Items keep their historical numbers for stable cross-references (gaps are
shipped/retired items — see the Done changelog). Order below is the
recommended build order: **16** no longer waits on anything (**15** the
rotation shipped 2026-07-13, **14** sources purge shipped 2026-07-13 — see
the Done changelog); the editorial pair (**17**, **19**) slots
anywhere (**20** ascii describe shipped 2026-07-13; **18** notes hygiene
shipped 2026-07-13). The cloud
chain (**6** → **7**+**7b** → **8** → **13**) builds when multi-machine
demand is real — though **8** is independently buildable (workers never
touch the ledger/store; item 7 is its throughput story, not a
prerequisite), and **6 needs a respec post-14** (see its note). (**10b**
retention GC shipped — see the Done changelog.) Unsettled ideas live in
**Parked** at the bottom — do not build those without a design session.

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

## 16. Step ergonomics: auto-name, default version  **[design settled 2026-07-12; 15 shipped]**

`@step` requires `name=` and `version=`; both should have defaults.

**Settled decisions (owner design session 2026-07-12):** `name` defaults to
`fn.__name__` (precedent: `@source` already does exactly this); `version`
defaults to `"0"`; **`code="warn"` stays the default** — the drift warning
is the teaching moment, and auto-recompute-on-edit stays a deliberate
opt-in (`code="auto"`). Explicitly considered and rejected: pairing an
omitted version with `code="auto"` (silent policy magic; owner 2026-07-12).

**Trap:** duplicate auto-names (two steps from same-named fns) must still
die loudly via the existing duplicate-name validation — the error message
should say the name came from the function. Bare `@step` (no parens) should
work if cheap, else `@step()` is fine — pick one and document it.

Acceptance: `@step()\ndef parse(...)` yields a step named `parse`, version
`"0"`; editing its body under the default warns (code-drift) rather than
recomputes; duplicate function names error with a message naming both
definitions; the tutorial's first example drops `name=`/`version=` and the
versioning docs still introduce them immediately after.

## 17. Rewrite `notes/invariants.md` values-first  **[editorial; owner reviews draft before commit]**

The eight invariants read as implementation facts and create weird
emphases; they should derive from the project's actual promises. Structure
the rewrite as ~4 core promises — *never pay twice for the same
computation; never lie about what happened; order and parallelism never
change results; bytes are disposable, facts are not* — with the current
invariants recast as supporting guarantees underneath (merge freely;
nothing user-visible changes).

**Trap:** the numbering is load-bearing — `models.py` ("invariant 8"
pairing guard), `gc.py` ("invariant 7"), AGENTS.md, and the docs site
(which publishes the file verbatim via snippet-include) all reference
numbers. Renumber if the new structure wants it, but grep-sweep every
reference in the same commit, and rebuild the docs. This is also the item
that answers "is the generations machinery necessary" — the schema exists
to serve *never lie about what happened*; if that promise survives the
rewrite unchanged, the Parked schema-simplification question dies with it.

Acceptance: `rg "invariant [0-9]" src tests notes docs AGENTS.md` resolves
against the new document with no dangling numbers; docs build clean; owner
signed off on the draft before the commit.

## 19. Comment cleanup pass  **[editorial; owner drives style]**

A pass over `src/`, `tests/`, and `examples/` replacing process-note
comments (what changed, which TODO item shipped it, why the diff was
correct) with code-truth comments (constraints the code can't show).
Known instances: `spec.py:91` ("TODO 10b"), `du.py:82`. The owner will
set the rewrite style on first contact; don't batch-rewrite ahead of that.

Acceptance: no comment in `src/` references a TODO item number or narrates
a past change; constraint comments (invariant references, trap guards)
stay.

──────────────────────────────────────────────────────────────────────

## Parked (ideas, deliberately unspecced — design session required before building)

- **Bucketed reduce** (`shape="reduce"` with a batch size). The naive
  "first 50 to finish" is nondeterministic and breaks order-independent
  cache identity; sorted-chunks shift every boundary on any insertion
  (near-total recompute). The viable design is **hash buckets**:
  `hash(lane_key) % ceil(n/50)` → stable membership, ~50-sized batches,
  only the touched bucket recomputes, each bucket fires as soon as its
  members land (pairs with `schedule="deep"`), and tree-reduce falls out
  free (a second reduce over the bucket outputs). Nondeterministic batching
  would only be tolerable with a cache-identity opt-out — its own can of
  worms. Owner: useful for some flows, not near-term (2026-07-12).
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
- **Generations/schema simplification** — gated on item 17: if the
  invariants rewrite keeps *never lie about what happened* as a core
  promise, this dies; if that promise is softened, revisit whether
  `materialization_lifecycle` + the pairing guard could shrink
  (**DANGEROUS** — touches invariant 8, GC safety, and crash recovery).

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above; git log has the detail)

**2026-07-13 — the rotation (item 15):** `PipelineBuilder` deleted;
`pipeline(name=...)` now returns a `Pipeline` — the one object steps
register on (`@p.step`/`@p.source` or `steps=[...]`, both stay) and verbs
live on as methods (`.run()`/`.plan()`/`.describe()`/`.definition()`); no
more `.build()` — the `PipelineSpec` builds and validates lazily on first
verb/`.spec` access and is cached. `id` is gone; `name` is the sole
identity (the ledger's `pipeline_id` column stores it verbatim, no schema
change). `schedule=`/`home=` moved from `run()`/`plan()` onto
`pipeline(...)` construction, joining `retention=`/`params_model=`
(retention's own validation stays eager, at `__init__`, since — unlike the
step-list checks — it doesn't depend on steps registered later). Module
rotation: `spec.py` is now pure data (`StepSpec`/`PipelineSpec`/`step()`/
`source()`/`definition()` only) and never imports upward; new
`src/rubedo/pipeline.py` sits above the engine and owns `Pipeline` +
`pipeline()` + the moved validation; `runner.py` split along its one seam
— segment machinery (`_partition_segments`/`_run_segment`, broad/deep,
`_scanned_for`) moved to new `src/rubedo/scheduler.py`, run/plan
orchestration stayed. `planning.py` untouched. `definition()`'s output is
pinned byte-identical across the rotation by
`tests/test_definition_snapshot.py` (recorded before, verified unchanged
after — the JSON's `"id"` key is retained, mirroring `"name"`, for
dashboard/history schema stability). Every test, example, and doc page
swept from `run(p)`/`plan(p)`/`describe(p)`/`PipelineBuilder` to
`p.run()`/`p.plan()`/`p.describe()`/`pipeline(...)`; live-verified against
the pre-rotation `.rubedo` store (not wiped): `count_lines` Reused: 22/22,
plus `newsroom`/`expand_feed`/`orders_rollup`/`github_health`/
`gutenberg_stats`/`weather_advisory` all run clean (`graphify`/`hn_digest`/
`pdf_digest` need `OPENROUTER_API_KEY`, swept but not executed). **Spec
ambiguity found and resolved pragmatically:** the settled `p.run(params=
None, force=False, progress=False)` signature omitted `workers=`/
`progress_cb=` (heavily used by tests for determinism); kept both as
per-invocation `Pipeline.run()` parameters — consistent with "run() keeps
only per-invocation things," just not exhaustively enumerated. Commits
`2edf4ed` (snapshot pin), `3280f67` (engine rotation), `3a1098f` (retention
eager-validation fix), `471eb44` (test sweep), `5e98587` (examples sweep),
`1bf31b4` (docs sweep).

**2026-07-13 — ascii describe (item 20):** `describe(format="ascii")` —
hand-rolled layered terminal DAG rendering in `spec.py` (depth = longest
path from a root; virtual passthrough nodes route edges spanning layers;
`├`/`┤` junctions vs corner arms chosen by rank bookkeeping). Deterministic
(spec order, snapshot-pinned byte-identical in
`tests/test_describe_ascii.py`); canvas >100 columns falls back to the
`text` renderer; zero new dependencies; `ValueError` lists all three
formats. Commit `1f117eb`.

**2026-07-13 — notes hygiene (item 18):** `notes/unification-plan.md`
deleted (historical; already unpublished from the docs site). Swept
`notes/producer-model.md` for item-14 fallout: the never-built `(subkey,
value)` expand emit contract corrected to the shipped bare-value/
`row-<hash>` contract; every `Manifest`/`ManifestEntry` reference removed
(that table, and the per-producer census it motivated, were both dropped
before shipping — reworded as a "tried, then dropped" retrospective); the
stale pre-build "What changes in the code" proposal section deleted
(wrong on most bullets in hindsight); Sequencing step 4a rewritten from
the deleted `pipeline(sources={...})`/`@step(source=)` API to the shipped
multi-`@source`-root reality. `tests/test_run_status.py` comment reworded
off the same dead "manifest" concept. Verified against `spec.py`/
`planning.py`/`execution.py`; `rg -i manifest src tests notes docs
README.md AGENTS.md` clean outside this changelog. Commit `404ee6c`.

**2026-07-13 — sources purge (item 14):** `sources.py` deleted — ingestion
is an `@source` (parentless expand) step, full stop. The `Source` protocol,
`SourceItem`, scan/load, `FolderSource`/`CsvSource`/`TableSource`, the
`folder=`/`source=`/`sources=` kwargs, `@step(source=)` routing, and
`source_for` are gone; replacements are recipes in
`docs/concepts/sources.md` (folder/CSV/table/cloud-token). `definition()`
`source_id` = sorted root-step names. Test sweep rewrote every scanned
folder as a root `@step(shape="expand")` (AGENTS.md Test conventions has
the pattern + two recurring judgment calls: headless param-fed roots for
supersession tests and for `plan()`-preview tests). **Acceptance-line
correction found in the build:** `plan()` never previews an expand root's
enumeration — a second `plan()` stays execute+pending forever (pinned by
`test_plan.py`); it is the second *run* that reuses. Live-verified:
count_lines 22/22, newsroom join 21/21, hn_digest real LLM run. Engine
`ef31228`, tests `190b020`, examples `ee138e7`, docs `967eb4c`.

**2026-07-11 — retention GC (item 10b, byte-deleting):** `pipeline(retention=N)` (keep-last-N terminal runs) + a global `rubedo gc [--max-bytes SIZE] [--delete]` budget, dry-run by default. Two phases on existing machinery (`src/rubedo/gc.py`): **demote** live mats outside a pipeline's keep-set with paired `pruned` lifecycle rows (never a ledger delete — bytes, not facts); **sweep** object files no live mat references anywhere (the shared-object ref-count), logging each in the new append-only `object_reclamations` table. End-of-successful-run auto-prunes when `retention=` is set (skips, never errors, while another run beats); unconfigured runs get a cheap cached warn-threshold. gc refuses while any run is live (restore race). **Trap 5 resolved with evidence:** an expand cache anchor appears in neither `RunCoordinateStatus` nor `MaterializationEdge`, so the keep-set is widened structurally — an anchor *is* a live mat with zero status refs, always kept (pinned: the anchor test asserts it would be demoted without the widening). `rubedo du` now reports reclaimed vs missing; lazy heal restores a pruned lane whose input reappears. `tests/test_gc.py` + `tests/test_du.py`.

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
