# TODO

Each open item below is a self-contained spec: the design decisions are
settled (owner design sessions 2026-07-10/11/12/14 — do not re-litigate;
flag genuine contradictions), with file pointers, gotchas, and acceptance
criteria. Read `CLAUDE.md` first for conventions, and `notes/invariants.md`
for vocabulary. One item = one (or a few) commits.

Items keep their historical numbers for stable cross-references (gaps are
shipped/retired items — see the Done changelog; the simplification arc —
**14** sources purge, **15** the rotation, **16** step ergonomics, **17**
the invariants rewrite, **18** notes hygiene, **19** comment cleanup,
ascii describe, **21** `secrets=`/`env=` + `rubedo check`, **22**
shape & dependency inference, and **23** removing `@source` — shipped
2026-07-13/14). **24** is next (independent but tiny), with **25**
deferred (owner: queued, do not build until asked). The cloud
chain (**6** → **7**+**7b** → **8** → **13**) builds when multi-machine
demand is real — though **8** is independently
buildable (workers never touch the ledger/store; item 7 is its throughput
story, not a prerequisite), and **6 needs a respec post-14** (see its
note). (**10b** retention GC shipped — see the Done changelog.) Unsettled
ideas live in **Parked** at the bottom — do not build those without a
design session.

## 24. Callable `StepSpec` + `describe()` TTY default  **[design settled 2026-07-14; independent quickie]**

Two zero-concept ergonomics: (a) `StepSpec.__call__` delegates to
`self.fn(*args, **kwargs)` so a decorated step is directly unit-testable
(`extract(scan={"text": "hi"})`) — pure passthrough, the engine keeps
calling `step.fn`, no behavior change anywhere else; (b) `describe()`
picks `ascii` when no explicit `format=` is passed and stdout is a TTY,
`text` otherwise (pipes, captures) — the existing >100-column
ascii→text fallback stays, explicit `format=` always wins.

**Trap:** pytest captures stdout (not a TTY), so test-suite default
behavior must be unchanged without any test edits; don't make `Pipeline`
callable — only the step.

Acceptance: calling a decorated step invokes the underlying fn with the
same args; `p.describe()` piped emits the `text` format byte-identically
to today; in a real TTY it renders ascii (verify by hand, note it);
explicit `format=` unchanged; full verification checklist green.

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
  generations/pairing machinery unchanged. The crash-safety guarantee
  survives (`notes/invariants.md`, promise 2): a worker
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
- **Sinks** (the return leg of the refinement loop: CSV/Sheet in →
  refined batch back out; Sheets via gspread, Excel via openpyxl as
  extras, CSV/Parquet trivially). Belongs **in code, in the pipeline
  file** — settled. The open fork is **step vs verb**, and it's the
  real design session. Owner leans *step* for simplicity
  (2026-07-13): a terminal reduce that writes the target gets
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
  touches the generations protocol and the pairing guard
  (`notes/invariants.md`), and would be the
  dashboard's first write surface (**DANGEROUS** — full design
  session required, do not sketch in code) (2026-07-13).
- **Failure triage view.** Blocked/failed lanes already accumulate in
  the ledger; a first-class "these 14 rows failed, retry just these"
  surface (CLI + dashboard) turns an engine fact into a refinement
  workflow (2026-07-13).

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above; git log has the detail)

**2026-07-14 — shape & dependency inference (item 22):** Kwargs that
restate what the code already says now default from it — the engine,
planner, and ledger never know inference existed, since all three resolve
to the same explicit `StepSpec` the API already built. A generator
function defaults `shape="expand"` (`inspect.isgeneratorfunction`); an
explicit non-`"expand"` shape on a generator raises. `join_on=`/
`group_key=` otherwise default `shape` to `"join"`/`"reduce"`; an explicit
conflicting shape still raises via the existing consistency checks (now
checked against the resolved shape). An omitted `depends_on` is inferred
in `pipeline.py::_build_spec` — the one place every sibling step's name is
known, which decoration time (`spec.py::step()`) can't see: every
non-`params` parameter of the function must name a registered step and
becomes a dependency, in signature order; an unmatched parameter raises
`ValueError` naming the step, the parameter, and the available step names
(no fuzzy suggestions — item 25, still deferred); `*args`/`**kwargs`
signatures skip inference entirely (root by default); a step with no
non-`params` parameters is a root, with no special-casing. `depends_on`
also grows a dict alias form, `{"param_name": "step_name"}`, binding a
parent's output to a differently-named parameter — execution-only
(`execution.py`'s new `_dep_kwarg`), never touching planning/addressing,
which still key everything on step names. Either explicit form (including
an explicit empty list) disables inference for that step.
`_build_spec` builds a fresh resolved step list (`dataclasses.replace`)
rather than mutating the `StepSpec` objects `@step`/`@source` handed back
to callers. **Trap finding:** `_hash_source` does include decorator
lines (verified empirically: a multi-line `@step(...)` call is captured by
`inspect.getsource`), so simplifying a shipped example's decorator would
move its code hash and warn on the owner's existing stores — the terse
style is taught in docs only (`tutorial.md`, `reference/api.md`,
`concepts/shapes.md`); every shipped example keeps its explicit kwargs
untouched, `docs/concepts/sources.md`'s `@source` recipes included (that
sweep is item 23's job). Five pre-existing test fixtures used stray,
never-bound parameter names on otherwise-root steps that the new inference
correctly rejects as unmatched — fixed by dropping the unused parameter or
declaring `depends_on=[]` (`test_group_key.py`, `test_skip_cache.py`,
`test_step_ergonomics.py`, `test_tier0_fixes.py`). New
`tests/test_shape_dependency_inference.py` pins: shape inference
(generator/`join_on=`/`group_key=`, explicit conflicts), `depends_on`
inference (param-name matching, root detection, the unmatched-parameter
error, `*args`/`**kwargs` skip), the dict alias form's execution-time
binding, `definition()` byte-identical between an inferred pipeline and
its fully explicit twin, and a full-reuse re-run over an existing store.
Live-verified: store wiped, `count_lines` run twice — Created: 22, then
Reused: 22 — and the tutorial's demo pipeline re-run step by step against
a fresh folder, every printed block unchanged from before the edit. 258
tests passed (243 pre-existing + 15 new), ruff/mypy/`mkdocs build --strict`
clean. Commits `8c482db` (engine + tests), `ba5b9bc` (docs).

**2026-07-14 — remove `@source` (item 23):** After item 22, `@source`'s
entire content (`shape="expand"` inferred from `yield`) made it an honest
synonym for `@step` — deleted. `source()` is gone from `spec.py` and its
export from `rubedo.__init__` (`from rubedo import source` now
`ImportError`, not aliased — the `run()` precedent from item 15);
`Pipeline.source`/`@p.source` is gone from `pipeline.py`. "Source"
survives as prose vocabulary for a parentless root; `envcheck.py`'s AST
lint no longer special-cases a `"source"` decorator name alongside
`"step"`. Full sweep: tests (`test_expand.py`, `test_headless_root.py`,
`test_describe_ascii.py`, `test_plan.py`, `test_envcheck.py`), all nine
`@p.source`-using examples (`count_lines`, `newsroom`, `expand_feed`,
`github_health`, `orders_rollup`, `executor_showdown`,
`weather_advisory`, `gutenberg_stats`, `graphify`), and prose across
`README.md`, `docs/`, `AGENTS.md`, `notes/llms.txt`,
`notes/invariants.md`, `notes/producer-model.md`, and
`marketing/src/App.jsx`. **Trap resolved, surprising result:** the
decorator-line edit does move a step's `code_hash` (`_hash_source`
hashes decorator and all, confirmed by diffing a stored
`Materialization.code_hash` against a fresh hash of the edited
`count_lines.py::input_files`), but the sweep produced *zero*
code-drift warnings anywhere — `@source` only ever decorated root
`expand` steps, and a root's planning decision is unconditionally
`"execute"` (`planning.py::_plan_step`: "Root expand = source: no
parent to cache against, so it always executes"); the code-drift check
only ever fires on a `"reuse"` decision, which a root never produces.
Live-verified: `examples/count_lines` run twice against its existing,
already-populated store — both runs `Created: 0, Reused: 22`, no
warnings. 258 tests passed, ruff/mypy/`mkdocs build --strict`/`npm run
build` (marketing) clean. Commits `c27faa1` (core + tests), `cf59bc1`
(examples), `36a2cb9` (docs/notes/marketing).

**2026-07-14 — `pipeline(secrets=, env=)` + `rubedo check` (item 21):**
`PipelineSpec` grows `secrets`/`env` tuple fields — declarations only, zero
effect on execution locally, never entering any step's cache identity
(verified live: two `Pipeline`s differing only in `secrets=`/`env=` produce
identical reuse on the second run). Validation is eager, in
`Pipeline.__init__`, matching the `schedule=`/`retention=` precedent rather
than `_build_spec` — both are step-independent checks, so failing fast at
construction fits the same reasoning already applied to `retention`: names
non-empty, unique across the combined list (which also catches overlap
between the two lists as a self-duplicate), never `RUBEDO_*`-prefixed.
`definition()` now emits `"secrets"`/`"env"` unconditionally, even empty —
unlike `retention`, these are declarations rather than a policy toggle —
which moved `tests/test_definition_snapshot.py`'s pinned fixture (a
legitimate, additive change; the pin guards the TODO-15 hashing rotation's
byte-identity, not this). New `src/rubedo/envcheck.py` holds `rubedo
check <file.py>`'s AST logic: a best-effort walk (no import of user code,
same principle as `server.py`) that finds `pipeline(...)` calls' declared
names and `@step`/`@source`-decorated functions' `os.environ[...]`/
`os.getenv(...)` reads, warning on anything undeclared; dynamic names and
reads reached only through a helper function are silently skipped. Advisory
forever — always exits 0, never blocks or gates. Live-verified against a
scratch file: warns naming the undeclared variable, passes clean once
declared into `secrets=`/`env=`; also checked against
`examples/hn_digest.py`, which passes clean because its `os.environ` reads
live in helper functions (`_chat`/`_get`), not directly in a step body — the
static approach's known limitation, not a bug. `tests/
test_env_declarations.py`, `tests/test_envcheck.py`. Commits `b5276f8`
(engine fields + validation + definition), `7aa53b9` (`rubedo check` +
envcheck.py).

**2026-07-14 — comment cleanup pass (item 19):** src/, tests/, and
examples/ comments no longer reference TODO item numbers or narrate past
changes; constraint content (trap warnings, ordering requirements,
cross-file contracts) stays, minus its process tags, and a few
narration-shaped comments were rewritten as current-state facts (verified
against the code they describe). Owner set the style — strip tags, keep
constraints — and reviewed the full diff before it landed. Comment-only:
no example step bodies touched, so no code-drift warnings. Commit
`159e008`.

**2026-07-13 — invariants rewrite, values-first (item 17):**
`notes/invariants.md` restructured under four core promises — *never pay
twice for the same computation; never lie about what happened; order and
parallelism never change results; bytes are disposable, facts are not* —
with the former eight invariants recast as supporting guarantees
underneath (nothing user-visible changes; no behavior changed). Owner
reviewed and approved the draft (`notes/invariants-draft.md`, deleted on
ship), with one override of the draft's own "keep numbers stable"
proposal: **renumber everything under the new promise-scoped scheme**
(`promise.guarantee`, e.g. `2.6`) **and strip invariant-number references
out of code entirely** — `models.py`'s pairing-guard comment and
`ImmutabilityError` message, and `gc.py`'s demote/pairing-guard comments,
now describe the constraint in plain language and point at
`notes/invariants.md` generally rather than citing a number; matching
`pytest.raises(match=...)` regexes and comments in
`tests/test_pairing_guard.py`/`test_immutability.py`/
`test_invalidate_downstream.py` updated in lock-step. Prose swept
everywhere a number could go dangling: `AGENTS.md`, `README.md`,
`notes/retention.md`, `notes/producer-model.md`, `docs/guides/retention.md`,
`docs/concepts/model.md` (its independent "eight invariants, plainly"
paraphrase rewritten to the same four-promise structure), `docs/index.md`,
and this file. **The Parked "Generations/schema simplification" idea
dies**, per its own terms: it was gated on whether *never lie about what
happened* survived the rewrite as a core promise, and it does — the
generations schema (append-only `materialization_lifecycle`, the
`before_commit` pairing guard) is the mechanism that makes that promise
mechanically true, not incidental plumbing, so no simplification of it
ships. `rg -i "invariant [0-9]"` is zero hits in `src/`/`tests/`;
`uv run mkdocs build --strict` clean. Commits `63e0c33` (doc swap +
code/test sweep), `d660d2c` (docs-site + notes sweep).

**2026-07-13 — step ergonomics (item 16):** `@step`'s `name=`/`version=`
both got defaults — `name` falls back to the decorated function's
`__name__` (the precedent `@source` already set), `version` defaults to
`"0"`, and `code="warn"` stays the default either way (an unbumped default
version behaves exactly like a hand-picked one: edits warn on drift rather
than silently recomputing). `@step` now works bare (`@step`, no parens) as
well as called (`@step()`, `@step(version=...)`), mirroring `@source`'s
existing `fn=None`-sentinel shape; all of `step()`'s validation moved
inside the decorator closure so it can resolve the name from the function
first. Duplicate step names — the realistic new collision, since two
same-named functions in different modules now silently produce the same
step name — die at `Pipeline._build_spec` construction time (moved up from
only being caught deep in `planning.topological_sort`, which keeps its own
simpler check as a backstop for direct `PipelineSpec` construction), naming
both functions' `module.qualname`. Docs: the tutorial's first pipeline
drops `name=`/`version=` entirely (every printed block re-run live and
updated); `concepts/versioning.md` and `reference/api.md` teach the
defaults where `version`/`code` are already explained. `tests/
test_step_ergonomics.py`; `docs build --strict` clean. Commits `b82f1f1`
(engine + tests), `7b5ee1d` (docs).

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
guards + the liveness-flip pairing guard · single `run()`/`plan()` entry
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
