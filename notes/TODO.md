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
a settled problem statement and a recommended spec but an unratified
fix — one line from the owner unlocks them; do not build first.
Unsettled ideas live in **Parked** at the bottom — do not build those
without a design session.

Items 29–34 come from the 2026-07-18 full-codebase review; every
finding and every sub-decision inside the specs (grouping keys, fix
mechanisms, blast radii) was re-verified against source on 2026-07-18
before being written down.

**Priority order:** review items 29–34 are all shipped. The cloud chain
(7 → 7b → 8 → 13) stays demand-gated — 8 is independently buildable if a
cluster user shows up first.

──────────────────────────────────────────────────────────────────────

## 25. Did-you-mean suggestions  **[DEFERRED — owner 2026-07-14: queued, do not build until asked]**

`difflib.get_close_matches` on the loud errors: item 22's unmatched
parameter names, unknown `depends_on`/`join_on` step names, unknown
`Selection` fields, CLI step/pipeline arguments. Small, self-contained;
waits for the owner's go.

## 6. Cloud object storage sources  **[RETIRED 2026-07-18 — shipped as a recipe]**

Item 14 deleted the `Source` protocol this item's `S3Source`/`GCSSource`
classes would have implemented, and the surviving design — LIST-only
enumeration, cheap `(key, etag, size)` change tokens, a downstream
cached step doing the GET — shipped as the documented recipe in
`docs/concepts/sources.md` § "Cloud object storage (S3/GCS)". There is
nothing left to build in the engine: the recipe's containment property
(a churned token recomputes one lane = one re-download) falls out of
content-addressed lanes. The old trap list (etag quote-stripping, LIST
pagination) dissolved with the classes — in recipe form the raw etag
string is payload data, consistent by construction. A moto-tested
example pinning "re-run reuses with zero GetObject calls" is Parked.
Full original spec in `notes/TODO-obsolete.md`.

## 7. Configurable cloud ledger + object store (Postgres / S3-GCS)  **[respecced 2026-07-18; all planes settled]**

Distinct from the retired item 6 (input data) — this is the *internal*
storage that backs every run. This — **not** the execution backend — is
the real prerequisite for genuine multi-machine/cloud execution
(item 8): a distributed deployment can't share a purely local SQLite
file + objects dir. Post-Arrow-rewrite there are **three planes**, not
two:

1. **Ledger DB** (`src/rubedo/db.py`) → Postgres via URL.
2. **Object store** (`src/rubedo/store.py`, spilled values + payloads)
   → S3/GCS behind a protocol.
3. **Lane store** (`src/rubedo/lane_store.py`, Arrow IPC files under
   `tables/<pipeline>/<step>.arrow`) — **new since the original spec.**
   This is now the content plane: reuse reads it, so a shared
   deployment must share it. The original spec predates its existence.

**Settled decisions (owner design session 2026-07-11, re-verified
against current code 2026-07-18 — do not re-litigate):**

- **`ObjectStore` protocol** (exists / read / write / delete, write
  carrying conditional-put semantics) with `LocalStore` + `S3Store`
  implementations; `store.py`'s module functions become thin delegates to
  a process-global store instance, so every call site (ledger, planning,
  execution, du, server) stays untouched. GCS rides the same protocol
  later. `client_factory=` pattern for endpoints/tests (fsspec was
  rejected: a heavyweight layer for four methods).
- **Staging is a local concept.** `LocalStore` keeps the staging dir +
  fsync + atomic `os.replace`; `S3Store` uploads directly with a
  conditional put (`If-None-Match: "*"`; GCS `ifGenerationMatch=0`) —
  a bucket upload is already atomic, so no staging keys exist and
  `cleanup_staged` is a cloud no-op.
- **Config = two URLs, no new concepts.** `RUBEDO_DB_PATH` already takes
  a URL (fix the mangling first — `db.py:38` wraps any non-`sqlite:///`
  string in `sqlite:///`, so a `postgresql://` URL silently becomes a
  SQLite file path; anything containing `://` must pass through
  verbatim, and the makedirs/`_ensure_gitignore` logic must skip URL
  targets — the module docstring currently overpromises here). Add
  `RUBEDO_STORE_URL` (`s3://bucket/prefix`) + a `store=`
  param on `run()`/`plan()` with the same explicit-param-over-env
  precedence `home=` has. `home=` itself stays local-only. WAL/
  `busy_timeout` pragmas stay SQLite-only (already conditional).
- **Tests: SQLite + moto only for now** (owner call). `S3Store` is
  moto-tested in the always-run suite; real-Postgres correctness is
  deferred to **item 7b**.

**Lane-store plane (ratified by owner 2026-07-18):** cloud mode writes
each buffered flush as its own immutable object
(`tables/<pipeline>/<step>/<flush-uuid>.arrow`) — object stores don't
append, but the lane store is already append-only batches flushed at
segment boundaries, so flush-per-object maps 1:1. The reader lists the
prefix and concatenates, **deduping rows by `row_id`**, so a listing
that races a compaction (sees the new base *and* not-yet-deleted
segments) is harmless by construction. **Compaction is writer-side —
no queue, no service:** at end of run, if a step's prefix exceeds a
segment-count threshold, the runner reads base + segments, writes a
new merged base via conditional put, then deletes the consumed
segments — file count stays bounded at one base plus the flushes since
last compaction. The bucket prefix *is* the durable queue (segments
are the messages, deletion is the ack); a separate queue service would
hold the same durable bytes twice and violate the zero-daemon
positioning — if the hosted control plane later wants a background
compactor, it consumes this same segment format. **Single-active-writer
lease per pipeline** via a conditional-put marker object (the cloud
analog of the local run heartbeat) — needed for concurrent
same-pipeline runs regardless, and it makes compaction race-free
(the sole writer compacts its own prefixes). Local layout stays the
single append file — zero change for the normal case.

**Trap (part of the spec, re-derived 2026-07-18):** **(1) The old
dialect trap is gone — don't chase it.** The original spec's
`sqlite_where` partial-index warning died with the `materializations`
table; `models.py` has **no** dialect-conditional DDL today (verified).
The dialect-sensitive machinery is now the IHU claim/fulfill UPDATE
lifecycle, the retry-once IntegrityError commit-collision path, and the
ORM immutability listeners — 7b covers them on real Postgres. **(2) 412
is success** — a conditional put failing with PreconditionFailed means
the object already exists: map it to the same idempotent early-return
the local exists-check takes, never an error. **(3) Missing reads
return None** — absent objects must read as `None` (du counts them as
missing); `S3Store.read` must map `NoSuchKey` to `None`, never raise.
**(4) Path stragglers** — `gc.py` imports `_get_object_path` directly,
and the server download endpoints build local paths; grep for direct
`objects/` path construction and move every call site behind the
protocol before calling it done. **(5) The lane store's read cache**
(`drop_table_cache` and friends, see `benchmarks/README.md`) assumes
local-file mtimes; the cloud reader needs its own invalidation story
(list-prefix etag set is the natural key).

Acceptance: `examples/count_lines` run twice with `RUBEDO_STORE_URL`
pointed at a moto/MinIO bucket → identical Created/Reused counts,
statuses, addresses, and lifecycle rows to the local run; a second
process against the same bucket + Postgres DB fully reuses the first's
outputs (the multi-machine story, simulated locally); a Postgres
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
under concurrent runs; the IntegrityError retry-once commit-collision
path; ORM immutability guards raising on update/delete; a
`queries.py`/selection smoke pass.
Also add the verification note to `AGENTS.md`: touching
`db.py`/`models.py` ⇒ run the PG suite (`docker run postgres` +
`RUBEDO_TEST_PG_URL=...`).
Acceptance: full suite green both with and without `RUBEDO_TEST_PG_URL`
set; the CI postgres job green on real Postgres.

## 8. Pluggable execution pools (bring-your-own cluster)  **[design settled 2026-07-11; independently buildable]**

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

## 13. Pass-by-reference payloads (workers talk to the store directly)  **[depends on items 7 + 8; respecced 2026-07-18 — scope shrank to spilled values]**

> **2026-07-18 premise update:** inline Arrow outputs changed the
> economics this spec assumed. Most outputs never touch the object
> store — they travel inside the `MatRef` the runner already holds, so
> "the runner is a byte hub" is now only true for **spilled** values
> (`output` = `"objects:<hash>"` ref strings, `execution.py`'s
> `_resolve_parent_value` → `read_output`). Refs therefore engage
> **per-parent, only when the parent's output is a spill ref**, and on
> the write side **only when the worker's result would spill** (the
> shim spills it store-side and returns the ref + metadata; small
> results return by value and the runner writes them inline as today).
> The payoff is real for blob-heavy pipelines (images, big frames) and
> zero for inline-only ones — build this last, on demonstrated demand.

**Settled decisions (owner design session 2026-07-11 — surviving parts;
do not re-litigate):**

- **Activation is automatic — no per-step knob.** Refs engage when the
  store is non-local AND the step's executor crosses a process boundary
  (`"process"` *or* an item-8 factory pool) AND the parent value /
  result is spilled per the premise update. One escape hatch:
  `run(payload_refs=False)` forces hub routing for the whole run.
- **Credential-less workers degrade, never fail.** Before the first ref
  submission per (pool, run), the runner submits a cheap probe task (the
  shim attempts a store access check worker-side). On failure: warn once
  (run event + `UserWarning`) and route that pool by value for the rest
  of the run. Don't probe per lane; don't cache across runs (credentials
  change).
- **Mechanism = a shim wrapping the fn.** The engine submits
  `_ref_call(store_config, refs, fn, …)` instead of `fn`; worker-side the
  shim GETs and deserializes spilled inputs, calls `fn`, and for a
  spill-worthy result serializes + hashes + conditional-PUTs it,
  returning only `(ref, content_type, size_bytes, …)`. The pool contract
  stays plain `.submit()` — item 8's seam untouched; store config
  travels via the picklable `client_factory` pattern.
- **Ledger commit stays main-thread**, from the returned metadata, via a
  commit variant that skips byte staging (the object is already in the
  store) but runs the full commit machinery unchanged. Crash-safety
  survives: a worker dying mid-PUT leaves at most an unreferenced object
  at a content-addressed key and no ledger row; a retry lands
  idempotently (item 7's 412-is-success).
- **Shapes: `map`/`aggregate`/`fold`/`join`; `expand` stays by-value**
  (the expand path mints coordinates and the anchor main-side).
- **Ephemeral parents stay by value.** `EphemeralRef`/skip_cache outputs
  aren't in the store by definition; refs are per-parent, so mixed
  submissions are the normal case, not an error.

**Trap (part of the spec):** **(1) The main-side value consumers.**
Output validation, data-quality `assertions`, and `Filtered` verdict
detection all touch the result value between `call()` and commit; under
refs the runner never sees spilled bytes, so each moves into the shim
(assertion callables travel with the submission; verdicts return in the
metadata) — grep everything that touches `result` between call and
commit and account for every consumer before shipping. **(2) One
hasher.** The shim must call the *same* serialization/hash/spill code
the runner uses (import, never copy), or identical values land at
different addresses and break dedup — the spill threshold decision
especially must be one function. **(3) Missing objects worker-side**
are a normal step failure with a clear error, never a silent None
payload. **(4) `size_bytes`** in the shim's metadata must equal what
the store reports, since du sums the ledger.

Acceptance: with a moto/MinIO store and the suite's fake factory pool, a
pipeline whose parents are spilled completes with **zero payload GET/PUT
calls by the runner** for ref-routed steps (assert by instrumenting the
runner-side store client), and its ledger rows, statuses, and addresses
are byte-identical to the same pipeline run with `payload_refs=False`;
an inline-only pipeline never engages refs (assert zero shim
submissions); a credential-less pool warns once and completes correctly
by value; an aggregate over N spilled parents fetches all N worker-side;
`expand` pipelines are untouched; a worker killed mid-PUT leaves no
ledger row and the re-run heals.

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
- **Moto-tested S3 recipe example** — an `examples/` folder for the
  `docs/concepts/sources.md` cloud recipe, pinning "re-run reuses with
  zero GetObject calls" against a moto bucket (the acceptance test the
  retired item 6 would have had). Small; waits for cloud-user demand.
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

- **2026-07-20 — item 34 shipped (Home injection, end-state):**
  `Home` owns `Database` + `LocalStore` + `LaneStore` for one root;
  `pipeline(home=Home(...))` injects it (path strings raise TypeError).
  Process-global `_init_home` / one-home-per-process guard deleted —
  concurrent different homes in one process are correct by construction
  (intern-by-abspath; tests in `tests/test_home_concurrency.py`).
  `_RunContext` / `_RunMemo` / `RunSummary` carry the home; planning,
  ledger, gc/du/trace/invalidate, CLI, and `create_app(home=)` follow.
- **2026-07-18 — item 33 shipped (the address salt):**
  compute_output_address gained a required `pipeline` parameter,
  appended as the always-last labeled segment; five planning.py call
  sites (two via the expand_anchor_address/expand_child_identity
  helpers, threaded from planning + execution). Lane keys and
  input_hash stay pipeline-free (pinned by test); gc.py untouched —
  sweep still refcounts content hashes globally. Four tests in
  tests/test_cross_pipeline_liveness.py cover invalidation, retention,
  lane-key equality, and shared-bytes survival. Two crash-recovery
  tests were "recovering" under a different pipeline name — exploiting
  exactly this bug — and now recover under the same name. Address
  formula updated in README/invariants/AGENTS/model.md; dev-stage
  reset ritual performed (Created: 22 → Reused: 22).
- **2026-07-18 — item 31 shipped:** declarative p.join()/p.union() now
  validate at declaration — join_on needs >=2 parents, union >=1 —
  raising step()-style ValueErrors instead of failing later inside
  planning. Three tests in tests/test_declarative.py; spec.py untouched.
- **2026-07-18 — item 30 shipped:** /api/current-outputs now groups by
  (pipeline_id, step_name, source_id, coordinate) — one row per (step,
  lane), no cross-pipeline collapse. Plus a second bug found while
  testing: the response's pipeline_id/step_name came from the shared
  Arrow row's metadata (content-addressed, so colliding pipelines showed
  whichever wrote first); now read off the authoritative
  RunCoordinateStatus row. Two regression tests in tests/test_api.py;
  live count_lines serves 22 rows (7x3+1). Item 33's address salt will
  remove the underlying Arrow-row sharing too.
- **2026-07-18 — item 32 shipped (docs reconciliation, wider than
  specced):** invariants.md fixed (Arrow schema now lists
  `output`/`output_identity`; IHU keyed by address alone; inline values
  no longer "(future)"); README + AGENTS.md now say the UI is read-only
  but the API has the unauthenticated local-use invalidate endpoint.
  Plus a fourth drift found during verification: "sources re-run every
  run" was false everywhere — roots are anchor-cached
  (tests/test_expand.py pins it) and `check_cache=False` is the rescan
  opt-in, but docs/concepts/sources.md's recipes omitted it (a
  folder/CSV/SQL/S3 source as documented would never notice new items).
  All recipes and prose fixed across sources.md, README, AGENTS.md,
  invariants.md; the fixed-list root case documented as the one that
  correctly stays anchored.
- **2026-07-18 — item 29 shipped:** expand-table Arrow rows now record
  the creating run's id — `_process_decision` gained `run_id` beside
  `pipeline_id` (scheduler passes `ctx.run_id`), the batch writes it,
  the false "filled by ledger" comment is gone. Regression test in
  `tests/test_expand_table.py` pins both expand paths to the real run
  id (verified to fail on the unfixed engine); the server's
  `created_by_run_id` fixed transitively.
- **2026-07-18 — owner ratifications (design session):** item 33
  settled as the **address-salt** variant — a `pipeline:<name>` labeled
  segment in `compute_output_address`, chosen over the composite
  `(pipeline_id, address)` key after tracing that no pipeline identity
  enters the `input_hash` chain (content-derived all the way to the
  roots) and that the salt scopes all four address consumers with a
  five-call-site diff; composite key, root-salting, and global shared
  cache recorded as rejected. Item 7's lane-store plane settled:
  flush-per-object + **writer-side run-end compaction** (bounded file
  count; the bucket prefix is the durable queue — owner's queue idea
  absorbed without a service dependency) + single-active-writer lease
  per pipeline + reader dedup by `row_id`. Both items dropped their
  [needs owner decision] tags; 29–34 are now all build-ready.
- **2026-07-18 — reprioritize + respec pass:** explicit priority order
  (29 → 32 → 30 → 31 → 34 → 33); item 30's grouping key settled from
  the UI's own columns; item 29's fix mechanism settled
  (`_process_decision` gains `run_id` beside the existing
  `pipeline_id`); item 33 firmed into a recommended spec (composite
  `(pipeline_id, address)` IHU key, global sweep preserved, nine-file
  blast radius enumerated); item 34 split into a settled guard slice +
  a parked end-state; item 6 **retired** (the recipe shipped in
  `docs/concepts/sources.md` — nothing left to build); item 7 respecced
  around the three storage planes (the Arrow lane store is the new one;
  flush-per-object recommendation awaiting ratification) with a
  re-derived trap list (the old `sqlite_where` trap is gone — verified
  no dialect-conditional DDL remains); item 13's premise updated for
  inline outputs (refs only pay for spilled values — demand-gated).
- **2026-07-18 — `fold` (item 28 Phase 2):** `in_shape="fold"` shipped —
  streaming accumulator, aggregate-identical caching/planning/ledger,
  coordinate-sorted execution, `fold_init` required + JSON-validated +
  snapshotted, unary (one parent), `arrow_aggregate` rejected.
  `tests/test_fold.py`. Item 28 is fully shipped (Phase 1 landed
  2026-07-17 as `in_shape`/`out_shape` + `aggregate` rename).
- **2026-07-18 — TODO restructure:** old TODO archived verbatim to
  `notes/TODO-obsolete.md`; items 26 (retired) and 27/28 (shipped —
  27 minus the `spills=` valve, now Parked) dropped from the live list;
  items 29–34 added from the re-verified codebase review.
