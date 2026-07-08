# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine contradictions),
with file pointers, gotchas, and acceptance criteria. Read `CLAUDE.md` first
for conventions, and `notes/invariants.md` for vocabulary. One item = one (or
a few) commits.

The **producer model is done** (content-addressed lanes → `expand` →
`group_key` → multi-source → N-way `join`); see the Done changelog and
`notes/producer-model.md`. What's left is grouped into the tiers below, and
items are numbered sequentially (1..12) across tiers so cross-references stay
stable. Tier 0 (bugs from the 2026-07-07 code review) uses B-letters so it
never collides with those numbers.

## Priority snapshot (recommended order — owner may reshuffle)

- **Tier 0 · Bugs & hardening** (2026-07-07 code review) — **status:
  B1–B7, H1–H3 landed** (commits 8071aab..7511491 + regression tests in
  `tests/test_tier0_fixes.py`). B2 was a **false positive** and is reverted
  — see its section. Still open: **H4** SSE event-loop blocking · **H5**
  CORS · **H7** DRY leftovers — opportunistic; **H6** packaging rides with
  item 1. Follow-up debt from the fixes, in the item sections: H2's
  missing-anchor-bytes fallback, H3's `type: ignore` count.
- **Tier 1 · Product shape & packaging** — the producer model is a natural
  "feature complete" moment; before a `pip install rubedo` push, keep the
  public surface trustworthy and the install lean: **1** dependency & packaging
  hygiene (now includes **H6**: `rubedo[server]` extra, find-directive
  packaging) · **2** read-only ops CLI (build early — a terminal view of
  ledger state is the fastest way to eyeball the output of everything else
  while building it; do **B4** and the new `pipeline:` selection term first,
  and ship failure introspection with it — see the item-2 spec).
- **Tier 2 · DX, Observability & UI** — **status: fully landed** (items 3, 4, 5, 13, 14, 15 complete).
- **Tier 3 · Scale & cloud** — a dependency chain, build when multi-machine
  demand is real, and only after **H2** (batched planning — no point
  distributing an N+1 planner): **6** cloud sources → **7** cloud
  ledger+store → **8** distributed execution; **9** lane-pipelined execution
  (independent).
- **Tier 4 · Deferred / careful** — **10** storage GC (**dangerous**) · **11**
  `expand` child-views (storage optimization) · **12** lane tooling.

══════════════════════════════════════════════════════════════════════
# Tier 0 · Bugs & hardening (code-review findings, 2026-07-07)
══════════════════════════════════════════════════════════════════════

Findings from a full read-through of `src/rubedo/` (tests green, 171
passing). Bugs are lettered B1..B7, hardening H1..H7, so the numbered items
1..12 keep their cross-references. Each bug is a small independent commit;
B1–B3 are the priority.

Items tagged **[⚠️ subtle]** touch cache identity, concurrency, or the
plan/execute interleave — read `notes/invariants.md` and the item's trap
paragraph before coding, and do not "simplify" the guarded behavior away.
Untagged items are safe, well-bounded changes.

## B1. Crash: multi-parent map step over parents with disjoint lanes  **[confirmed, repro'd]**

`_plan_step` builds a dependent map step's coordinate set as the *union* of
all parents' coordinates (`planning.py:577-588`). A coordinate missing from
one parent makes `coord_step_mats.get((coord, dep))` return `None`, which
falls through every status check (`"blocked"`/`"failed"`/`"pending"`/
`getattr(ref, "filtered", False)`) and lands in `parent_mats` as `None`;
`_compute_step_input_hash` then dies with `AttributeError: 'NoneType' object
has no attribute 'output_content_hash'` (`planning.py:131`) — an unhandled
exception that fails the *whole run*, not just the lane. Repro: two root
`expand` steps yielding different payloads + one map step with
`depends_on=["a", "b"]` — a realistic user mistake whose correct tool is
`join`, and the engine should say so. Fix in `_plan_step`: when a dep has no
entry for a coordinate, raise a clear "parents produce disjoint lane sets —
a multi-parent map step requires aligned coordinates; use shape='join'".
**Trap:** a `None` lookup is *not* the same as a `"pending"`/`"filtered"`/
`"blocked"` parent — those must keep their existing per-lane propagation;
only a truly absent `(coord, dep)` key is the error. Do not "fix" it by
silently skipping unmatched coordinates (that changes semantics into an
implicit join). Acceptance: the repro produces that message; a diamond (two
parents derived from the same source) still runs; the pending/blocked
propagation tests stay green.

## B2. ~~`httpx2` dev dependency is a typo~~  **[FALSE POSITIVE — fix reverted]**

The review got this one wrong: `httpx2` is the continuation package that
Starlette's `TestClient` now prefers (`starlette/testclient.py` does
`import httpx2 as httpx`, falling back to plain `httpx` with a
`StarletteDeprecationWarning`). The original pin was intentional. The
review-driven swap to `httpx` introduced that deprecation warning into the
suite and was reverted; `httpx2>=2.5.0` is the correct dev dependency.
Recorded so nobody "fixes" it again.

## B3. `invalidate()` commits partial flips on failure

`invalidation.py`: if the flip loop raises midway, the `except` block sets
`run.status = "failed"` and calls `session.commit()` *without rolling back*
— committing whatever `is_live` flips (with their lifecycle rows) were
already pending, under a run recorded as failed. Add `session.rollback()`
at the top of the except before writing the failure status. Acceptance: a
test forcing a mid-loop exception observes zero flipped materializations.

## B4. Selection query: duplicate IDs + N+1 coordinate lookups

`get_selection_materialization_ids` (`selection.py:96-124`): joining
`RunCoordinateStatus` (for `source:`/`coord:`) multiplies rows, so the
returned id list can contain duplicates — harmless inside `invalidate()`
(the `is_live` check dedupes) but it leaks into the API response's
`materialization_ids`. The coordinate-glob path then runs one extra query
per materialization, matching against an arbitrary "latest status row".
Fix: `.distinct()` on the join; batch the coordinate lookup. Do this
*before* item 2 — the CLI builds directly on this function.

## B5. skip_cache parents of `join`/`group_key` crash with AttributeError

`_plan_join` and `_group_reduce_lanes` read index entries via `ref.id`
(`planning.py:276,459`), but a skip_cache parent leaves an `EphemeralRef`
(no `.id`) in `coord_step_mats`. Since skip_cache steps can't declare
`index=` anyway, the combination can never work — reject it in `pipeline()`
validation (`spec.py`): a `join_on` side or a `group_key` reduce parent may
not be skip_cache. Acceptance: build-time ValueError, no plan-time crash.

## B6. `expand` can't yield bytes  **[⚠️ subtle]**

Expand children are hashed with `hash_json(value)` (`execution.py:257`, and
the planning-side identity in `expand_child_identity`), which raises
TypeError on `bytes` — yet `_serialize` (`store.py`) happily stores bytes
for every other shape. Either hash the serialized form (`_serialize` →
`hash_bytes`) so payload support matches the rest of the engine, or
document the JSON-payload constraint in the `@step` docstring and raise a
clear error at yield time. **Trap:** the child hash IS the child lane's
cache identity (coordinate, input_hash, and output_address all derive from
it — `expand_child_identity`), so switching the hash function for
already-JSON-able payloads silently changes every existing expand child's
address and orphans the whole cache. Dev-stage rules allow a cache reset
(say so in the commit and follow the DB-reset ritual in CLAUDE.md), but the
*cheap* fix — keep `hash_json` for JSON values, add a labeled bytes branch
(e.g. `hash_bytes` prefixed so bytes/JSON can't collide) — preserves
identity for existing pipelines. Prefer that unless the owner says
otherwise. Acceptance: expand yielding bytes round-trips (run twice →
Reused), and a pre-existing JSON expand cache still reuses.

## B7. Minor correctness cleanups

- Unreachable code: the post-loop "Retries exhausted." return in
  `execution.py:346-354` can never run — the final attempt always returns
  inside the loop (`retryable` is False once `attempt > step.retries`).
  Delete it.
- `_finish_run` (`ledger.py:602-607`) marks a run "failed" when
  `created == reused == 0` even if lanes were successfully `filtered` — a
  filter-heavy run with one failure misreports. Count filtered as success
  in the status decision.
- Dead `_to_dict(m)` call in `preview_selection` (`server.py:517`).

## H1. `_RunMemo` serializes all skip_cache execution  **[⚠️ subtle]**

`compute()` (`execution.py:83`) holds a single RLock *while running the
producer*, so every ephemeral computation across all worker threads runs
one-at-a-time — quietly defeating `workers=` for any consumer of a
skip_cache util. Move to per-key locking. **Traps:** (1) chained skip_cache
utils resolve *recursively on the same thread* (`_compute_ephemeral` →
`_resolve_parent_value` → `_compute_ephemeral`), which the current RLock
permits — a naive per-key non-reentrant lock must only ever be held for a
*different* key when recursing (true today: dependencies form a DAG, so
recursion never re-enters the same key; verify, don't assume). (2) The
"compute at most once + memoize exceptions" contract must survive: use the
once-per-key primitive pattern (a per-key `threading.Event`/future stored
under a short-lived dict lock, producer runs *outside* the dict lock) —
never double-checked locking on a bare dict. (3) Exceptions stay memoized
so every consumer of a failed util sees the same failure. Acceptance: a
test with two lanes consuming two *different* ephemeral coords observes
overlapping execution (e.g. via a barrier), `tests/test_skip_cache.py`
stays green.

## H2. Planning is N+1 on the ledger  **[⚠️ subtle]**

One live-materialization query per lane per step (`_plan_step`), one per
child in `_plan_expand_reuse`, one per lane per field in group/join
planning. Fine at 15 lanes; painful at 50k CSV rows. Addresses are
computable up front, so a batched `output_address IN (...)` per step (and
one index query per step for group/join) is a straightforward win — and far
cheaper than any Tier-3 scaling work. Do before Tier 3. **Traps:** this is
a pure query-batching refactor — decision *semantics* must not move an
inch: staleness (`stale_after` reads `refreshed_at or created_at`),
code-drift flags, force, filtered-reuse, and the expand anchor→children
sequence (the anchor must be read first; its child list *then* determines
which addresses to look up — that lookup can batch, the anchor read can't
fold into the same query) all stay byte-identical. Planning is interleaved
with execution per step (`runner.py` loop) and `coord_step_mats` mutates
between steps, so batch *within* one `_plan_step` call only — never across
steps. Keep `_plan_step` read-only. Acceptance: full suite green with zero
test edits; a quick benchmark script (1k-row CSV) shows plan queries
dropping from O(lanes) to O(steps).

**Status: landed** (commit 7f01030), reviewed faithful. One follow-up:
the batched expand path treats an anchor whose object bytes are *missing
from the store* as an empty expansion (`read_materialization_output` →
`None` → falsy → zero children planned) where the old code crashed loudly.
Store corruption should fall back to re-running the expansion (the
incomplete-cache path), not silently plan nothing.

## H3. mypy exempts the core modules

`pyproject.toml` sets `ignore_errors` for `rubedo.models`, `planning`,
`invalidation`, `server`, `ledger`, `runner` — the "typing pass" in the
Done changelog covers everything *except* the modules that matter most.
Burn the overrides down module by module. Caution: annotate, don't
restructure — `coord_step_mats` is genuinely heterogeneous (MatRef |
EphemeralRef | status-string sentinels); give it an honest union type
rather than "cleaning up" the sentinel design to satisfy the checker.

**Status: landed** (commit 7511491) — the overrides are gone and mypy is
clean, but largely via ~60 `type: ignore` comments (23 in `planning.py`
alone). Follow-up debt: replace the ignores with real types where a
union/`TypeAlias` would do; each ignore is a spot the checker is switched
off.

## H4. `stream_run` blocks the event loop

The SSE generator (`server.py:120`) runs synchronous SQLAlchemy queries
inside `async def`, freezing all other requests for the duration of each
poll. Make it a sync generator (Starlette threads those) or push the
queries through `run_in_executor`.

## H5. CORS config is invalid and permissive

`server.py:48-55`: `allow_origins=["*"]` with `allow_credentials=True` is
rejected by browsers per the CORS spec, and the state-changing invalidate
endpoint sits behind it. Pin to the Vite dev origin; drop credentials.

## H6. Packaging leanness (extends item 1)

`fastapi`/`uvicorn` are core dependencies but only `server.py` imports them
— move to a `rubedo[server]` extra (same pattern item 1 plans for cloud
extras). Also `packages = ["rubedo"]` won't include future subpackages;
switch to the setuptools find directive.

## H7. Small DRY / N+1 leftovers

`_ensure_gitignore` is duplicated in `db.py` and `store.py`;
`get_pipelines_api` (`server.py:557`) re-queries the latest run per
pipeline instead of one grouped query.

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

**Additions from the 2026-07-07 review (they ride the same read-query
layer, so build them here):**
- **`pipeline:` selection term.** `Selection` has no pipeline filter, so
  `invalidate(step:extract)` cross-hits every pipeline sharing a step name.
  The column already exists on `Materialization`; add a `pipeline_id` field
  to `Selection`, a `pipeline:` prefix to `Selection.parse()`, and the
  filter in `get_selection_materialization_ids`. Load-bearing before the
  CLI's `invalidate` ships. Fix B4 first (same function).
- **Failure introspection.** `RunSummary` carries counts only — finding
  *which* coordinates failed and why takes raw SQL or the web UI today. Add
  a `failures(run_id)` read-query (coordinate, step, error_type, message)
  to the shared layer, surfaced as `rubedo show <run> --failed` and as a
  `RunSummary.failures()` accessor so script-driven retry loops are natural.

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

UI enhancements (live run view animations, pipelines page drill-down and last-run details, rich JSON viewer for materialization payloads) · Terminal progress feedback (`run(progress=True)`) · pipeline-level `params_model` validation · partial fan-in policy (`on_failed="use_passed"|"block"`) · Dependency hygiene: `litellm` moved from core `dependencies` to the `dev`
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
