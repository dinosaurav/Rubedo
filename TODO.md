# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine
contradictions), with file pointers, gotchas, and acceptance criteria.
Read `CLAUDE.md` first for conventions, and `docs/invariants.md` for
vocabulary. One item = one (or a few) commits.

──────────────────────────────────────────────────────────────────────

## 0. Trim pass  **[owner-approved removals — do this first]**

All verified dead (zero consumers) or explicitly approved. Suggested as
three commits: (a) engine/schema trims, (b) metadata-filtering removal,
(c) dashboard + stale examples. Address format and schema columns change
→ reset `.batchbrain/` per CLAUDE.md and note it in the commit.

**(a) Dead engine/schema code:**
- Remove `@step(config=...)` entirely: the `config` param and
  `StepSpec.config`/`config_hash` fields (spec.py), the `config_hash`
  component of `compute_output_address` (hashing.py) and its call site
  (planning.py — also drop `"config"` from the skip_cache EphemeralRef
  identity dict), `config` in `definition()` (spec.py), and the
  `code_version`-adjacent `config_hash` column on Materialization
  (models.py) plus its writes in `ledger._commit_materialization`.
  Rationale: v1 residue; runtime knobs are `params`, behavior-change
  identity is `version`/`code="auto"`. Update the README "a step consumes
  up to three things" paragraph (now two: data, params) and the
  invariants.md output-address definition, and the CLAUDE.md address
  formula line.
- Remove `Selection.coordinates` (field + query branch, selection.py).
- Remove `Selection.output_content_hash` (field + query branch) and the
  `content:` term in `Selection.parse` (+ docstring). `address:` covers
  exact-output selection.
- Remove `Manifest.manifest_hash` (models.py) and its computation in
  `ledger._snapshot_source` (the `sorted_items`/`manifest_data` block
  exists only for it — delete all of it).
- Remove `ManifestEntry.size_bytes`/`mtime_ns` columns and their writes
  in `_snapshot_source`. Keep `SourceItem.metadata` (csv line /
  key_collision still use it).
- Remove `previous_output_address`/`previous_materialization_id` from
  RunCoordinateStatus (models.py), the `last_rc` N+1 query + fields in
  `_snapshot_source` (ledger.py), and the two fields in
  `RunCoordinateStatusOut` (schemas.py). Update the assertion in
  tests/test_run_status.py (~line 179) that reads them — the removed-row
  behavior itself is unchanged.
- Remove `SelectionPreviewItem.coordinate` (always None — schemas.py +
  the server dict) AND `coordinate_count` from SelectionPreviewResponse
  (always equals materialization_count). Update tests/test_api.py's
  `coordinate_count` assertion and check web/src for usages of either.

**(b) Metadata *filtering* removal (aggressive — approved).** Metadata
storage and display are untouched (`ProcessResult.metadata`,
`metadata_json`, preview/UI display, `index=` remains the search
channel). Remove only the query path:
- `MetadataFilter` class, `Selection.metadata` field, and the entire
  Python-side metadata filtering loop in
  `get_selection_materialization_ids` (selection.py). NOTE: the
  coordinate-glob Python-side filtering in that same loop STAYS — only
  the metadata branch goes.
- `meta.*` terms and `_coerce_literal` in `Selection.parse` (+ docstring
  and the language hint text in SelectionBuilder.tsx).
- SelectionBuilder.tsx: the metadata key/op/value form section and its
  state (`metaKey`/`metaOp`/`metaValue`, `addMetaFilter`,
  `removeMetaFilter`, `selection.metadata`).
- Tests: remove/replace the metadata-selection test in test_engine.py
  (~line 190, `Selection(source_id=..., metadata=[...])`) and the meta
  cases in test_selection_language.py. test_api.py's preview *display*
  assertion (`items[0]["metadata"]["line_count"]`) stays — display is
  kept.
- Docs: invariants.md "Searching" entry becomes two channels (lane keys,
  indexed fields); README search paragraph likewise.

**(c) UI + examples:**
- Delete the Dashboard page: web/src/pages/Dashboard.tsx, its import/
  nav-link/route in App.tsx; route "/" now renders Runs. Keep the api.ts
  fetchers (shared).
- Delete examples/simple_process.py, examples/dag_pipeline.py,
  examples/test_invalidation.py (redundant with count_lines.py and the
  planned llm_enrich.py). Update the examples list in
  PROJECT_CAPABILITIES_AND_STRUCTURE.md.

**Acceptance:** full suite green, ruff clean, `tsc -b` clean, DB reset +
count_lines runs twice (14 created then 14 reused), a `Selection.parse`
round-trip via the API still works, and README/invariants/CLAUDE.md no
longer mention config=, metadata filters, or the removed fields.

──────────────────────────────────────────────────────────────────────

## 1. Fan-in / reduce steps  **[design settled — build it]**

**Goal:** `@step(name="combine", version="1", depends_on=["grade"],
shape="reduce")` — one output computed from *all* lanes of its parents
("combine the sheets"). v1 is full fan-in (N→1) only; grouped reduce
(N→k, `by=`) is explicitly deferred.

**Decisions:**
- New `StepSpec.shape: str = "map"` (`map` | `reduce`), param `shape=` on
  `@step` (spec.py). Reject `shape="reduce"` combined with `skip_cache`.
- The reduce step's single lane key is the literal string `"@all"`.
- Step function contract: each dep parameter receives a dict
  `{lane_key: value}` over that parent's surviving lanes, e.g.
  `def combine(grade): ...` where `grade == {"a.txt": {...}, "b.txt": {...}}`.
  `params` works as for map steps.
- **input_hash** = `hash_json({dep: {lane: parent_content_hash, ...}, ...})`
  with lanes sorted — the lane *set* is part of identity, so adding or
  removing a lane recomputes the reduce.
- **Upstream state semantics:** filtered parent lanes are *excluded* from
  the input dict and the hash (filtered = "not part of the dataset").
  Any parent lane failed or blocked → the reduce is `blocked`
  (`blocked_parents` metadata should name the lanes, not just the step).
  In `plan()`: any parent lane pending → the reduce is `pending`.
  Zero surviving lanes → still runs with empty dicts (an empty dataset is
  a valid dataset).
- Downstream of a reduce: ordinary map steps over the single `@all` lane —
  no special handling needed (verify with a test).
- Lineage: one `MaterializationEdge` from every surviving parent
  materialization to the reduce's materialization (dedupe by parent id —
  `_materialized_ancestors` already returns a dict keyed by id).

**Implementation notes:**
- `planning._plan_step` currently loops `scanned_items`; a reduce step
  instead produces exactly ONE decision. Branch early:
  `if step.shape == "reduce":` collect
  `{coord: ref for (coord, dep_step), ref in coord_step_mats.items() if dep_step == dep}`
  per dep; apply the state semantics above; emit a single `StepDecision`
  with `coordinate="@all"` and `parent_mats[dep] = {lane: ref}` (note the
  type widens for reduce decisions — map decisions keep `dep -> ref`).
- `execution._execute_step`'s `call()`: for reduce decisions build
  `kwargs[dep] = {lane: _resolve_parent_value(ref, ...) for lane, ref in ...}`
  (ephemeral parents of a reduce thereby resolve lazily — memo makes this
  cheap; this is legal, no need to reject utils upstream of a reduce).
- `ledger` needs no protocol changes — the reduce materialization commits
  like any other; only the edge loop must handle the nested parent_mats
  shape (flatten before `_materialized_ancestors`).
- `_snapshot_source` removal detection skips reduce steps (like skip_cache):
  `@all` is not a source lane. Add `if step.shape == "reduce": continue`.
- `describe()`/`definition()` in spec.py: include `"shape": "reduce"` in
  the step entry when non-map; DagView can render it with a distinct badge
  (small follow-up, don't block on it).

**Tests (new file `tests/test_reduce.py`, fixture from test_index.py):**
1. 3 files → map step → reduce → assert single `@all` materialization whose
   value saw all 3 lanes; N lineage edges.
2. Cache: rerun → reduce reused. Change ONE file → reduce recomputes
   (lane hash changed). Add a file → recomputes (lane set changed).
3. Filtered lane: filter step upstream; filtered lane absent from reduce
   input; un-filtering (content change) recomputes the reduce.
4. Failed lane → reduce blocked, `blocked_parents` metadata present.
5. Downstream map step off the reduce works and caches.
6. `plan()`: fresh state shows reduce as pending; cached state as reuse.
7. Registration: `shape="reduce"` + `skip_cache=True` raises;
   `shape="banana"` raises.

──────────────────────────────────────────────────────────────────────

## 2. TableSource (SQL rows)

**Goal:** `TableSource(engine_url, table="leads", key="id")` — rows of a
SQL table as lanes; the "crucially rows in a table" case.

**Decisions:**
- Constructor: `TableSource(url, *, table, key, columns=None)`. `key` is a
  required kwarg exactly like CsvSource (str or list). Payload = row dict.
- `content_hash = hash_json(row_dict)` (stringify non-JSON scalars:
  Decimal→str, datetime→isoformat, bytes→hex; write one `_jsonable(row)`
  helper).
- **source_id must not leak credentials**: build it from
  `dialect+host+database+table+key` (e.g. `table:postgresql/mydb/leads#key=id`),
  never the raw URL.
- Duplicate keys: same policy as CsvSource — identical (key, content)
  collapses; different content gets `#hash[:6]` suffix. **Refactor**: pull
  CsvSource's grouping logic into a module-level
  `_disambiguate(rows: list[tuple[base_key, content_hash, ref, meta]])`
  helper and use it from both sources.
- Scan reads all rows via a fresh SQLAlchemy engine per scan (dispose
  after). Incremental scans via updated_at column: deferred, note only.

**Tests:** use a temp SQLite file as the "remote" DB (fixture creates a
table with rows). Cover: scan/coordinates/load; row edit → only that lane
recomputes end-to-end; duplicate-key disambiguation; credential-free
source_id (assert password not in id when url contains one).

──────────────────────────────────────────────────────────────────────

## 3. CPU-bound parallelism — `@step(executor="process")`

**Decisions:**
- `StepSpec.executor: str = "thread"` (`thread` | `process`).
- Registration-time validation for `process`: the fn must be picklable —
  reject when `"<locals>" in fn.__qualname__` (closures/test-local defs)
  with a clear message ("process-executor steps must be module-level").
- **Parent-side orchestration, child runs the bare fn**: in
  `execution._execute_step`, when `executor == "process"`, build
  args/kwargs in the parent (so `_resolve_parent_value` / ephemeral
  resolution and `_build_step_params` stay parent-side; the resulting
  values must pickle — document this), then submit `step.fn` itself to a
  `ProcessPoolExecutor`. Retries and the rate limiter stay in the parent:
  acquire the limiter before each submit; on failure, resubmit per the
  retry policy. The existing `process()` closure logic can be reshaped so
  the attempt loop wraps "submit + future.result()" for the process case
  and the direct call for the thread case.
- One pool per step execution (same lifecycle as the current
  ThreadPoolExecutor), `max_workers=workers or step.workers`.
- `Filtered`/`ProcessResult` returns work unchanged (they pickle).

**Tests:** module-level fn steps in the test file (they must be, that's the
point). Cover: results correct across the pool; registration rejection of
a local fn; retries still counted (module-level fn that fails via a
tempfile-based counter — closures can't cross processes); pickling error
surfaces as a normal step failure, not a crash.

──────────────────────────────────────────────────────────────────────

## 4. Cross-process concurrency safety

**Problem:** two simultaneous runs (separate processes) can both pass the
live-materialization existence check and collide on the
one-live-per-address partial unique index at commit.

**Decisions:**
- SQLite hardening in `db.init_db`: enable WAL and busy_timeout via an
  `sqlalchemy.event.listens_for(engine, "connect")` hook —
  `PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;`.
- `ledger._commit_materialization`: wrap the new-row INSERT/flush in
  try/except IntegrityError → rollback → re-query the live row at the
  address → apply the existing bytes-equality rule (identical → return
  (live, "reused"); different → demote it and retry the insert ONCE; a
  second failure propagates). Keep it a small loop, not recursion.
- The run-level `Run` insert and status writes are already per-run-id and
  cannot collide.

**Tests:** true multi-process tests are flaky; simulate instead — insert a
conflicting live materialization at the same address from a second session
between plan and commit (monkeypatch `stage_and_commit` to do the insert
before returning), then assert the commit resolves per the bytes-equality
rule in both branches. Assert PRAGMAs are set on a fresh connection.

──────────────────────────────────────────────────────────────────────

## 5. Pairing-rule guard (lifecycle rows enforced mechanically)

**Invariant 8**: every `is_live`/`refreshed_at` flip must ship a
`materialization_lifecycle` row in the same transaction.

**Decision — enforce at commit, not flush** (the supersede path
legitimately flushes the demotion before its lifecycle row exists):
- In models.py, add a `before_flush` session listener that *accumulates*
  into `session.info`: ids of Materializations whose `is_live` or
  `refreshed_at` changed, and materialization_ids of new
  MaterializationLifecycle rows.
- A `before_commit` listener asserts changed-ids ⊆ lifecycle-ids and
  raises `ImmutabilityError` naming the offenders; clear `session.info`
  on commit AND rollback (`after_transaction_end` is simplest).
- Exemption for tests that legitimately flip `is_live` directly
  (test_immutability's projection-column test): those tests should now
  add a lifecycle row or use raw SQL — update them; the guard is the
  point.

**Tests:** flipping `is_live` without a lifecycle row raises at commit;
the invalidate/supersede/restore/refresh paths all still pass (they
already pair correctly); rollback clears the tracking state.

──────────────────────────────────────────────────────────────────────

## 6. Semantic version ordering + range selection

**Decisions (already settled):**
- Add `packaging` to pyproject dependencies. No validation at
  registration: parseable versions order, unparseable ones ("read-v1")
  are opaque labels — equality only, range operations never match them.
- Selection language: `version:` values beginning with `< <= > >= !=`
  become a range term (e.g. `version:<2.0`); plain values stay exact
  match. New `Selection.version_range: Optional[str]` field holding the
  raw specifier; `Selection.parse` routes accordingly. Use
  `packaging.specifiers.SpecifierSet` for matching.
- SQL can't compare versions: apply `code_version` range filtering in the
  existing Python-side filtering pass of
  `selection.get_selection_materialization_ids` (where glob/metadata
  filtering already happens), catching `InvalidVersion` → no match.
- Frontend: add a version-aware sort comparator in `DataTable.tsx` for
  columns whose accessorKey is `code_version` (split on dots, numeric
  where possible, fall back to string).

**Tests:** parse routing (exact vs range); range matching incl. unparseable
stored versions skipped; end-to-end `invalidate(Selection.parse("version:<2.0"))`
over mats with versions 1.0.0 / 2.1.0 / "legacy-v1".

──────────────────────────────────────────────────────────────────────

## 7. UI polish cluster (one commit each)

- **API error state everywhere**: add a `fetchJson(url)` helper in
  `web/src/api.ts` that throws on `!res.ok`/network failure; pages catch
  and render an "API unreachable: ..." message (Pipelines.tsx already does
  this — copy its pattern). Today a dead API renders as an empty database.
- **Filtered lanes in Current Outputs**: `/api/current-outputs` includes
  status `filtered` rows (the verdict materialization exists and is live —
  keep the `mat.is_live` check). UI: the existing status column shows a
  muted "filtered" badge. Rationale: "what is the current state of every
  lane" should include "correctly excluded".
- **Reduce badge in DagView** once item 1 lands (`shape: "reduce"` in the
  definition snapshot → e.g. a doubled border or "reduce" badge).

──────────────────────────────────────────────────────────────────────

## 8. Examples + LLM seed prompt (positioning)

- `examples/llm_enrich.py`: CsvSource over a small checked-in CSV,
  `screen` (Filtered) → `enrich` (fake-LLM function — deterministic
  stand-in with a comment showing where a real client call goes;
  `retries=3, retry_on=..., rate_limit="30/min"`, `index=["company"]`) →
  reduce summary once item 1 lands. Heavy comments; this is the flagship
  example.
- `examples/scraper.py`: FolderSource of URL-list files or CsvSource of
  URLs; fetch step with `stale_after="24h"`, retries, rate_limit;
  fake fetcher (no network in examples).
- `docs/llms.txt`: a compact API-teaching doc for LLMs generating
  pipelines: concepts in 10 lines, the step contract (payload positional,
  parents by name, `params` by declaration), cache identity rules
  (version/code/params/config slots), policies table, a complete worked
  example, common mistakes (mutating inputs; forgetting version bumps;
  skip_cache on expensive steps). Source it from README rather than
  inventing new claims.
- README pitch paragraph: lead with "dbt-style state for Python tasks,
  built for non-idempotent steps (LLMs, scraping)"; the
  generations/lifecycle model is the differentiator.

──────────────────────────────────────────────────────────────────────

## 9. Joins  **[DO NOT BUILD without a design session with the owner]**

Direction (not yet settled enough to build): a join creates pair lanes
from two parents (`left|right`), which requires coordinate-*creating*
steps (shape="expand") and multi-root pipelines. The conceptual
foundation (lane keys vs identity vs search) is settled and reduce (item
1) builds half the machinery. Open questions needing the owner: pair
explosion control (predicates before materialization?), expand-step
manifest caching, multi-source pipeline API. Bring a proposal first.

## 10. Naming  **[parked by owner]**

Brainstorm + PyPI availability check when asked. Not the current priority.

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
resolved-won't-do: arbitrary-rules plugin surface (wrapper-or-built-in
rule); plan()-in-UI (server never imports user code — use plan() in
Python).
