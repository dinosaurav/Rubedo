# Arrow Storage — design

Status: **design phase** (owner design session 2026-07-15). This doc
supersedes the external "Arrow Architecture Discussion.md" — the decisions
settled there and in the follow-up conversation are recorded here as the
build spec. Read `notes/invariants.md` for the current vocabulary; this doc
will update it when the model changes.

The headline: outputs move from per-lane JSON blobs (one SQLite
`materializations` row per lane) to **append-only Arrow IPC files** (one
row per lane, per attempt, stacked across time). The transactional
control plane stays in SQLite. The result deletes four of the hardest
pieces of machinery in the engine — the partial unique index "one live mat
per address," the `materialization_lifecycle` table, the pairing guard,
and the supersede/restore/savepoint dance in `_commit_materialization` —
at the cost of one new keyed table that does soft-lock + GC + crash
detection in one structure.

## The three substrates

| Layer | Technology | Role |
|---|---|---|
| Output storage | Arrow IPC files under `.rubedo/tables/` | append-only history of what each step's lanes produced |
| Query over outputs | pyarrow (`pa.ipc` + `pa.compute`) | reuse checks, selection, trace alignment, du scans |
| Control plane | SQLite + SQLAlchemy | run history, the soft lock, crash detection, object reclamations, immutable audit log |

DuckDB is **not** in scope for v1. pyarrow scans cover every query we need.
DuckDB may be revisited once the Arrow storage is stable, as a query-layer
accelerator — it composes with both Arrow storage and SQLite control plane
without displacing either. Do not adopt it speculatively.

## The per-step Arrow file

One file per step output. Append-only rows = one attempted output per lane:

```
columns:
  lane_key        string          the coordinate through the DAG
  input_hash      string          identity of the inputs this row consumed
  output          <inline or blank>   the value, or null for a tombstone
  content_hash    string          hash of output bytes (for dedup + child input_hash)
  ts              timestamp       when this row was written (latest = current)
  run_id          string          which run produced it
```

- **Filled row** = a computed result. The output column is either an inline
  scalar/struct, or an object-store ref (`objects/<hash>` for large/binary
  values; see "TOAST spill" below). `content_hash` is the hash of the
  serialized value — used by children as `input_hash` and by dedup.
- **Blank row** = an invalidation tombstone (`output IS NULL`). Written by
  `invalidate()` only — never by execution. The latest row for a lane is
  blank → readers see "pending, will recompute next run."
- **Absent row** = the lane was never computed, or the worker crashed
  mid-run. Distinct from blank: an invalidate wrote a tombstone; a crash
  wrote nothing. The new `input_hash → last_run_id` table tells the two
  apart (see below).

**Latest-by-`ts` is current state.** No `is_live` projection, no partial
unique index, no generations protocol. "What is live" is a query: scan the
file for the latest row for `(lane_key)`; filled = live, blank =
invalidated, absent = pending-or-crashed.

## What gets deleted from the current ledger

| Current | Fate | Why |
|---|---|---|
| `materializations` table | → Arrow rows | the lane history IS the table |
| `materialization_lifecycle` table | **gone** | the row sequence in Arrow IS the lifecycle log |
| `materialization_index` table | **gone** | the indexed field is a column in the Arrow file — selection scans it (pyarrow predicate, no SQL denormalization) |
| `uq_live_output_address` partial unique index | **gone** | no longer one-live-per-address; multiple rows per `input_hash` across time are expected and correct |
| the pairing guard (`_assert_liveness_pairing`) | **gone** | nothing to pair — there's no `is_live` flip and no lifecycle row to ship with it |
| the supersede/restore/savepoint dance in `_commit_materialization` | **gone** | recompute just appends; identical bytes are detected by `content_hash` equality, not by a unique-index collision |
| `output_content_hash` *as a row identity* | gone as identity; **stays as a column** | the child's `input_hash` is the parent's `content_hash` (`planning.py:145`), so the content hash must be readable — it's just data now, not a unique-index key |

## What stays in SQLite

| Table | Status | Notes |
|---|---|---|
| `runs` | unchanged | run identity, params, definition snapshot, heartbeat |
| `run_events` | unchanged | append-only audit log: run lifecycle, retries, drift, warnings, human messages with severities. Per-lane outcome events (`step_cache_hit`, `materialization_created`, …) overlap with the structured outcome table, but the audit-feed shape (severities, free-text messages, per-attempt retry traces) is genuinely different. Keep until a real reader wants to consolidate. |
| `object_reclamations` | unchanged | GC audit of deleted object bytes |
| `run_coordinate_statuses` | **trimmed** (see "decision below") | drop `output_address` and `materialization_id` columns — both now derivable from `(step, lane_key, input_hash)` via an Arrow lookup. Keep `status`, `error_*`, `source_id`, `metadata_json`. The structural mat-linkage the server UI depends on becomes a join against the Arrow file, not against SQLite. |
| `materialization_edges` | **kept for now** (defer deletion; see "edges") | the lineage table. Deletion is the goal but `expand` parentage is not derivable from bytes alone — expanded child `lane_key`s are their own content hashes, not their parent's. Deletion requires persisting parent lane origin alongside each expanded child output. Doable but a separate sub-project; keep the table until then. |

## What's added to SQLite

**One new table: `input_hash_usages`** (working name — the `input_hash → last_run_id` map we settled on).

```
input_hash_usages
  input_hash     VARCHAR  PRIMARY KEY     -- the lane content identity
  step_name       VARCHAR  NOT NULL        -- which step's output
  pipeline_id     VARCHAR  NOT NULL        -- which pipeline
  last_run_id     VARCHAR  NOT NULL        -- the most recent run that claimed it
  claimed_at      VARCHAR  NOT NULL        -- when the claim was inserted
  fulfilled       BOOLEAN  NOT NULL DEFAULT 0  -- does a filled Arrow row exist for this claim?
```

This one table carries three jobs:

1. **Scheduler soft lock.** Before a worker executes `(step, input_hash)`,
   the engine inserts a row (or updates `last_run_id` / `claimed_at`). A
   second worker consulting it sees an in-flight claim and defers. This
   replaces the partial unique index's race-loser-buys-free path: today
   the loser of the INSERT race gets `IntegrityError` and re-reads the
   winner's output; under the new model, the loser sees the soft-lock row
   and either waits or reads the once-filled output. Not perfectly atomic
   — it's a hint the scheduler consults, not a storage-engine constraint.
   Two workers *can* both run the step; one's output becomes history.
   Acceptable for the LLM/scraping workload (lanes with distinct
   `input_hash` are the norm; races are rare). Documented as soft.

2. **Crash detection.** A row with `fulfilled=0` for a run that has
   reached terminal status means a worker crashed mid-execution. The next
   run sees the unfulfilled claim and knows to retry (the Arrow file has
   no filled row for this lane, so reuse-check naturally misses). On a
   successful commit, the engine flips `fulfilled=1`.

3. **GC handle.** Retention prunes by run recency. Today `gc.py` joins
   `materializations` ↔ `materialization_lifecycle` ↔ `runs` to build the
   keep-set. Under the new model, "is this output still referenced by a
   recent run?" is a lookup on `input_hash_usages.last_run_id` joined to
   `runs.started_at`. One index, one join — strictly simpler than the
   current three-table dance.

## The two mechanisms, and only two

This is the heart of the simplification. The design has *two* mechanisms
where the current engine has *four*:

1. **Blank rows** = invalidation *only*. `invalidate()` inserts a blank
   row with the `lane_key` + `ts`. That's it. Not a claim token, not a
   soft lock — purely "this lane is pending, recompute it." The latest
   row being blank is the invalidation state; the next run sees it and
   recomputes.

2. **`input_hash_usages`** = claim + crash + GC. The scheduler consults it
   before claiming; the engine updates it on commit; retention reads it
   for keep-set. Three jobs, one table, no Arrow involvement.

Crash semantics — pin these explicitly, since the simplification leans on
them:
- A worker that **succeeds** writes a filled Arrow row + flips
  `fulfilled=1` on the claim row.
- A worker that **crashes** writes neither. The claim row remains with
  `fulfilled=0` and the Arrow file has no row for this `lane_key`. The
  next run sees the unfulfilled claim and a missing latest row — they
  agree: "pending, retry."
- A worker that **fails terminally** (exhausts retries) writes the claim
  row with `fulfilled=1`? No — the run reached a terminal "failed" state
  and the `run_events` has the per-attempt tracebacks; the claim row
  stays `fulfilled=0` and the next run retries. This matches today: a
  failed lane is not cached; `run_events.step_failed` is the record.
  `fulfilled=1` is reserved for "a filled Arrow row exists."

## Reduces and dedup

Lane dedup (two lanes with identical `input_hash` reusing one execution)
survives the move but is no longer emergent. Today it falls out of the
unique index: lane B's `INSERT` fails, B reads A's committed output. Under
the new model, two lanes with the same `input_hash` are two rows; the
engine needs an explicit rule to populate B without re-running:

- **Plan-time copy**: at plan time, if `(step, input_hash)` already has a
  filled row in the step's Arrow file, copy the output value into this
  lane's row. No re-execution, one write. This is the analog of today's
  `IntegrityError` path, just eager in planning instead of lazy on
  collision.

The `content_hash` column is what makes byte-identical reuse still work:
- parent re-runs, produces a new row with identical bytes → `content_hash`
  is identical → child's `input_hash` (= parent's `content_hash`) is
  unchanged → child reuses. The downstream cascade is skipped for free.
  The *parent* paid (it re-executed), but that's not a regression — today
  the parent also re-executes to find out the bytes are identical. The
  downstream protection that actually mattered survives via `content_hash`
  in `input_hash`.

## TOAST spill (deferred to Phase 2b, not blocking v1)

A 50KB LLM response or a 2MB image shouldn't bloat the inline Arrow
column. Postgres's TOAST pattern: small inline, large spilled to the
object store with a hash ref in the column. Not needed for v1 — a v1
Arrow file can hold arbitrary-sized string columns; the bloat cost is
deferred until profile evidence justifies the complexity. When added:

- Type-based: `bytes`/images always spill.
- Size-based: a value > threshold (e.g. 4KB) spills; column stores the
  hash instead.
- Declaration: `@step(spills=["ocr_text"])` forces spill, overrides size.

The object store stays content-addressed; spilled values are ordinary
objects, GC'd via `object_reclamations` as today.

## Edges (deferred deletion — the one open sub-project)

`materialization_edges` is the lineage table. Deletion is the goal —
lineage *should* be derivable from `lane_key` alignment between adjacent
step Arrow files (the §11/§12 "zips not joins" idea):

| Shape | Alignment derivation |
|---|---|
| `map` chain | zip on `lane_key` — same keys, same order. Trivial. |
| `join` | pair `lane_key` is `a\|b\|c` — split on `\|`, look up each component in its parent's file. Self-describing. |
| `reduce` | group via the `group_key` rule (from the step spec, already in planning). Needs code, no extra metadata. |
| `filter` | `RunCoordinateStatus.status == "filtered"` distinguishes filtered from absent — already present. No extra metadata. |
| **`expand`** | **broken without persisted parentage** — expanded child `lane_key` is the child's own content hash, not the parent's. Parent→child alignment is impossible without persisting the parent `lane_key`(s) each expanded child came from. |

**Deletion prerequisite:** make `expand` parentage self-describing on
disk. Either an extra Arrow column `_parent_lane_keys` on expand-child
outputs, or a composite `parent_key|child_hash` keying scheme. This is a
real storage-format change, not a query rewrite — defer until Phase 2 is
stable and the lineage queries are the concrete pain.

Until then, `materialization_edges` stays. Its only readers are
`trace._bfs` (lineage closure) and the downstream-invalidation blast
radius. Everything else — planning, scheduling, execution, GC, retention,
selection — is already independent of it.

## What's not in scope (do not build)

- **Stream batching / per-batch durability** (the §10 Arrow IPC stream
  mode). Today each lane commits individually; the new model writes one
  Arrow IPC file per step. Stream batching (flush N-lane batches to disk
  as a durable log, recover on crash) is an optimization for the 1M-lane
  table-shaped case. The ACRIS investigation (~50 lanes, the workload
  where Rubedo's value is real) doesn't benefit. Defer until a real user
  has a 1M-lane step that feels the cost.
- **Grandparent column access without passthrough** (§11). The "zips not
  joins" idea is a real simplification — but it changes `input_hash` to
  "hash only declared columns, pulled from possibly-grandparent files,"
  which is a `spec.py` extension, not a storage tweak. Sequence it
  *after* Phase 1, gated on real user pain with passthrough.
- **`shape="join_table"`** (§18). The combined join/reduce shape mints
  one lane for a joined table instead of M×N ephemeral lanes. Saves 2-5
  seconds of ledger overhead at 1M+ lanes. The in-body `df.join()`
  workaround produces the same cached result today. Defer — the
  Arrow-serialized table-as-output (Phase 1) makes the workaround fast
  without minting a new verb.
- **DuckDB as query layer.** Not in v1. pyarrow scans cover every query.
  Revisit once Arrow storage is stable and a real query path is
  measurably slow.
- **Total SQLite replacement.** The transactional control plane stays in
  SQLite. The Arrow move is a data-plane refactor, not a control-plane
  redesign — the ORM immutability guards, the proven WAL recovery, and
  the stdlib no-cost dependency all earn their keep for the control plane.

## Implementation phasing

### Phase 1 — Arrow serialization (smallest change, biggest impact)
1. New `_serialize` branch in `store.py`: `pl.DataFrame` / `pa.Table` →
   Arrow IPC bytes, `content_type="arrow-ipc"`. Stored as an ordinary
   content-addressed object in `objects/` (no new directory yet — the
   table layout comes in Phase 2a).
2. Matching `read_materialization_output` branch for the new
   `content_type`.
3. One test that round-trips a polars DataFrame through the store.
**Value**: DataFrame-returning steps become cacheable. The ACRIS detect
pipeline (7 steps all `skip_cache=True` today) becomes cached end-to-end;
threshold changes recompute only the affected pattern step.

### Phase 2 — The refactor (the bulk of this work)
This is the big change. Suggested order, each a separate commit:

- **2a.** New `tables/` directory. Replace the Phase 1 Arrow-IPC-in-`objects/`
  approach with one IPC file per step output under `tables/<pipeline>/<step>.arrow`.
  The file is append-only; rows stack across runs.
- **2b.** New `input_hash_usages` SQLite table (the `input_hash → last_run_id`
  map). Soft-lock insert path; `fulfilled` flip on commit. No old code
  depends on it yet — add it alongside, wire up next.
- **2c.** Rewrite `ledger._commit_materialization` from the
  supersede/restore/savepoint dance to a one-line append of an Arrow row.
  Delete the `IntegrityError`-fallback retry path. The engine writes a
  filled row + flips `fulfilled=1`; that's the commit.
- **2d.** Delete `materialization_lifecycle` table and the pairing guard
  (`_assert_liveness_pairing`, `_track_liveness_pairing`, the
  `before_flush`/`before_commit`/`after_rollback` listeners in
  `models.py:313-366`). `invalidate()` writes a blank Arrow row, not a
  lifecycle row. No pairing needed.
- **2e.** Delete `materialization_index` table + the index-writer code in
  `_commit_execution_result`. Rewrite `selection.get_selection_materialization_ids`
  to scan the Arrow file's struct column with a pyarrow predicate
  (`pa.compute.field("company") == "acme"`). `@step(index=[...])` stays
  as a query-validation declaration (which fields are searchable), but
  stops denormalizing into SQLite.
- **2f.** Trim `run_coordinate_statuses`: drop `output_address` and
  `materialization_id` — both derivable from `(step, lane_key, input_hash)`
  via an Arrow file lookup. Keep `status`, `error_*`, `source_id`,
  `metadata_json`. Rewrite the server/queries readers that today join on
  `materialization_id` to instead resolve through the Arrow lookup.
- **2g.** Rewrite `gc.py`: keep-set is now "the `input_hash_usages` rows
  whose `last_run_id` is in the pipeline's last N runs" — one index join
  against `runs`, no `materializations`/`materialization_lifecycle`
  walk. Sweep unchanged (object bytes still deleted via
  `object_reclamations` when unreferenced).
- **2h.** Rewrite `trace._bfs` for the `map`/`join`/`reduce` lineages to
  align `lane_key`s between Arrow files (zip / pair-key split / group-key
  rule). Keep `materialization_edges` for `expand` until 2i (or defer
  indefinitely — the table is small and the blast-radius reader is the
  only consumer; correctness is paramount).
- **2i.** (Optional, deferred) Persist `expand` parentage as an Arrow
  column on child outputs (`_parent_lane_keys`), rewrite `trace._bfs` for
  expand, delete `materialization_edges`. Separate sub-project.

### Phase 3 — Old-table deletion (after Phase 2 is verified)
Delete `materializations`, `materialization_lifecycle`,
`materialization_index` tables. Drop the ORM guards for the deleted
models. Update `invariants.md`.

## What this means for `invariants.md`

The four promises don't change. The mechanisms that keep them do:

- **1.1 "Already done" checked against the ledger.** Today: a
  `Materialization` row keyed on `output_address`. Tomorrow: the latest
  filled row for `(step, lane_key)` in the step's Arrow file. Same
  durability, same crash-safety (Arrow IPC is append-only and valid at any
  flush point — a half-written last row is detectable on scan).
- **2.2 "A committed materialization is immutable."** Today: ORM
  `before_update`/`before_delete` guards. Tomorrow: Arrow IPC is
  immutable by construction — you append, you never edit. The immutability
  guarantee moves from the ORM layer to the storage format.
- **2.3 "Workers may die without corrupting state."** Today: execution is
  DB-free; a killed process leaves no half-written ledger row. Tomorrow:
  a killed worker leaves an unfulfilled `input_hash_usages` row and no
  Arrow row for that lane — the next run sees "pending" and retries.
  Cleaner than today, where failure has to be inferred from `run_events`
  because no mat row exists.
- **2.6 "Every `is_live` flip ships a lifecycle row in the same
  transaction."** **Gone.** There's no `is_live` projection and no
  lifecycle row. The append-only row sequence *is* the lifecycle log;
  liveness is a query ("latest row for this lane"), not a stored
  projection. The pairing guard was the mechanical enforcement of the old
  invariant — it's not needed when the invariant itself is rephrased.

## Open questions to resolve during Phase 2

1. **`run_coordinate_statuses` vs `run_events` consolidation.** They
   overlap on per-lane outcome signal. `run_events` is the audit log
   (severities, retry traces, run lifecycle); `run_coordinate_statuses`
   is the structured outcome table (`source_id`, indexed `status`, the
   one-row-per-lane invariant for "latest run's lanes"). Neither
   subsumes the other without losing readers. **Working decision: keep
   both for now; revisit after Phase 2 lands and the server readers are
   rewritten.** The consolidation is a cleanup, not a refactor — defer.
2. **Per-lane materialization for dict outputs.** The whole "1 mat row per
   table-shaped step" optimization... is *not* part of this design. This
   design keeps one Arrow row per lane, per step, whether the output is a
   dict or a DataFrame. A table-shaped step returning one DataFrame
   produces one Arrow row (the DataFrame is the value of that one row);
   50 lanes producing 50 dicts produce 50 rows in the step's file. The
   1M-lane overhead fix came from *choosing the right step shape*
   (return a table, don't `yield` 1M rows), not from collapsing per-lane
   rows. This is simpler and matches how readers work today.
3. **`expand` parentage persistence (2i).** Real storage-format change.
   Don't spec it until 2h is done and the lineage queries are the
   concrete pain. Keep `materialization_edges` working in the meantime.

## Origin notes

This doc synthesizes a design session on 2026-07-15 with the owner,
building on the external "Arrow Architecture Discussion.md" from the
ACRIS demo project. The five decisions that shifted the design from
the original doc:

1. The control plane stays in SQLite. The data plane moves to Arrow.
   "Store everything in Arrow" crosses past where Arrow's strengths are
   into rebuilding a transactional store, badly.
2. The original doc's "lanes ARE rows" isomorphism is oversold. Arrow is
   a better serialization format for *one specific kind of step output —
   tables* (and, transitively, for dict outputs once they're inline
   struct columns). The rest of the engine shouldn't notice.
3. The blank-row mechanism is invalidation *only*, not a claim token.
   Crashed workers write nothing — the unfulfilled `input_hash_usages`
   row is the crash signal.
4. Lane dedup survives but stops being emergent. The unique index no
   longer catches duplicate `input_hash`es; a plan-time copy rule does.
5. `materialization_index` deletes unconditionally with pyarrow alone —
   DuckDB is not a prerequisite. The indexed field is an Arrow column;
   a predicate scan replaces the SQLite denormalization lookup.