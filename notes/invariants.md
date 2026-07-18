# Rubedo Invariants and Vocabulary

## The promises

Everything below exists to keep four promises. Read these first; the
vocabulary and guarantees that follow are how the promises are kept, not
the point in themselves.

1. **Never pay twice for the same computation.** If Rubedo has already
   produced a given output for a given input, it will find and hand back
   that output instead of recomputing — even for a non-idempotent step
   (an LLM call, a scrape) where recomputing would be wasteful or wrong.
2. **Never lie about what happened.** What the ledger says ran, succeeded,
   or is currently live is always true, and it stays true after crashes,
   concurrent runs, and time. If something didn't actually commit, no row
   claims it did; if a fact was ever true, no later write erases it.
3. **Order and parallelism never change results.** The same pipeline over
   the same inputs produces the same addresses and the same ledger rows
   whether steps run on one thread or many, one process or a pool, in
   whatever order the scheduler happens to pick.
4. **Bytes are disposable, facts are not.** Storage can always be
   reclaimed under pressure or a retention policy — but doing so only ever
   deletes object bytes, never the record of what ran, what it produced,
   or that the deletion happened.

## Vocabulary

**Object/output bytes:**
Immutable stored result — an inline value in the Arrow lane store's
`output` column (the common case), or a blob in the content-addressed
object store (`objects/`) for values that spill (large/binary),
referenced from the column as an `"objects:<hash>"` ref string.

**Output address:**
Deterministically computed from identity inputs:
`hash(step, code_version, input_hash[, params][, code], pipeline)`.  The
comprehensive cache identity — two lanes with the same address are the
same computation and share one cached result. `pipeline` is a required,
always-present, always-last segment (not optional like `params`/`code`):
it scopes every address to its owning pipeline, so an identically
named+versioned step with identical input in a *different* pipeline
never shares an address, a liveness row, or a materialization (TODO 33)
— the copy-a-pipeline-to-experiment case no longer cross-invalidates.

**Lane store (Arrow):**
One IPC file per step under `.rubedo/tables/<pipeline>/<step>.arrow`.
Each row is one successful computation: `row_id, lane_key, address,
input_hash, code_version, output, output_identity, content_type,
code_hash, ts, run_id, filtered`.  `output` holds the value itself in a
native Arrow type (struct for dicts, int64, string) or an
`"objects:<hash>"` ref string for spilled values; `output_identity` is
the value's content-identity hash, computed once at commit time and read
back at plan time (so Arrow's struct null-fill for heterogeneous dicts
can never shift identity).  Pure data — no tombstones, no liveness, no
`is_live` column.  The file is append-only; rows stack across runs, and
a row is written only for created/refreshed outputs — pure reuse writes
nothing.

**Input hash usage (liveness gate):**
The `input_hash_usages` SQLite table — one row per output address
(`address` is the primary key; the planner already knows which pipeline
and step it is asking about, and constructs the Arrow file path
`tables/<pipeline>/<step>.arrow` itself).  `fulfilled=True` means a
filled Arrow row exists (reuse);
`fulfilled=False` means recompute (covers crash, in-flight claim, and
invalidation — all three mean "no filled Arrow row to reuse").  This is
the single source of truth for liveness.  The table is the one
non-append-only ledger table — `fulfilled` and `last_run_id` legitimately
update (claim at plan time, fulfill at commit time, tombstone on
invalidate, demote on prune).

**Materialization:**
Successful committed output. The `materializations` SQLite table is
**deleted** — the Arrow lane store is the sole source of truth for output
metadata. A materialization is a filled Arrow row + `input_hash_usages.fulfilled=True`.

**Run:**
A user-triggered execution attempt over some scope.

**Attempt/event:**
Something that happened during execution, successful or not.

**Coordinate (lane key):**
The engine's dataflow key: within a run it matches a step's output to its
consumers; across runs it decides "the same item." Unique within a scan,
stable across scans. A lane minted from a payload is **content-addressed**
(`row-<hash>`, so identical rows collapse and an edit reads as removed +
added, never "changed in place"). It may also be **minted mid-DAG**:
`expand` mints content-addressed `row-<hash>` child lanes; `join` mints
`a|b|…` pair lanes. It is *not* the identity of work (that is the
content-addressed output address) and not the primary search handle
(the output struct's fields).

**Searching:**
Two channels, one home each: lane keys for source-shaped questions
(`coordinate_glob`); fields of the output *value* for content-shaped
questions (the output struct's fields, searchable directly — a label is
just data a step chose to return).

**Source:**
Not a separate type — ingestion is a root step. A parentless generator
function decorated `@step` (its `out_shape="many"` inferred automatically
— the `shape="expand"` alias)
yields payloads; each becomes a content-addressed `row-<hash>` lane. A
pipeline may declare several source-shaped roots; `join` doesn't care
that its parents are roots. Conceptually a source is the root
**producer** — the same lane-minting primitive as `expand`/`join` (see
`producer-model.md`).

**Root (head of a pipeline):**
Any step with no `depends_on` originates lanes, and its `out_shape` sets how
many: an `expand` root yields N (a source-shaped root — anchor-cached
like any expand, so the generator is not re-run while its identity is
unchanged; sources that watch external state declare `check_cache=False`
to re-enumerate each run) or a `map` root mints a single `@root` lane
whose input is its params.
A pipeline needs at least one root to originate lanes.

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed,
blocked, filtered. "Filtered" means a step declined the coordinate — a
cached, first-class verdict, not an error.

**Collective Steps & Fan-in:**
Collective steps (`aggregate` / `fold` / `join`) default to partial fan-in
(`on_failed="use_passed"`): if a parent lane fails or is blocked, the step
drops it and proceeds with the surviving lanes (firing a `partial_fan_in`
warning). They block entirely only if `on_failed="block"` is requested,
or if zero lanes survive and the emptiness is caused by failures/blocks.

**Invalidation:**
Flip `input_hash_usages.fulfilled=False` for the matching address(es).
The Arrow row stays as history; the next run sees `fulfilled=False` and
recomputes.  No lifecycle row, no blank tombstone in the Arrow file.
Recovery is lazy — the next run re-executes and flips `fulfilled=True`.

**Pruned (retention GC):**
A liveness transition, like invalidation, but driven by *run recency*
rather than a selection: an output outside a pipeline's keep-set (its
last N terminal runs) is demoted `fulfilled=False`. Facts stay; only
eligibility changes. The keep-set is widened to always include expand
cache anchors (structurally, the fulfilled entries no
`RunCoordinateStatus` references), so pruning never silently forces an
expand re-run.

**Reclaimed (retention GC):**
A *physical* object-file deletion, logged in the append-only
`object_reclamations` table. The sweep deletes an object only when
**every** reference to it — across all pipelines — is non-live (the
shared-object rule: one live reference anywhere keeps the bytes).
Recovery is lazy — if a pruned lane's input reappears, the next run
rewrites the bytes.

**Liveness:**
`input_hash_usages.fulfilled` is the single gate. `True` = reuse (read
content from the Arrow lane store); `False` = recompute.  The
`Materialization` model and `materialization_lifecycle` table are
**deleted** — liveness is a column in a keyed table, not a projection of
an append-only log.  Run's status columns remain a projection of the
`run_events` log — they are **terminal-only**
(`completed` / `completed_with_failures` / `failed`; NULL while in
flight). "running" is never stored: it is a present-tense claim no
durable row can keep truthfully. Readers derive `running`/`interrupted`
from `last_heartbeat_at`, an ephemeral presence signal the run process
bumps from a timer thread (`effective_run_status()` in models.py).

## Guarantees, by promise

Each promise above is kept by one or more concrete guarantees, numbered
`<promise>.<guarantee>` (e.g. `2.6` is the sixth guarantee under promise
2) so a reference always carries which promise it serves. These numbers
are prose cross-reference identifiers for *this document only* — code
never names them; comments and error messages describe the constraint
directly and, at most, point at this file in general terms.

### 1 — Never pay twice for the same computation

- **(1.1) "Already done" is checked against the liveness gate, not
  memory.** Skip-if-exists is an `input_hash_usages.fulfilled=True`
  lookup keyed on the deterministic output address — so it survives
  process restarts, new workers, and separate runs equally.
- **(1.2)** Content-addressing does the rest of the work implicitly: an
  output address is
  `hash(step, code_version, input_hash[, params][, code], pipeline)`
  — identical inputs *within the same pipeline* always land on the same
  address, so a cache hit is found by construction, not by a lookup
  table someone has to maintain. `input_hash` and lane-key minting
  (`row-<hash>`) stay pipeline-free by design — only the output address
  is pipeline-scoped (TODO 33) — so identical payload content still
  mints the identical lane key and the identical `input_hash` in any
  pipeline; only the resulting output address, and hence liveness/reuse,
  is scoped per pipeline.
- **(1.3)** The generations protocol extends this across time: identical
  bytes reuse (no new Arrow row); only genuinely different bytes
  supersede (a new Arrow row is appended). A pruned lane whose input
  later reappears with the same content reuses the old row for free
  instead of recomputing.

### 2 — Never lie about what happened

- **(2.1) No materialization row exists unless the output committed
  successfully.** A row is never created optimistically or speculatively.
  (The `input_hash_usages` claim at plan time has `fulfilled=False` —
  that's not a materialization, it's a "work in progress" signal.)
- **(2.2) A committed materialization is immutable.** Facts about a
  generation, once true, are never edited in place — a change means a new
  generation, never a rewritten old one.
- **(2.3) Workers may die at any point without corrupting committed
  state.** Execution is DB-free; a killed process leaves an unfulfilled
  `input_hash_usages` claim (`fulfilled=False`) and no Arrow row — the
  next run sees "recompute" and retries. Cleaner than the old model,
  where failure had to be inferred from `run_events` because no mat row
  existed.
- **(2.4) Users enumerate through current views (the latest run's active
  lanes), never raw object storage.** What you're shown is always ledger
  truth, never an accidental read of bytes the ledger doesn't currently
  vouch for.
- **(2.5) Run status lives on the run-coordinate edge, not on output
  bytes.** The same output can be `created` in one run and `reused` in
  the next — that's a fact about the run, not a property of the object,
  and the ledger keeps them separate rather than conflating them.
- **(2.6) Ledger tables are append-only, enforced by ORM guards; the only
  legal updates anywhere are the projection columns (`Run` lifecycle) and
  the `InputHashUsage` liveness columns (`fulfilled`, `last_run_id`,
  `claimed_at`).** The `Materialization` model is **deleted** — no
  `is_live` projection, no `refreshed_at`, no lifecycle log. The
  `InputHashUsage` table is the one intentionally mutable ledger table:
  claim/fulfill/tombstone/demote are in-place updates, not append-only
  history.

### 3 — Order and parallelism never change results

- **(3.1)** Output addresses are computed from `step`, `version`,
  `input_hash`, `pipeline`, and optionally `params`/`code` — never from
  wall-clock order, thread scheduling, or worker assignment. Two runs of the same
  pipeline over the same inputs always produce the same addresses
  regardless of how the work was scheduled.
- **(3.2)** `schedule="broad"` (stage-at-a-time) and `schedule="deep"`
  (pipelining consecutive ≤1-parent map steps through a lane) are a
  scheduling choice only — the resulting ledger rows are identical either
  way.
- **(3.3)** `executor="thread"` vs `executor="process"` changes how step
  functions are run, never what address or content they produce.
- **(3.4)** `aggregate`/`join` fan-in is keyed on which lanes survived (by
  content, via `group_key`/`join_on`), never on the order lanes happened
  to finish in.

### 4 — Bytes are disposable, facts are not

- **(4.1) Invalidation never silently deletes historical facts. Retention
  GC deletes *bytes*, never *facts*:** it demotes outputs
  (`fulfilled=False` in `input_hash_usages`) and sweeps object files that
  no live output references, but never removes a ledger row or an Arrow
  row — the record of what ran, and of the deletion itself
  (`object_reclamations`), always survives.
