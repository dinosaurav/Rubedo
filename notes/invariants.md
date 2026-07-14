# Rubedo Invariants and Vocabulary

## The promises

Everything below exists to keep four promises. Read these first; the
vocabulary and guarantees that follow are how the promises are kept, not the
point in themselves.

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
Immutable stored result.

**Output address:**
Deterministically computed from identity inputs: `hash(step, code_version, input_hash)`

**Output content hash:**
Hash of the actual produced bytes/logical result.

**Materialization:**
Successful committed output.

**Run:**
A user-triggered execution attempt over some scope.

**Attempt/event:**
Something that happened during execution, successful or not.

**Coordinate (lane key):**
The engine's dataflow key: within a run it matches a step's output to its
consumers; across runs it decides "the same item." Unique within a scan,
stable across scans. A coordinate is **content-addressed by default**
(`row-<hash>`, so identical rows collapse and an edit reads as removed +
added); declare `key=` for a stable, legible coordinate (an edit then reads as
"changed" — same coordinate, new hash — and a non-unique declared key is an
error, not a silent suffix). It may also be **minted mid-DAG**: `expand`
mints content-addressed `row-<hash>` child lanes, keyed on the child's own
content rather than on parent+position — an edit to one child never
renumbers its siblings, and identical children (even from different
parents) collapse to one lane; `join` mints `a|b|…` pair lanes. It is *not*
the identity of work (that is the content-addressed output address) and not
the primary search handle (`index=`) — for file sources it merely coincides
with the path.

**Searching:**
Two channels, one home each: lane keys for source-shaped questions
(`coordinate_glob`); indexed fields of the output *value* for content-shaped
questions (declared with `@step(index=[...])`, extracted at commit — a label
is just data someone chose to index).

**Source:**
Not a separate type — ingestion is a root step. A parentless generator
function decorated `@step` (its `shape="expand"` inferred automatically)
yields payloads; each becomes a
content-addressed `row-<hash>` lane. A folder, a CSV, a SQL table are
*recipes* (a `pathlib` walk, a `csv.DictReader` loop, a `SELECT` loop) —
documented in `docs/concepts/sources.md`, not shipped as classes. A pipeline
may declare several source-shaped roots; `join` doesn't care that its
parents are roots. Conceptually a source is the root **producer** — the same
lane-minting primitive as `expand`/`join` (see `producer-model.md`).

**Root (head of a pipeline):**
Any step with no `depends_on` originates lanes, and its `shape` sets how many:
an `expand` root yields N (a source-shaped root; re-runs every run — see
below) or a
`map` root mints a single `@root` lane whose input is its params (or a
constant when it takes none). The map root is addressed by
`hash(step, version, @root, params)`, so identical params reuse the cached
output and changed params make a new generation — a lane fed *into* the head
rather than scanned for. A pipeline needs at least one root to originate lanes.

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed, blocked,
filtered. "Filtered" means a step declined the coordinate — a cached,
first-class verdict, not an error.

**Collective Steps & Fan-in:**
Collective steps (`reduce` / `join`) default to partial fan-in (`on_failed="use_passed"`): if a parent lane fails or is blocked, the step drops it and proceeds with the surviving lanes (firing a `partial_fan_in` warning). They block entirely only if `on_failed="block"` is requested, or if zero lanes survive and the emptiness is caused by failures/blocks.

**Invalidation:**
Removal from current/canonical eligibility, not necessarily physical deletion.

**Pruned (retention GC):**
A liveness transition, like invalidation, but driven by *run recency* rather
than a selection: a materialization outside a pipeline's keep-set (its last N
terminal runs) is demoted `is_live=False` with a paired `pruned`
lifecycle row. Facts stay; only eligibility changes. The keep-set is widened
to always include expand cache anchors (structurally, the live
materializations no `RunCoordinateStatus` references), so pruning never
silently forces an expand re-run.

**Reclaimed (retention GC):**
A *physical* object-file deletion, logged in the append-only
`object_reclamations` table. The sweep deletes an object only when **every**
materialization referencing it — across all pipelines — is non-live (the
shared-object rule: one live reference anywhere keeps the bytes). Distinct from
**missing** (a ledger-named object absent from disk unexpectedly, i.e.
corruption): a reclaimed object was deleted *on purpose*, and `rubedo du`
reports the two separately. Recovery is lazy — if a pruned lane's input
reappears, the next run rewrites the bytes and restores the row.

**Liveness / lifecycle:**
`Materialization.is_live` and `refreshed_at` are mutable projections; the
append-only `materialization_lifecycle` table (invalidated / restored /
superseded / refreshed rows) is the truth about every liveness and freshness
transition. Every `is_live`/`refreshed_at` change must be accompanied by a
lifecycle row for the same materialization in the same transaction — a session
guard enforces this at commit (see guarantee 2.6, under "never lie about what
happened," below). Similarly, Run's status columns are a projection of the
`run_events` log — and they are **terminal-only** (`completed` / `completed_with_failures` / `failed`; NULL
while in flight). "running" is never stored: it is a present-tense claim no
durable row can keep truthfully (a killed process would leave it lying
forever). Readers derive `running`/`interrupted` from `last_heartbeat_at`, an
ephemeral presence signal the run process bumps from a timer thread
(`effective_run_status()` in models.py). The heartbeat is exempt from event
pairing — presence is about *now*, not history, and nothing durable is ever
derived from it. A machine that sleeps and wakes resumes beating, so an
"interrupted" run flips back to "running" on its own.

## Guarantees, by promise

Each promise above is kept by one or more concrete guarantees, numbered
`<promise>.<guarantee>` (e.g. `2.6` is the sixth guarantee under promise 2) so
a reference always carries which promise it serves. These numbers are prose
cross-reference identifiers for *this document only* — code never names them;
comments and error messages describe the constraint directly and, at most,
point at this file in general terms.

### 1 — Never pay twice for the same computation

- **(1.1) "Already done" is checked against the ledger, not memory.**
  Skip-if-exists is a materialization lookup keyed on the deterministic
  output address, not a runtime cache — so it survives process restarts,
  new workers, and separate runs equally.
- **(1.2)** Content-addressing does the rest of the work implicitly: an
  output address is `hash(step, code_version, input_hash[, params][, code])`
  — identical inputs always land on the same address, so a cache hit is
  found by construction, not by a lookup table someone has to maintain.
- **(1.3)** The generations protocol extends this across time, not just
  within a run: identical bytes reuse or restore the existing row; only
  genuinely different bytes supersede it (`ledger.py`'s
  `_commit_materialization`). A pruned lane whose input later reappears with
  the same content restores the old row for free instead of recomputing.

### 2 — Never lie about what happened

- **(2.1) No materialization row exists unless the output committed
  successfully.** A row is never created optimistically or speculatively.
- **(2.2) A committed materialization is immutable.** Facts about a
  generation, once true, are never edited in place — a change means a new
  generation, never a rewritten old one.
- **(2.3) Workers may die at any point without corrupting committed state.**
  Execution is DB-free; a killed process leaves no half-written ledger row.
- **(2.4) Users enumerate through current views (the latest run's active
  lanes), never raw object storage.** What you're shown is always ledger
  truth, never an accidental read of bytes the ledger doesn't currently
  vouch for.
- **(2.5) Run status lives on the run-coordinate edge, not on output
  bytes.** The same output can be `created` in one run and `reused` in
  the next — that's a fact about the run, not a property of the object,
  and the ledger keeps them separate rather than conflating them.
- **(2.6) Ledger tables are append-only, enforced by ORM guards; the only
  legal updates anywhere are the projection columns (`Run` lifecycle,
  `Materialization.is_live`/`refreshed_at`).** Every `is_live`/`refreshed_at`
  flip must ship a `materialization_lifecycle` row for that materialization
  in the same transaction — a `before_commit` session guard (the pairing
  guard) rejects an unpaired flip. This is the guarantee that makes the
  rest of this section mechanically true rather than a design intention:
  without it, the ledger could go stale-but-silent.

### 3 — Order and parallelism never change results

- **(3.1)** Output addresses are computed from `step`, `version`,
  `input_hash`, and optionally `params`/`code` — never from wall-clock
  order, thread scheduling, or worker assignment. Two runs of the same
  pipeline over the same inputs always produce the same addresses
  regardless of how the work was scheduled.
- **(3.2)** `schedule="broad"` (stage-at-a-time) and `schedule="deep"`
  (pipelining consecutive ≤1-parent map steps through a lane) are a
  scheduling choice only — the resulting ledger rows are identical either
  way.
- **(3.3)** `executor="thread"` vs `executor="process"` changes how step
  functions are run, never what address or content they produce.
- **(3.4)** `reduce`/`join` fan-in is keyed on which lanes survived (by
  content, via `group_key`/`join_on`), never on the order lanes happened to
  finish in.

### 4 — Bytes are disposable, facts are not

- **(4.1) Invalidation never silently deletes historical facts. Retention
  GC deletes *bytes*, never *facts*:** it demotes materializations (a
  paired `pruned` lifecycle row) and sweeps object files that no live
  materialization references, but never removes a ledger row — the
  record of what ran, and of the deletion itself
  (`object_reclamations`), always survives.
