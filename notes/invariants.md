# Rubedo Invariants and Vocabulary

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
yields `parent/subkey` lanes, `join` mints `a|b|…` pair lanes. It is *not* the
identity of work (that is the content-addressed output address) and not the
primary search handle (`index=`) — for file sources it merely coincides with
the path.

**Searching:**
Two channels, one home each: lane keys for source-shaped questions
(`coordinate_glob`); indexed fields of the output *value* for content-shaped
questions (declared with `@step(index=[...])`, extracted at commit — a label
is just data someone chose to index).

**Source:**
Not a separate type — ingestion is a root step. `@source` (sugar for a
parentless `expand`) is a generator that yields payloads; each becomes a
content-addressed `row-<hash>` lane. A folder, a CSV, a SQL table are
*recipes* (a `pathlib` walk, a `csv.DictReader` loop, a `SELECT` loop) —
documented in `docs/concepts/sources.md`, not shipped as classes. A pipeline
may declare several `@source` roots; `join` doesn't care that its parents
are roots. Conceptually a source is the root **producer** — the same
lane-minting primitive as `expand`/`join` (see `producer-model.md`).

**Root (head of a pipeline):**
Any step with no `depends_on` originates lanes, and its `shape` sets how many:
an `expand` root yields N (`@source`; re-runs every run — see below) or a
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
guard enforces this at commit (see invariant 8). Similarly, Run's status
columns are a projection of the `run_events` log — and they are
**terminal-only** (`completed` / `completed_with_failures` / `failed`; NULL
while in flight). "running" is never stored: it is a present-tense claim no
durable row can keep truthfully (a killed process would leave it lying
forever). Readers derive `running`/`interrupted` from `last_heartbeat_at`, an
ephemeral presence signal the run process bumps from a timer thread
(`effective_run_status()` in models.py). The heartbeat is exempt from event
pairing — presence is about *now*, not history, and nothing durable is ever
derived from it. A machine that sleeps and wakes resumes beating, so an
"interrupted" run flips back to "running" on its own.

## Core Invariants

1. No materialization row exists unless the output committed successfully.
2. A committed materialization is immutable.
3. Workers may die at any point without corrupting committed state.
4. Skip-if-exists checks materialization existence, not worker memory.
5. Users enumerate through current views (the latest run's active lanes), never raw object storage.
6. Run status lives on the run-coordinate edge, not on output bytes.
7. Invalidation never silently deletes historical facts. Retention GC deletes
   *bytes*, never *facts*: it demotes materializations (a paired `pruned`
   lifecycle row) and sweeps object files that no live materialization
   references, but never removes a ledger row — the record of what ran, and of
   the deletion itself (`object_reclamations`), always survives.
8. Ledger tables are append-only, enforced by ORM guards; the only legal
   updates anywhere are the projection columns (Run lifecycle,
   Materialization.is_live). Every `is_live`/`refreshed_at` flip must ship a
   `materialization_lifecycle` row for that materialization in the same
   transaction — a `before_commit` session guard rejects an unpaired flip.
