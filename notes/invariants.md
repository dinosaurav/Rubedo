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
Anything that can enumerate coordinates with content hashes and load their
payloads (folder of files, CSV rows, table rows), identified by a stable
`source_id`. A pipeline may declare several (`sources={name: Source}`); a root
step picks one with `@step(source="name")`. Conceptually a source is the root
**producer** — the same lane-minting primitive as `expand`/`join` (see
`producer-model.md`).

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed, blocked,
filtered. "Filtered" means a step declined the coordinate — a cached,
first-class verdict, not an error.

**Collective Steps & Fan-in:**
Collective steps (`reduce` / `join`) default to partial fan-in (`on_failed="use_passed"`): if a parent lane fails or is blocked, the step drops it and proceeds with the surviving lanes (firing a `partial_fan_in` warning). They block entirely only if `on_failed="block"` is requested, or if zero lanes survive and the emptiness is caused by failures/blocks.

**Invalidation:**
Removal from current/canonical eligibility, not necessarily physical deletion.

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
7. Invalidation never silently deletes historical facts.
8. Ledger tables are append-only, enforced by ORM guards; the only legal
   updates anywhere are the projection columns (Run lifecycle,
   Materialization.is_live). Every `is_live`/`refreshed_at` flip must ship a
   `materialization_lifecycle` row for that materialization in the same
   transaction — a `before_commit` session guard rejects an unpaired flip.
