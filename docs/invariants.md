# BatchBrain Invariants and Vocabulary

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
The engine's dataflow key: it matches a step's output to its consumers within
a run, and matches "the same item" across runs so "changed" (same coordinate,
new hash) is distinguishable from "removed + added". Unique within a scan
(sources disambiguate collisions mechanically), stable across scans. It is
*not* the identity of work (that is the content-addressed output address) and
not the primary search handle — it merely coincides with a natural human name
for file-like sources, where it is a path.

**Searching:**
Two channels, one home each: lane keys for source-shaped questions
(`coordinate_glob`); indexed fields of the output *value* for content-shaped
questions (declared with `@step(index=[...])`, extracted at commit — a label
is just data someone chose to index).

**Source:**
Anything that can enumerate coordinates with content hashes and load their payloads (folder of files, CSV rows, table rows). Identified by a stable `source_id`.

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed, blocked,
removed, filtered. "Filtered" means a step declined the coordinate — a cached,
first-class verdict, not an error.

**Invalidation:**
Removal from current/canonical eligibility, not necessarily physical deletion.

**Liveness / lifecycle:**
`Materialization.is_live` and `refreshed_at` are mutable projections; the
append-only `materialization_lifecycle` table (invalidated / restored /
superseded / refreshed rows) is the truth about every liveness and freshness
transition. Every projection change must be accompanied by a lifecycle row in
the same transaction. Similarly, Run's status columns are a projection of the
`run_events` log.

## Core Invariants

1. No materialization row exists unless the output committed successfully.
2. A committed materialization is immutable.
3. Workers may die at any point without corrupting committed state.
4. Skip-if-exists checks materialization existence, not worker memory.
5. Users enumerate through manifests/current views, never raw object storage.
6. Run status lives on the run-coordinate edge, not on output bytes.
7. Invalidation never silently deletes historical facts.
8. Ledger tables are append-only, enforced by ORM guards; the only legal
   updates anywhere are the projection columns (Run lifecycle,
   Materialization.is_live).
