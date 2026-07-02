# BatchBrain Invariants and Vocabulary

## Vocabulary

**Object/output bytes:**
Immutable stored result.

**Output address:**
Deterministic address computed from identity inputs: `hash(step, code_version, input_hash, config_hash)`

**Output content hash:**
Hash of the actual produced bytes/logical result.

**Materialization:**
Successful committed output.

**Run:**
A user-triggered execution attempt over some scope.

**Attempt/event:**
Something that happened during execution, successful or not.

**Coordinate:**
Human-facing selection key, e.g. a file path or a row key. Produced by a Source; must stay stable across scans so "changed" (same coordinate, new hash) is distinguishable from "removed + added".

**Source:**
Anything that can enumerate coordinates with content hashes and load their payloads (folder of files, CSV rows, table rows). Identified by a stable `source_id`.

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed, skipped, out_of_scope, etc.

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
