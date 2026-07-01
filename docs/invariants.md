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
Human-facing selection key, e.g. file path.

**Run-coordinate status:**
Relationship between a run and a coordinate: created, reused, failed, skipped, out_of_scope, etc.

**Invalidation:**
Removal from current/canonical eligibility, not necessarily physical deletion.

## Core Invariants

1. No materialization row exists unless the output committed successfully.
2. A committed materialization is immutable.
3. Workers may die at any point without corrupting committed state.
4. Skip-if-exists checks materialization existence, not worker memory.
5. Users enumerate through manifests/current views, never raw object storage.
6. Run status lives on the run-coordinate edge, not on output bytes.
7. Invalidation never silently deletes historical facts.
