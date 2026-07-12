# Retention & Garbage Collection

Rubedo's object store keeps every generation of every output **forever by
default** — recompute-avoidance is the whole point of the engine, and old
bytes are cheap insurance against re-running a non-idempotent step (an LLM
call, a scrape). Retention is the opt-in mechanism for when old bytes stop
being worth their storage. This page is the user-facing summary; for the
full model — the two policies that were considered and rejected, the exact
demote/sweep algorithm, and every guarantee with its rationale — see
[`../notes/retention.md`](../notes/retention.md).

## Default: keep everything

With no `retention=` set on a pipeline, nothing is ever deleted. Once the
store crosses roughly 1 GiB, a run prints a one-line warning pointing at
`retention=` / `rubedo gc` (the size check is cached and re-computed at
most hourly, so it never costs a full stat walk on every run).

## `pipeline(retention=N)`: keep the last N runs

```python
pipeline(id="scrape", ..., retention=5)   # keep only the last 5 runs' outputs
```

This is the set-and-forget policy. It keeps every materialization
referenced by the pipeline's last `N` **terminal** runs (`N >= 1`; the
latest run always survives) and prunes everything older that no other kept
run still references. The policy travels with the pipeline's code — it's
recorded in every run's `definition()` snapshot, so the CLI and dashboard
can read it back without ever importing your pipeline module.

**Auto-prune runs at the end of every successful run.** It:

- Only fires after a run **completes successfully** — a failed run's
  keep-set could be incomplete, so retention never prunes on the back of
  one.
- **Skips silently** (logging a `retention_skipped` event, never raising)
  if any *other* run's heartbeat is currently live — never blocks or fails
  your run waiting for a lock.
- Actually deletes bytes when it does run — auto-prune is not a dry-run;
  that's the point of a policy you set once and forget.

## `rubedo gc` / `gc()`: reconcile on demand

```bash
rubedo gc                        # dry-run: exactly what --delete would prune
rubedo gc --max-bytes 2GiB       # dry-run against a global byte budget
rubedo gc --max-bytes 2GiB --delete   # apply it
```

```python
from rubedo.gc import gc
gc(max_bytes=2 * 1024**3, delete=True)
```

`gc()` applies every pipeline's *recorded* `retention=` policy (read from
each pipeline's latest run's `definition()` snapshot — same rule as
auto-prune, no user code imported), then — if `max_bytes` is given — prunes
the globally **oldest-referenced** outputs across every pipeline, skipping
anything any pipeline's *latest* run still uses, until the store fits the
budget.

!!! warning "Dry-run is the default — `--delete` is what actually removes bytes"
    `rubedo gc` with no flags computes and *prints* exactly what a
    subsequent `--delete` would prune, and touches nothing — the dry-run
    and the real run share one planner, so what you see is a promise, not a
    guess. Nothing is deleted until you pass `--delete` explicitly.

    **`gc --delete` refuses outright (exit 1) while any run's heartbeat is
    live** — a concurrent run could be mid-commit on an output that points
    at bytes GC is about to unlink (the restore race: the run's
    exists-check passes just before GC deletes the file, then the run
    commits a live materialization pointing at nothing). Retry once the
    other run finishes. Dry-run (no `--delete`) is always safe to run
    regardless of what else is running.

## What a prune actually does

Every prune — auto or manual — runs the same two phases, driven entirely by
the ledger:

1. **Demote.** Every live materialization of the pipeline that falls
   outside the keep-set gets `is_live=False`, each flip paired with a
   `pruned` lifecycle row in the same transaction (the same append-only
   bookkeeping `invalidate()` uses — invariant 8 enforces the pairing).
2. **Sweep.** A physical object's bytes are deleted only when **every**
   materialization referencing that content hash — across *all* pipelines,
   *all* steps, all history — is now non-live. Because the store dedupes
   identical bytes, one live reference anywhere keeps the object; "this
   pipeline pruned this row" never implies "the bytes are gone." Each
   deletion is logged in the append-only `object_reclamations` table
   *before* the file is unlinked.

## Invariant 7: bytes never facts

Retention deletes **bytes**, never **facts**. A demoted or swept
materialization keeps its ledger row, its lineage edges, and its index
entries forever — `trace()` still walks straight through a pruned node;
only the stored payload itself reads as absent. Every deletion is itself a
permanent, append-only fact (`object_reclamations`): content hash, byte
count, trigger, run id, timestamp. `rubedo du` reports these as
**reclaimed** — deliberate, logged, expected — distinct from **missing**
objects (absent from disk with no matching log row: genuine corruption).

**Recovery is lazy.** If a pruned lane's input reappears — a file comes
back, a row's content reverts to something that was seen before — the next
run recomputes it: a cache miss, the non-idempotent cost paid again, same
as any other cache miss. But the recompute lands on the *same* content
hash and the *same* object path, so the old (pruned) ledger row is
**restored** rather than duplicated, resuming its history in place.

## Expand anchors are always kept

Every expand step's cache reuse hangs off a parent-addressed "anchor"
materialization (the list of its children's content hashes) that no real
lane's `RunCoordinateStatus` ever references. Retention's keep-set always
includes these anchors unconditionally, under both policies — pruning one
would silently force the next run to re-execute the expand function (the
scrape, the LLM call) it exists to avoid, which is exactly the cost
retention is supposed to prevent, not cause.

## Rule of thumb

Set `retention=` on pipelines whose old generations you'd genuinely never
resurrect — periodic scrapes, rolling reports, anything where "history
beyond N runs" has no value to you. Leave it unset where inputs churn in
and out of a source and recomputation would be expensive if a pruned input
ever reappeared — the keep-orphans default (nothing pruned, ever) is the
safer choice there.

See [`../notes/retention.md`](../notes/retention.md) for the full design —
including the two policies that were considered and rejected (age-based
expiry, per-pipeline byte budgets) and why, plus every guarantee with the
trap it closes.
