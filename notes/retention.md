# How retention works

Rubedo's object store is content-addressed and keeps every generation of
every output by default — recompute-avoidance is the whole point of the
engine, and old bytes are cheap insurance against re-running non-idempotent
steps (LLM calls, scrapes). Retention is the mechanism for when old bytes
stop being worth their storage: it deletes **bytes, never facts**. Every
ledger row, lineage edge, and lifecycle event survives forever; only the
stored payloads of sufficiently old outputs are removed, and every removal
is itself recorded.

This note explains the model, the two policies, what a prune actually does,
the guarantees, and the one trade-off you accept by turning it on.

## Why "old runs," not "unreferenced objects"

A classic garbage collector asks "which objects does nothing reference?" —
and in Rubedo's steady state the answer is *almost none*. Current
generations are live by definition, and even outputs whose input lanes
vanished stay live on purpose (the keep-orphans decision,
`producer-model.md` Q2): if the data comes back, the cache is still warm.
So sweeping unreferenced bytes reclaims nearly nothing.

What actually accumulates is **history**: superseded generations from runs
where inputs or code have since changed, and orphaned outputs that only
old runs ever used. Retention therefore prunes by **run recency**. The
unit of the policy is a *run*, not an object: everything the last N runs
of a pipeline touched is untouchable; everything older is fair game.

## The two policies (and the two that were rejected)

1. **Per-pipeline keep-last-N-runs** — `pipeline(..., retention=N)`.
   The pipeline keeps every materialization referenced by its last N
   terminal runs (N ≥ 1; the latest run always survives). This is the
   set-and-forget policy: it lives in code next to the steps, is recorded
   in each run's `definition()` snapshot, and is applied automatically at
   the end of each successful run.
2. **Global byte budget** — `rubedo gc --max-bytes 2GiB [--delete]` /
   `gc(max_bytes=...)`. After applying every pipeline's recorded
   retention, prune the *oldest-referenced* outputs across all pipelines —
   never anything a pipeline's latest run used — until the store fits.

Deliberately absent: **age-based knobs** (redundant with keep-last-N for
run-shaped churn) and **per-pipeline byte budgets** — the store dedupes
identical bytes *across* pipelines, so one physical object can belong to
several pipelines at once and "this pipeline's bytes" has no crisp
meaning. Run counts are per-pipeline; bytes are only global.

**Default: keep everything.** With no `retention=` set, nothing is ever
deleted; once the store crosses ~1 GiB a one-line warning at the end of a
run points at `retention=` / `rubedo gc` (the size estimate is cached and
recomputed at most hourly — no per-run stat storm).

## What a prune actually does: demote, then sweep

Every prune — auto or manual — runs the same two phases, and both are
driven entirely by the ledger (the store directory is never enumerated to
decide anything):

**Demote.** Compute the keep-set: materializations referenced by the
pipeline's last N terminal runs (via `RunCoordinateStatus`), plus every
expand cache anchor (see below). Every still-live materialization of that
pipeline *outside* the keep-set is flipped `is_live=False`, each flip
paired with a `pruned` lifecycle row in the same transaction — the same
append-only bookkeeping as invalidation and supersede (the pairing guard
enforces it mechanically; see `notes/invariants.md`).

**Sweep.** A physical object's bytes are deleted only when **every**
materialization referencing that content hash — across all pipelines, all
steps, all history — is now non-live. One live reference anywhere keeps
the bytes: because the store dedupes identical content, "this
materialization is pruned" never implies "its bytes are unreferenced."
Each deleted object is logged in the append-only `object_reclamations`
table *before* the file is unlinked, so the ledger stays the truth about
the store even if an unlink fails.

The demote/sweep split is why retention composes with everything else:
demotion is ordinary liveness bookkeeping the rest of the engine already
understands (planning simply sees no live materialization and recomputes),
and the sweep is a pure consequence of liveness.

## Triggers

- **End of run (auto-prune).** A pipeline with `retention=N` prunes
  *itself* after each successful run — never after a failed run (its
  keep-set could be incomplete), and it silently skips (with a note) if
  any other run's heartbeat is live. It deletes for real; that is the
  point of a persisted setting.
- **`rubedo gc` / `gc()` (manual).** Applies every pipeline's recorded
  policy — read from each pipeline's latest run's definition snapshot, so
  the CLI never imports user code — plus the optional byte budget.
  **Dry-run is the default**: `rubedo gc` prints exactly what `--delete`
  would demote and delete, and touches nothing. The dry-run and the
  delete share one planner, so the correspondence isn't a promise, it's
  the same code path.

## Guarantees

- **The latest execution and the latest *full* (`kind='process'`) run of
  every pipeline always survive**, under both policies. Thus a fresh partial
  trial remains reusable, while that newer `kind='partial'` run cannot
  displace the authoritative full snapshot from the keep-set or global-budget
  protection.
- **Ledger rows are never deleted.** A pruned generation keeps its
  materialization row, its lineage edges, its index entries, and gains a
  `pruned` lifecycle row. `trace` still walks through it; only the payload
  itself reads as absent.
- **Shared bytes are safe.** The sweep ref-counts across the entire
  ledger; a single live reference in any pipeline keeps the object.
- **Expand anchors are always kept.** Expand reuse hangs off a
  parent-addressed cache-anchor materialization that no run-status row
  references; the keep-set includes all such anchors unconditionally,
  because pruning one would silently re-run the scrape/LLM it caches —
  the exact cost the engine exists to prevent. (Anchors are tiny: a JSON
  list of child content hashes.)
- **No deletion while anything runs.** `gc --delete` refuses (exit 1) and
  auto-prune skips while any run's heartbeat reads "running" — this
  closes the restore race, where a concurrent run's exists-check passes
  just before GC unlinks the file and the run then commits a live
  materialization pointing at nothing. Dry-run is always allowed.
- **Deliberate ≠ corruption.** `rubedo du` reports *reclaimed* objects
  (absent from disk, logged in `object_reclamations`) separately from
  *missing* ones (absent and unlogged — genuine corruption).

## The trade-off you accept

Retention softens the keep-orphans default: outputs that only old runs
referenced get pruned even though their inputs *might* return. If a pruned
lane's input does reappear — a file comes back, a row's content reverts —
the next run **recomputes** it: a cache miss, the non-idempotent cost paid
again. The heal is lazy and safe: the recompute rewrites the missing bytes
(same content hash, same object path) and the pruned ledger row is
*restored*, resuming its history. With `retention` unset, the keep-orphans
default stands and this trade-off never applies.

Rule of thumb: set `retention=` on pipelines whose old generations you
would never resurrect (periodic scrapes, rolling reports); leave it unset
where inputs churn in and out and recomputation is expensive.

## Reading the aftermath

- `rubedo du` — sizes, live counts, the reclaimable estimate, and the
  reclaimed-vs-missing split.
- `materialization_lifecycle` — every prune is an `action="pruned"` row
  with reason `retention GC (auto_prune)` or `retention GC (gc)`, tied to
  the triggering run (manual `gc --delete` records a synthetic
  `kind="gc"` run).
- `object_reclamations` — one row per deleted object: content hash, byte
  count, trigger, run id, timestamp.
- A reclaimed hash that a later lazy-heal rewrote is simply present again
  and counts as a normal object; the old reclamation row remains as
  history and is ignored.

## Boundaries

Retention is not **invalidation**: `invalidate` marks specific outputs
wrong so the next run recomputes them (bytes usually stay); retention
removes the bytes of outputs that are merely old (their correctness is not
in question). Both demote liveness through the same paired-lifecycle
machinery; they differ in *why* and in what happens to the bytes.

The store supports a local filesystem backend and an S3-compatible
``ObjectStore`` (TODO item 7). Destructive ``gc(delete=True)`` hard-refuses
non-local stores until dry-run auditing and object-versioned buckets gate
it — S3/GCS deletes have no trash can. Cloud retention still demotes
liveness (``auto_prune``) without deleting bytes.

Implementation: `src/rubedo/gc.py` (module docstring covers internals),
`tests/test_gc.py` (each guarantee above is pinned by a test). Decision
history: TODO item 10b in `notes/TODO.md`'s Done changelog.
