# The Producer Model — design

Status: **owner design session in progress.** This supersedes the direction
notes in `TODO.md` — item 1 (Joins) and the `expand`/`flat_map` bullet in
item 2. Nothing here is built yet. Read `invariants.md` for vocabulary first.

## The move

Today the DAG is **coordinate-preserving**: every coordinate (lane key) is
minted by the `Source` at scan time, and steps are only `map` (1:1) or
`reduce` (N:1). This privileges two things — `Source` (the sole
coordinate-creator) and `Manifest` (the sole census) — and it blocks
everything data-dependent: `expand` (fetch an RSS feed, *then* yield a lane
per article) and `join` (both need coordinate creation, and join needs two
independent roots).

The generalization: **stop privileging `Source`. There are only producers
that emit keyed items; a `Source` is the producer that emits from no input.**
Coordinate creation moves into the core. `Source`, `map`, `filter`, `expand`,
`reduce`, `join` all become instances of one primitive.

## The producer

```
produce(inputs: {coord -> value}) -> Iterable[(coord, value)]
```

| kind    | input arity | collective? | cardinality | coords    |
|---------|-------------|-------------|-------------|-----------|
| source  | 0 (nullary) | —           | 0 → N       | **mints** |
| map     | 1           | no          | 1 → 1       | preserves |
| filter  | 1           | no          | 1 → {0,1}   | preserves |
| expand  | 1           | no          | 1 → N       | **mints** |
| reduce  | 1 (grouped) | **yes**     | N → groups  | mints (group key) |
| join    | 2+ roots    | **yes**     | N×M → pairs | **mints** |

Two axes carry all the meaning: **collective?** (can this be computed one
input lane at a time, or does it need the whole input set first — a DAG
barrier) and **cardinality** (does it preserve, mint, or collapse
coordinates). The named "shapes" are sugar over `(arity, group_key,
cardinality)`.

### The distinction that matters most: diamond ≠ join

A **diamond** — a step with two parents that share a coordinate lineage (both
descend from the same root) — is **not** a join. It is a *multi-input map*:
per-lane, coordinate-preserving, joins its parents by **inherited coordinate
equality**. This already works today (`planning.py:289` reads
`coord_step_mats[(coord, dep)]` for each parent) and needs nothing new but
`hash_on`.

The natural shape (no synthetic passthrough): `A` parses the source into
`{doc, customer}`; `B` depends on `A` with `hash_on=["doc"]`, so it **dedupes**
across customers by projecting `customer` out of its identity; `C` depends on
`(A, B)`, reads `customer` from `A` and the fetched result from `B`, and
**forks** per customer. A, B, C share one lineage from the same root, so C
pairs them by inherited coordinate — a real step `A` you already have carries
the metadata, so nothing has to be threaded through `B`.

A **join** combines two lane sets from **different roots**, whose coordinates
are unrelated, so coordinate-equality is meaningless. It must match on a
**declared key**, and it **mints** new pair coordinates. It is collective (you
need both full sets to match) and coordinate-creating. That is why join is not
a kind of reduce — reduce *collapses*, join *expands*. Join is the binary,
collective member of the **expand** family, which is exactly why putting
coordinate-creation in the core is what unblocks it.

## What a manifest is (and the per-producer generalization)

A manifest is a **remembered census**, chained run-over-run. Each run a
producer takes a roll-call — "these coordinates exist now, with these content
fingerprints" — and stores it (`Manifest` + `ManifestEntry`,
`parent_manifest_id` links the chain). Its sole job is to detect **removal and
change**, which content-addressing cannot see on its own: an address hit means
reuse and a miss means create, but a *vanished* coordinate leaves no trace in
the current scan. So you diff this run's census against last run's; anything
present before and absent now is `removed`.

Today only the source keeps a census (`_snapshot_source`, `ledger.py:585`).
Generalized: **every producer keeps its own census** ("the lanes I emitted
this run"), and removal is a per-producer diff. The cascade is automatic — a
removed input yields no output, so the downstream census shrinks and records
the removal there too. The source is just the root of the cascade.

**Removal ≠ invalidation — two different axes.** Removal (a coordinate
vanishing) is a per-*lane* fact: it records a `removed` status + event and
correctly does **not** touch `is_live`, because `is_live` is a property of a
*materialization* (an address), not a lane. Invalidation (a result is bad) is
the per-*materialization* operation that flips `is_live=False`
(`invalidation.py:58`) + appends a lifecycle row. A removed lane's
materialization is still a valid result — some future lane may re-address it —
so keeping it live is right. These stay orthogonal in the producer model.

## Decisions (locked this session)

**A — Coordinate assignment: content-addressed by default, optional declared
key.** A minted lane's key is `hash(emitted value)` unless the producer
declares a key extractor. One rule at every mint site (source, expand, join),
identical to the source `key=` decision already taken for the
content-addressed-lane refactor. Identical content collapses to one lane and
reuses; an edit reads as removed + added.

**B — Join is an equijoin on a declared, hashed key.** Each side declares a
key extractor; the engine hashes both sides' keys and bucket-matches; it emits
one pair lane per matching `(left, right)`. The **pair coordinate is the match
key** (content-disambiguated when a side's key is not unique). No arbitrary
pair predicate in the core — that is the N×M explosion; a predicate is
expressed as a `filter` step *after* the join. Matching is answered entirely
by hashes, so it stays cheap.

**C — Caching is per-lane, plus a membership census.** An `expand`/`join` is
**not** one blob. Each emitted lane content-addresses and reuses on its own
(unchanged from map), and the producer's manifest records **which** lanes
exist this run. Reuse survives; membership is the only new bookkeeping.

**D — `reduce` gains a `group_key`.** Today's single `@all` lane is
`group_key = ()` (one total group). A declared group key gives per-group
aggregation (N → one-per-group) for free under the collective-producer
framing.

**E — Worth it.** The generalization *deletes* the Source/step distinction,
the source-authoritative-scan invariant, and the map/reduce/expand/join zoo
(all become one producer). It *adds* per-producer census bookkeeping, a
removal cascade with more moving parts, and — the real cost — it makes
**lineage load-bearing for comprehension, not just queries**: "why does lane X
exist?" is no longer "it's in the source folder" but "step 3 emitted it from
input Y," visible only via `MaterializationEdge`. Accepted, on the condition
that the map-a-folder common path stays a one-liner via defaults — the general
core must never leak into the simple case.

## What changes in the code (map, not a plan)

- **`spec.py`** — shapes become producer kinds; add `expand`/`join` specs, a
  `group_key` on reduce, and key extractors (`key=` / `on=`).
- **`sources.py`** — `Source` becomes the nullary root `Producer` (its
  `scan()` is `produce({})`); `PipelineSpec.source` (singular) becomes a set
  of root producers, which is also what multi-root/join needs.
- **`planning.py`** — dependent-step target gathering already reads
  `coord_step_mats`; add coordinate-minting for expand/join, group_key for
  reduce. The in-memory join table handles new coordinates unchanged — it is
  just more entries.
- **`ledger.py`** — `_snapshot_source` → `_snapshot_producer`, per
  `(run, step)`; removal diff per producer. Possibly fold the census onto
  `run_coordinate_statuses`, which already records `(run, coordinate, step)`.
- **`models.py`** — `Manifest` gains a producer/step dimension.

## Open questions (owner)

1. **Invalidation cascade — RESOLVED: lazy-via-recompute.** `invalidate()`
   flips `is_live` on the *selected* materializations only; dependents
   reconcile on the next run via recompute + content-addressing. This is the
   whole core mechanism — invalidate a specific bad case and let it
   recompute. No eager descendant cascade (it would re-run
   expensive/non-idempotent descendants that recompute identically). Broader
   lane-level invalidation is a *tooling* concern, deferred to `TODO.md`, not
   core.
2. **Orphan retention — RESOLVED: not a correctness issue, keep them.** A
   removed lane's materializations stay `is_live` and unreferenced. This is
   harmless (nothing computes against them) and actually *beneficial*: a
   removed lane that later returns with the same content re-addresses the live
   materialization and restores for free. It's also consistent with invariant
   7 (never silently delete history). The only cost is storage growth under
   heavy churn — a deferrable, opt-in retention/GC policy (age-out or
   ref-count), never a core concern.
3. **Fan-out bound — RESOLVED: no limit, by design.** An `expand` may yield
   unboundedly, deliberately past normal size constraints. No cap, no warn.
4. **Multi-root pipeline API.** How do you declare two roots + a join in
   `pipeline()` once `source=` is plural?
5. **Reduce/join removal → the per-producer census.** Group-key reduce breaks
   the `ledger.py:642` skip-reduce shortcut: reduce now emits one lane per
   group, so a group that empties is a removal — but today's loop only diffs
   *source* coordinates against the *source* scan, and group lanes are
   neither. Resolved by giving reduce (and join) its own census; a vanished
   group / unmatchable pair is detected there and reads as `removed`
   (graceful, not an error). This is a concrete forcing function for the
   per-producer census, not incidental.
6. **Group-key stability** under content-addressed lanes (the same
   removed-vs-changed churn as A, one level up).

## Step 2+ design (grounded in the runner)

Reading the runner before building step 2 changed the plan — two findings:

**Finding 1 — the runner already interleaves plan→execute per step and feeds
executed results forward.** `run_pipeline` loops `for step in topo: plan →
execute` (`runner.py:294-313`), and `_commit_execution_result` writes each
executed `MatRef` into `coord_step_mats[(coord, step)]` (`ledger.py:559`),
which the next step's `_plan_step` reads to find its parents
(`planning.py:267-280`). So a coordinate-**minting** step needs almost no new
scheduling: an `expand` executes and writes *N* entries for *N* minted
coordinates instead of one; the next step gathers them like any parent's. In
`plan()` (dry-run) expand can't run, so downstream is `pending` — the existing
mechanism. **Expand needs no new execution model.**

**Finding 2 — expand needs no schema change and no census for a correct MVP.**
Minted coordinates are just strings in `run_coordinate_statuses` +
`materializations`. They are not source coordinates, so source-manifest removal
never touches them; a vanished expanded lane simply becomes orphaned-live —
exactly the resolved Q1/Q2 behavior. So the schema-touching per-producer census
is **not** a prerequisite for expand.

**Revised sequencing — go vertical, skip the churn.** The behavior-preserving
`Source`→`Producer` refactor buys nothing until something uses the generality,
so it is premature abstraction. Instead ship the capability first and let the
census follow when a concrete need (expand *caching*) pulls it in.

**Expand emit contract (proposed):**
- `@step(shape="expand")`; the fn returns an iterable of `(subkey, value)`.
- Minted coordinate: content-addressed by decision A — `row-<hash(value)>` by
  default; the fn's `subkey` may seed a lineage-legible `f"{parent}/{subkey}"`
  when declared. (Not identity — identity stays the content-addressed output.)
- Execution: `_execute_step` yields one outcome per emitted pair;
  `_commit_execution_result` writes one materialization per minted coordinate,
  each `MaterializationEdge`-linked to the parent.

**The one real fork — expand caching.** An expand is 1:N, so it cannot cache
by a single `output_address` like a map, and re-running a non-idempotent expand
(LLM) every run is unacceptable. To reuse when the parent lane is unchanged we
must remember what the expand emitted for that parent (its membership). Two
options: **(a)** a minimal membership record now — a small table keyed
`(step, parent_coord) → {subkey: content_hash}` enabling "parent unchanged →
replay emitted lanes without running"; ships with expand. **(b)** the full
per-producer census now — more work, but it is coming anyway. **Recommend (a)**,
generalized to (b) when the census lands.

**`group_key`'s distinct constraint (later):** grouping parent lanes by a field
of their *value* needs values at plan time, but planning only reads content
hashes. So `group_key` must either group by coordinate / an indexed field, or
reduce-planning must read parent values (tolerable, since reduce is already a
barrier). Decision deferred to that increment — and it is why `expand`, not
`group_key`, is the cleaner next step.

## Sequencing

*(revised after the runner findings above)*

0. **Content-addressed lanes** (drop `_disambiguate`, make `key=` optional) —
   the substrate; stands alone. ✅ **DONE** (`sources.py`). `key=` is now
   optional on `CsvSource`/`TableSource`: omit it for content-addressed
   `row-<hash>` lanes (identical rows collapse, edits read as removed+added);
   declare it for stable legible lanes (coordinate = key value). A declared
   key that maps to two *different* rows now **raises** instead of silently
   content-suffixing (`_disambiguate` and the `key_collision` flag are gone,
   replaced by `_finalize`, which dedups identical lanes and enforces
   uniqueness). Streaming `TableSource` (`batch_size`) requires a key, since
   `load()` re-fetches by it. No schema change. Verified: full suite green
   (146), ruff clean, and a live keyless CSV pipeline caches created→reused
   with an identical duplicate row collapsing to one lane. Querying keyless
   lanes by their human handle is deferred to the index/lineage tooling
   (`TODO.md` item 5) — keyed lanes stay legible so nothing regresses there.
1. **`expand`** (`shape="expand"`, 1:N minting) — the flagship capability, next
   up. Rides the interleaved runner (Finding 1); content-addressed minted
   coordinates; a minimal membership record for caching (fork option **a**);
   no schema change, orphan-on-removal. The behavior-preserving `Producer`
   refactor is **dropped** as premature — the Source/step unification stays
   conceptual until something needs it.
2. **Per-producer census** — upgrades expand caching to its general form and
   adds removal *reporting* for minted/group lanes. Schema change (`Manifest`
   gains a producer dimension) → `.rubedo` wipe. Behavior-improving, pulled in
   by expand's caching need rather than done speculatively.
3. **`group_key` reduce** — with the "group by what" decision (plan-time value
   access) settled first.
4. **Multi-root + `join`** — binary collective expand, once roots are plural.
