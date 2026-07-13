# The Producer Model — design

Status: **✅ designed and shipped** (this doc is now the design + build log).
The whole line landed: content-addressed lanes → `expand` (cached) →
`group_key` reduce → multi-source → N-way `join`. It superseded `TODO.md`
item 1 (Joins) and the `expand`/`flat_map` bullet. Read `invariants.md` for
vocabulary first; the sequencing section at the bottom traces what was built.

## The move

Today the DAG is **coordinate-preserving**: every coordinate (lane key) is
minted by the `Source` at scan time, and steps are only `map` (1:1) or
`reduce` (N:1). This privileges two things — `Source` (the sole
coordinate-creator) and a source-only removal census (the sole
change-detector) — and it blocks everything data-dependent: `expand` (fetch
an RSS feed, *then* yield a lane per article) and `join` (both need
coordinate creation, and join needs two independent roots).

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

## Removal detection: a census that was tried, then dropped

Before this redesign, only the source detected removal: each run it took a
roll-call — "these coordinates exist now, with these content fingerprints" —
and diffed it against the previous run's chained roll-call. That machinery
(and the table it lived in) is gone; what follows is what motivated, and
eventually killed, extending the idea.

The generalization considered here was to give **every producer** its own
roll-call ("the lanes I emitted this run"), so removal would be a per-producer
diff and the cascade automatic — a removed input yields no output, so the
downstream roll-call shrinks and records the removal there too. The source
would just be the root of the cascade.

**Removal ≠ invalidation — two different axes.** Removal (a coordinate
vanishing) is a per-*lane* fact and correctly does **not** touch `is_live`,
because `is_live` is a property of a *materialization* (an address), not a
lane. Invalidation (a result is bad) is the per-*materialization* operation
that flips `is_live=False` (`invalidation.py:58`) + appends a lifecycle row. A
removed lane's materialization is still a valid result — some future lane may
re-address it — so keeping it live is right. These stay orthogonal.

**The generalization was never built.** See "Sequencing" step 2 below: silent
orphaning of a vanished lane already gives the same practical outcome as a
removal report, so the schema-touching bookkeeping — source-only or
generalized — wasn't worth it. A vanished coordinate today simply doesn't
appear in this run's lane statuses; nothing diffs against history.

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

**C — Caching is per-lane, no membership census.** An `expand`/`join` is
**not** one blob: each emitted lane content-addresses and reuses on its own
(unchanged from map). The membership bookkeeping this decision proposed (a
per-producer record of which lanes exist each run) was never built — see the
removal-detection section above and Sequencing step 2: silent orphaning of
vanished lanes turned out sufficient, so no census shipped for any shape.

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
   materialization and restores for free. It's also consistent with the
   bytes-are-disposable-facts-are-not promise (`notes/invariants.md`) — history
   is never silently deleted. The only cost is storage growth under
   heavy churn — a deferrable, opt-in retention/GC policy (age-out or
   ref-count), never a core concern.
3. **Fan-out bound — RESOLVED: no limit, by design.** An `expand` may yield
   unboundedly, deliberately past normal size constraints. No cap, no warn.
4. **Multi-root pipeline API — RESOLVED: no pipeline-level API at all.** Item
   14 settled it: a root is just any step with no `depends_on`, and a
   pipeline may declare as many as it likes (`@source` sugars a parentless
   `expand` root). `join`/multi-parent steps `depends_on` whichever roots
   they need — no dict, no per-step routing kwarg. See 4a below.
5. **Reduce/join removal — RESOLVED: just orphan.** A group that empties or a
   pair whose match vanishes simply stops being emitted; its old
   materialization orphans (live, unreferenced), exactly like a vanished
   expanded lane. No census, no removal report — consistent with the step-2
   decision that reporting a removal is a low-value notification, not an
   operation. The `ledger.py:642` skip-reduce shortcut is fine to leave as-is.
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
`materializations`. They are not source coordinates, so the source's own
removal tracking never touches them; a vanished expanded lane simply becomes
orphaned-live — exactly the resolved Q1/Q2 behavior. So the schema-touching
per-producer census is **not** a prerequisite for expand.

**Revised sequencing — go vertical, skip the churn.** The behavior-preserving
`Source`→`Producer` refactor buys nothing until something uses the generality,
so it is premature abstraction. Instead ship the capability first and let the
census follow when a concrete need (expand *caching*) pulls it in.

**Expand emit contract (as shipped):**
- `@step(shape="expand")`; the fn yields bare payload values — no subkey, no
  pair.
- Minted coordinate: content-addressed by decision A, always —
  `row-<hash(value)[:12]>` (`expand_child_coord`, `planning.py`). Identical
  payloads collapse to one lane. Not identity — identity stays the
  content-addressed output address.
- Execution: `_expand_outcomes` (`execution.py`) yields one outcome per
  distinct emitted value; each becomes its own materialization,
  `MaterializationEdge`-linked to the parent.

**The one real fork — expand caching.** An expand is 1:N, so it cannot cache
by a single `output_address` like a map, and re-running a non-idempotent expand
(LLM) every run is unacceptable. To reuse when the parent lane is unchanged we
must remember what the expand emitted for that parent (its membership). Two
options: **(a)** a minimal anchor now — one record addressed by `(step,
parent-content-hash)` storing the list of child content hashes, enabling
"parent unchanged → replay emitted lanes without running"; ships with expand.
**(b)** the full per-producer census — more work, and only pays for itself if
something besides expand needs it. **Recommend (a)**, revisit (b) only on
demand.

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
   *(`sources.py` and its `CsvSource`/`TableSource` classes were later
   deleted wholesale by item 14 — the sources purge — and replaced by
   `@source` recipes, `docs/concepts/sources.md`; the content-addressing
   behavior described above is unchanged, just moved into plain generator
   code.)*
1. **`expand`** (`shape="expand"`, 1:N minting) — ✅ **DONE, cached**. A step
   yields bare payload values (no subkey, no pair); each distinct value mints
   a content-addressed `row-<hash>` lane with address `hash(step, version,
   child-content-hash[, params])` — the child's identity is its own content,
   so identical children collapse and there is no parent/subkey in the
   identity — an edge to the parent, and normal downstream chaining.
   **Caching (the key insight, owner-driven):** an expand also stores its full
   yielded list as a **cache anchor** — one materialization addressed by the
   *parent* (`hash(step, version, parent-content)`), which *is* predictable
   from the parent. So on re-run with an unchanged parent, planning finds the
   anchor live and replays the child lanes as reuse decisions **without running
   the fn** (`_plan_expand_reuse`). The anchor is stored but is not a lane — no
   status/count/edge/coord_step_mats (`is_anchor`). This makes expand correct
   for **scraping**: scrape once, cache, don't re-run; `stale_after` on the
   expand gives periodic re-scrape for free. No schema change; vanished lanes
   orphan (Q1/Q2). Verified: `tests/test_expand.py` (8) + full suite (154)
   green, ruff clean, and a live non-deterministic "scrape" runs exactly once
   across two runs (3 article lanes cached→reused).
   **Known cost — since resolved:** as shipped this was option **(a)** — the
   anchor stored the full yielded list *and* each child was extracted into its
   own materialization, so scraped data was stored twice; option **(b)**
   (children as views into the anchor) was deferred to `TODO.md` item 11. The
   unification Phase 1 (`2850e74`, 2026-07-06) then slimmed the anchor to the
   child *content hashes*, which ended the double storage as a side effect —
   payloads live once in the child materializations, the anchor is a tiny hash
   list, and item 11 is retired. The behavior-preserving `Producer` refactor
   stays **dropped** as premature.
2. **Per-producer census** — ❌ **dropped from the critical path (owner call),
   then the source-only version dropped too.** A removal report never deletes
   or unlives anything — it was only ever a notification. Minted lanes
   already orphan silently (they're not tracked by any removal report), and
   silent orphaning is functionally identical to reporting "removed." So the
   census — whose only job was extending that report to minted/group lanes —
   bought nothing worth a schema change, and the original source-only version
   was later removed as well: today there is no removal report anywhere, only
   silent orphaning. If "what orphaned?" is ever wanted, it's the
   orphan/lane-following tooling in `TODO.md` item 5, not a census.
3. **`group_key` reduce** — ✅ **DONE**. `@step(shape="reduce",
   group_key="field")` partitions the reduction's parent lanes by a named
   `@step(index=[...])` field of the parent output, emitting one output per
   group (coordinate = the group value); `group_key=None` is the old single
   `@all`. Grouping reads `MaterializationIndexEntry` rows at plan time — no
   value reads, plan stays value-free (`_group_reduce_lanes`,
   `_reduce_group_decision`). A lane with several values for the field joins
   each group (list-valued index); a lane with none raises (index it on the
   parent). Also fixed reduce to gather lanes from `coord_step_mats` rather
   than only source coordinates, so a reduce now folds in **minted/expanded
   lanes** — `expand → group_key reduce` works. A vanished group just orphans
   (step 2). Execution/ledger unchanged (a group is a decision whose
   `parent_mats` is scoped to that group). Verified: `tests/test_group_key.py`
   (6) + full suite (160) green, ruff clean, and a live feed → 5 story lanes →
   per-topic digest pipeline.
4. **Multi-root + `join`** — the finale, split in two:
   - **4a. Multi-source** — ✅ **DONE, then subsumed by item 14.** What
     shipped here (`pipeline(sources={name: Source})`, a named-sources dict,
     and `@step(source="name")` routing) is gone: item 14 deleted the
     `Source` protocol entirely, and with it the routing kwarg. Today
     "multi-source" is just several parentless `@step(shape="expand")` roots
     (`@source`) declared in the same pipeline — no pipeline-level kwarg, no
     per-root routing. A downstream step names whichever root(s) it needs in
     `depends_on`; coordinates never collide because `coord_step_mats` is
     keyed by `(coord, step)`. Live-verified in
     `examples/newsroom/newsroom.py` (two `@source` roots joined on
     publisher) and covered by `tests/test_join.py`.
   - **4b. `join`** — ✅ **DONE, N-ary**. `@step(shape="join",
     depends_on=[a, b, ...], join_on={a: field, b: field, ...})`: an **N-way**
     equijoin matching each side's `@step(index=[...])` field by value
     (plan-time, value-free, like `group_key`). It buckets each side by its
     key, intersects on shared values, and mints one lane per matched tuple
     (cartesian within a shared value) with coordinate `a|b|...`. 2-way is the
     common case; `join_on={a:"uid", b:"uid", c:"uid", d:"uid"}` is a 4-way
     star. Joins on *different* pairwise keys compose by chaining join steps;
     predicates are a `filter` after the join. Planning-only
     (`_plan_join`/`_join_pair_decision`) — execution/ledger treat a matched
     tuple as a multi-parent map decision, so a join lane edges to all its
     sides. Verified: `tests/test_join.py` (5) incl. the 4-way star + full
     suite (170) green, and a live two-source order↔customer enrichment.

**The producer-model line is complete:** content-addressed lanes → `expand`
(cached) → `group_key` reduce → multi-source → N-way `join`. Every shape
(`map`/`filter`/`expand`/`reduce`/`join`) is now a producer, exactly as the
taxonomy predicted.
