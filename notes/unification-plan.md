# Unification plan: source ≡ root expand

The north-star simplification the producer model was always heading toward.
Execute in phases; each phase is independently shippable (green suite + a live
example) and committed. Read `producer-model.md` for how we got here.

## North star

**One producer primitive. Everything is a step; some steps are roots.**

- A **source is a root expand** — an `expand` step with no `depends_on`. It
  reads the world and yields payloads, exactly like any expand yields payloads.
- **Every lane is content-addressed.** Coordinate = `row-<hash(payload)>`.
  There is no `key=`. A human handle (email, path, id) is just a field you
  `index=[...]` and query — never the coordinate.
- **No `Source` type.** `pipeline(steps=[...])`; a root expand is the entry.
  `CsvSource`/`folder=`/`@source` survive only as thin sugar that *build* a
  root-expand step.
- **`removed` and the per-source manifest go.** "Current" is redefined as the
  latest run's lanes, so a vanished item just isn't there — no bookkeeping.

Net deletions: `Source` ABC, `SourceItem`, `scan`/`load`, `source=`/`sources=`,
`key=`/`_finalize` uniqueness, `Manifest`/`ManifestEntry`/`source_id`,
`_snapshot_source`, `removed` status. A lot of concept for a lot less surface.

## The unified model

A step is a producer over its input lanes:

| shape | in | out | root form (no `depends_on`) |
|---|---|---|---|
| `map` | 1 lane | 1 value | — (a root must mint the *set*, so roots are `expand`) |
| `expand` | 1 lane | N payloads (`yield`) | **the source**: reads the world, mints the initial lanes |
| `reduce` | N lanes | 1 per group | — |
| `join` | N roots | 1 per match | — |

**The one real rule that makes root-expand behave like a source:** a producer
with no parent has nothing to cache its emission against, so **it re-runs its
body every run** (re-enumerate → catch new/changed/removed data). `stale_after`
becomes the opt-in "cache the enumeration for N minutes" TTL — a capability
sources don't have today.

## Phases

Order matters: align the contract, prove root-expand alongside the old path,
migrate, then delete. The map-a-folder common path stays working throughout.

### Phase 1 — Expand yields payloads, content-addressed  ✅ DONE
`expand` now `yield`s a payload (not `(subkey, value)`); each child is a
content-addressed lane `row-<hash(value)>`, identical payloads collapse, child
identity = the value's content hash (no parent/subkey). The anchor stores the
child *hashes* (keyed on parent content for cache reuse) — which also ends the
double-storage since it no longer holds the values. `execution._expand_outcomes`,
`planning.expand_child_identity`/`expand_child_coord`/`_plan_expand_reuse`,
`tests/test_expand.py` (+ a collapse test replacing the subkey-collision one),
`tests/test_group_key.py`, `examples/{expand_feed,newsroom}`. Full suite green
(170); both examples verified created→reused with no re-scrape. **Closed:** the
source⇄expand contract mismatch.

### Phase 2 — Root expand = source (new path, additive)  ✅ DONE
A `shape="expand"` step with no `depends_on` is now a pipeline root: it takes no
payload, **always executes** (the re-run rule — no parent to cache against, so
no anchor), and mints top-level `row-<hash>` lanes. `pipeline(steps=[...])`
works with no `source=` (validation: a source-less pipeline needs a root
expand). The old `source=` path is untouched — additive. `spec.py`
(expand allows 0–1 parents; `pipeline()`/`source_for`), `planning.py` (root
expand branch → one always-execute decision), `execution.py` (`call()` reads no
source, `_expand_outcomes` skips the anchor when parentless), `runner.py`
(`_source_name_for` returns None for root expands, empty-sources ok),
`tests/test_expand.py` (root-expand-as-source test). Full suite green (171); a
live `pipeline(steps=[fetch(root expand)→classify→group_key reduce])` verified
created→reused with the root re-scanning each run. **Closed:** the core of the
unification.

### Phase 3 — Content-address row sources; drop `key=`
Remove `key=` from `CsvSource`/`TableSource`; always content-address
(coordinate = `row-<hash(row)>`). `_finalize` collapses to a plain
content-dedup (its uniqueness error can't occur without keys). Touch:
`sources.py`, `tests/test_sources.py`, `tests/test_table_source.py`, and every
example/test passing `key=` (`gutenberg`, `weather`, `github`, `orders`,
`newsroom`, `test_join`) — the joins still match on **indexed fields**, so they
keep working once the join keys are indexed. **Closes:** `key=` removal,
`_finalize` simplification. *Decision D1 (folders) deferred to Phase 6.*

### Phase 4 — `@source` + built-ins as root-expand sugar; migrate examples
Add `@source` = sugar for a root expand (`@step(shape="expand")`, no deps),
content-addressed, id defaulted to qualname. Reimplement `CsvSource`/
`TableSource`/`FolderSource`/`folder=` as helpers that **return a root-expand
step** (or a Source adapter the engine runs as one). Migrate examples to the
root-expand idiom where it reads cleaner; keep `source=` as sugar. Touch:
`sources.py`, `spec.py`/`__init__.py` (export `source`), examples, a new
`examples` showing `@source`. **Closes:** Tier-1 TODO #2 (`@source`).

### Phase 5 — Reconsider `removed`/manifest/`source_id`
Redefine **Current Outputs** as the latest run's live coordinates (server
query), so a deleted item simply isn't current — no `removed` needed. Then
delete `_snapshot_source`, the `removed` status path, `Manifest`/
`ManifestEntry`, and `source_id` (or keep it purely for display). Schema change
→ `.rubedo` wipe + repopulate. Touch: `ledger.py`, `runner.py`, `models.py`,
`server.py`, `web/` (drop the Removed column/stat), tests. **Closes:** the
`removed`/manifest deletion. *Decision D2.*

### Phase 6 — Delete `Source`; `pipeline(steps=[...])` only; folders
Once built-ins are sugar and nothing needs the protocol, delete the `Source`
ABC, `SourceItem`, `scan`/`load`, `coerce_source`, and `source=`/`sources=`
(or leave `source=` as a one-line alias). Decide folders (**D1**): content-
address (path → indexed field, identical files collapse) vs. keep path as the
one legible coordinate. Touch: `sources.py`, `spec.py`, `runner.py`,
`selection.py`, docs. **Closes:** the big deletion; the model is now literally
"steps, some of which are roots."

### Later (fold in as we go)
- **Streaming/chunked scan** — consume the root-expand generator in bounded
  chunks instead of a list (pairs with non-topological execution).
- **Lazy large sources** (folders/S3) — the change-token + on-demand fetch form
  for sources where obtaining an item is the cost (ties to cloud-source TODOs).
- **Lane-following tooling** (TODO 14) — becomes load-bearing once coordinates
  are opaque hashes: it's how you answer "which row is this?"
- Independent, slot anytime: data-quality assertions, mypy/`py.typed`.

## Open decisions (confirm before the phase that needs them)

- **D1 — Folders.** Content-address (lose path-as-coordinate; path becomes an
  indexed field; identical files collapse to one lane) *[proposed]*, or keep
  the path as the one exception (folders stay "keyed"). Blocks Phase 6.
- **D2 — Delete `removed`/manifest** and redefine current = latest run
  *[proposed]*. The only thing it costs: cross-run "what got deleted" is no
  longer a stored fact (rederivable by diffing runs). Blocks Phase 5.
- **D3 — Delete `Source` outright** vs. keep `source=`/`CsvSource` as permanent
  sugar over root expands *[proposed: keep the sugar, delete the ABC/protocol]*.
  Shapes Phase 6.
- **D4 — Lazy sources.** Defer entirely to the streaming/cloud work
  *[proposed]*; eager root expands (yield payloads) are the only form until
  then. Note: **folders** genuinely want lazy (don't load every file to hash),
  so D1+D4 interact — content-addressing folders eagerly means reading file
  contents at scan.
- **D5 — Expand loses legible child coordinates** (`parent/f1-0` →
  `parent/row-<hash>`) *[proposed: yes, content-address]*. Blocks Phase 1.

## Sequencing note

Phases 1→2 are low-risk and prove the idea. Phase 3 is broad but mechanical.
Phase 4 is where users feel the win. Phases 5–6 are the irreversible deletions
— do them last, deliberately, once the new path carries everything.
