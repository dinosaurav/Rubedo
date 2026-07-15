# Search & Surgical Invalidation

Every output is stored at a content-addressed, opaque address — great for
caching, useless for asking "which rows mention `acme`?" or "recompute
everything for `region=EU`." Indexing bridges that gap: it makes outputs
findable by what a step *computed*, not by file name or row position. This
page covers `@step(index=[...])`, the `Selection` query language, and the
two ways to act on a selection: `invalidate()` and `trace()`.

## Indexing: `@step(index=[...])`

```python
@step(index=["company", "meta.region"])
def extract(row: dict):
    return {"company": row["company"], "meta": {"region": row["region"]}, ...}
```

`index` names fields of the step's **output value** to extract into a
search index at commit time. Dotted paths reach into nested dicts
(`"meta.region"` pulls `value["meta"]["region"]`); a list-valued field
indexes one entry per element, so a step that returns `{"tags": ["a", "b"]}`
with `index=["tags"]` is findable by `tags:a` *or* `tags:b`.

Indexing is purely operational:

- It never touches cache identity — adding, removing, or changing `index=`
  doesn't change a step's output address and doesn't force a recompute.
- Only *newly created* materializations get indexed under a new
  declaration; outputs that already existed before you added `index=` stay
  unindexed until they're recomputed (bump `version`, or use `invalidate()`,
  if you need old rows searchable retroactively).
- It's incompatible with `skip_cache=True` — nothing is stored to search, so
  the combination is rejected at pipeline-build time.

`group_key` (on a `reduce` step) and `join_on` (on a `join` step) both read
indexed fields at *plan* time to decide which lanes belong together — see
[`../concepts/shapes.md`](../concepts/shapes.md).

### Labels are just indexed data

A "label" isn't a separate concept — it's whatever you chose to put in
`index=`. It's **non-unique** (many rows can share `company:acme`),
**multi-valued** (a list field indexes every element), attachable at *any*
step in the DAG (not just roots), and it is **never part of cache
identity** — two rows with identical content but different index values
would still collapse to the same lane if their content matches; indexing
only affects what you can find, not what recomputes.

## The `Selection` query language

```python
from rubedo import Selection

Selection(index={"company": "acme"})                        # programmatic
Selection.parse("step:extract company:acme live:true")      # query string
```

`Selection.parse()` splits the query on whitespace into `key:value` terms
(quote values containing spaces: `company:"acme corp"`). Some prefixes are
**reserved** — they map to engine facts, read straight off the
`Materialization` row — and everything else is treated as an indexed-field
match:

| Term | Matches | Notes |
|---|---|---|
| `source:<id>` | `Materialization`'s source, via a `RunCoordinateStatus` join | |
| `coord:<glob>` / `coordinate:<glob>` | the lane's coordinate, glob-matched | `fnmatch`-style: `coord:*.txt`; matched against the *latest* recorded coordinate for that materialization |
| `step:<name>` | `Materialization.step_name` | exact match |
| `pipeline:<id>` | `Materialization.pipeline_id` | exact match |
| `version:<v>` | `Materialization.code_version` | exact match, e.g. `version:1.0.0` |
| `version:<range>` | `Materialization.code_version` | when the value starts with `<`, `>`, `=`, or `!`, parsed as a [PEP 440](https://peps.python.org/pep-0440/) specifier via `packaging.SpecifierSet` — `version:<2.0`, `version:>=1.5,<2.0` |
| `address:<hash>` | `Materialization.output_address` | exact match |
| `live:true` / `live:false` | `Materialization.is_live` | `live:false` selects invalidated/superseded/pruned generations |
| anything else, `field:value` | an indexed field (`@step(index=[...])`) | `MaterializationIndexEntry(field, value)`; every other term in the query must also match (AND) |

A query can combine any number of terms; all of them must match. Reserved
terms are pushed down into SQL; `coord:` globbing and `version:` range
matching happen in Python after the SQL query returns (glob matching and
PEP 440 specifiers don't map cleanly onto `LIKE`).

!!! note "Two channels, one home each"
    Lane-key globs (`coord:`) answer *source-shaped* questions — "the file
    at this path," "this CSV row's key." Indexed fields answer
    *content-shaped* questions — "outputs where the step computed
    `company=acme`." They're deliberately separate: a coordinate is an
    engine-facing dataflow key (content-addressed by default, not a human
    label), and `index=` is the only supported way to search by what a step
    actually produced.

## `invalidate()`: a logical tombstone

```python
from rubedo import Selection, invalidate

invalidate(Selection(index={"company": "acme"}), reason="bad prompt")
```

```bash
rubedo invalidate "step:enrich company:acme" --reason "bad prompt"
```

`invalidate()` flips every matched materialization's `is_live` to `False`
and writes a paired `invalidated` lifecycle row — it **never deletes**
anything. The materialization row, its lineage edges, and its index entries
all survive; only its eligibility as "the current answer" changes. The next
`p.run()` sees no live materialization at that address and recomputes it —
recovery from an invalidation is never more than re-running the pipeline.

`reason` is required and is stored on the lifecycle row — it's what shows
up later in `trace --all` or a lifecycle audit, so make it useful ("bad
prompt," "source data was wrong," not "oops").

## Widening the blast radius: `downstream=True`

```python
invalidate(selection, reason="...", downstream=True)
```
```bash
rubedo invalidate "step:enrich company:acme" --reason "bad prompt" --downstream
```

By default, invalidation only touches what matched the selection directly.
`downstream=True` (CLI: `--downstream`) widens the tombstone to the **full
downstream closure**: every materialization derived — transitively, through
however many steps — from a matched lane, walked over the recorded
`MaterializationEdge` lineage. This is the same traversal `trace()` uses, so
the set `--downstream` invalidates is *exactly* the set `rubedo trace`
would show you as `seed` + `downstream` for the same query.

!!! warning "Preview before you widen — `trace` is the blast-radius report"
    `downstream=True` can invalidate a lot more than the seed count
    suggests, especially through a `reduce` or `join`: fan-in is honest,
    not surgical — **one bad lane inside a reduce's group or a join's match
    set contaminates the whole fan-in output**, so the fan-in
    materialization and everything derived from *it* also flip, even
    though only one of its many inputs was actually wrong. Always run
    `rubedo trace "<same query>"` first and read the `seed` / `upstream` /
    `downstream` counts before adding `--downstream` to an `invalidate`.
    There's no way to invalidate "downstream except the fan-in" — recovery
    is always a full re-run of the invalidated set, which is exactly what
    the recompute is for.

Upstream is never touched by `invalidate()` (with or without
`downstream=True`) — invalidating a `report` step's output never
invalidates the `extract` step that fed it, and it never deletes the bytes
of anything: `is_live=False` is a liveness flag, not a delete. If a pruned
or invalidated lane's *input* reappears unchanged in a later run, the
lane's old address is recomputed and, if it lands on identical bytes, the
generation is simply restored.

## `trace()`: read the blast radius before you touch it

```python
from rubedo import Selection, trace
print(trace(Selection.parse("company:acme")))
```
```bash
rubedo trace "company:acme"
```

`trace()` seeds on whatever a selection matches and walks
`MaterializationEdge` in both directions: **upstream** to the source items
everything derived from (roots resolve their stored payload, so you see
the actual input value, not just an address), and **downstream** to
everything derived from the seeds. It's the one-command answer to "this
output looks wrong — what produced it, and what did it contaminate?"

By default, only **live** materializations seed a trace — "the current
state of the world." Pass `include_superseded=True` (CLI: `--all`) to seed
history too: superseded, invalidated, and pruned generations. Either way,
traversal always follows the *real* derivation edges regardless of
liveness — a live output's recorded parent can itself be a superseded
generation (the parent was recomputed, but this output hasn't been
re-derived from the new parent yet), and hiding that edge would misrepresent
the derivation. Non-live nodes are marked `[not live]` in the output, never
hidden.

```text
$ rubedo trace "step:top_story"
Trace: 15 seed, 0 upstream, 21 downstream
  seed       top_story            row-4d3d039e1592             @ 2581baed330a  value={'id': 48877668}
  seed       top_story            row-2f62339fd0eb             @ da063d067fa4  value={'id': 48876505}
  ...
  downstream screen               row-4d3d039e1592             @ 9c1a4e2b7f10
  downstream digest               @all                         @ f30a221cc890
```

Running `rubedo trace "<query>"` with the *same query* you're about to pass
to `rubedo invalidate --downstream` is the standard way to preview exactly
what a widened invalidation would flip — the header's `downstream` count is
the number of materializations `--downstream` would add on top of your
seed matches, and it carries the same reduce/join fan-in honesty described
above: a small seed set can still report a large downstream count if it
feeds a fan-in step.

See [`../concepts/model.md`](../concepts/model.md) for the ledger tables
`trace` reads, and [`../guides/inspecting-runs.md`](inspecting-runs.md) for
`trace()`'s other use — auditing lineage without invalidating anything.
