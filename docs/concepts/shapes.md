---
search:
  exclude: true
---

# Shapes

!!! note "Long form"
    Start at [How it works](model.md#shapes). This page is the full shape
    reference — inference, caching, and the traps.

Most steps are 1:1: one item in, one item out. The other shapes cover
fan-in, fan-out, and joins — still just Python functions, with a different
count of lanes in and out.

A step's `in_shape`/`out_shape` decide how many output lanes it produces
from its input lanes. There are five conceptual shapes: `map` (1:1),
`aggregate` (N:1), `fold` (N:1, sequential), `expand` (1:N), and
`join` (N-way equijoin, minting pair lanes — inner or symmetric outer via
`join_mode`). Every shape is a special case of the same underlying idea —
a producer that takes some input lanes and emits some output lanes — but
each has a distinct planning and caching story worth knowing on its own.
See [`../development/producer-model.md`](../development/producer-model.md)
for the design behind the taxonomy.

The five conceptual shapes map to `in_shape`/`out_shape` pairs: `map` (`one`/`one`),
`aggregate` (`aggregate`/`one`), `fold` (`fold`/`one`), `expand` (`one`/`many`),
`join` (`join`/`many`). The legacy `shape=` kwarg is kept as an alias:
`shape="map"`/`shape="expand"`/`shape="join"` each translate
to the corresponding pair and are never stored on the spec.

Most of the time you don't pass `shape=` (or `in_shape=`/`out_shape=`) at
all: it's inferred from what the code already says — a generator function
defaults to `expand` (`out_shape="many"`), `join_on=` defaults it to `join`,
`group_key=` defaults it to `aggregate` (`in_shape="aggregate"`),
and anything else is `map` (`one`/`one`, the default). An explicit `shape=`
(or `in_shape=`/`out_shape=`) always overrides the
inference, and an explicit value that contradicts the code (a generator
decorated `shape="map"`, say) raises rather than silently misbehaving. See
[API reference: `@step`](../reference/api/step.md)
for the full inference rules, including how a step's `depends_on` is
likewise inferred from its parameter names.

## `map` — 1:1 (the default)

The default. One input lane in, one output lane out, same coordinate. This
is almost every step you'll write:

```python
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

p.run()
```

Reach for `map` whenever a step's output depends on exactly one thing per
item — which is most transformation, extraction, and enrichment logic.

### The source-less `map` root

A root (`no depends_on`) is usually `expand`-shaped (`out_shape="many"`,
the `shape="expand"` alias) — the ingestion shape
(see [sources.md](sources.md)). But a plain `map` root with **no**
`depends_on` is also legal, and mints a single lane whose input is its
`params` (or a constant, if the function takes none):

```python
p = pipeline(name="pdf")

@p.step                                    # no parents, not a generator
def load_pdf(params): return split(params["pdf"])   # mints the single '@root' lane

p.run(params={"pdf": "report.pdf"})
```

The lane's coordinate is the fixed constant `@root`, so its address reduces
to `hash(step, version, "@root", params_hash)`: same `params=` reuses the
cached output, a changed `params=` makes a new generation. It's the
everyday counterpart to an `expand` root, which mints N lanes instead of
one — a way to feed a value *into* the head of a pipeline instead of
scanning for one. See [`../examples.md`](../examples.md)
(`examples/pdf_digest`) for this feeding an `expand` → vision-LLM →
`aggregate` chain end to end.

### Broadcasting a single value into per-row steps

A source-less root's value isn't limited to steps that depend on nothing
else — name it alongside a real per-row dependency in a downstream `map`
step, and every row sees the same value:

```python
@p.step
def threshold(params): return params["min_score"]      # mints '@root'

@p.step(out_shape="many")
def rows():
    yield from read_csv("scores.csv")

@p.step
def flagged(rows: dict, threshold: int):
    return rows["score"] > threshold                    # threshold, broadcast to every row
```

`flagged` has one real per-row dependency (`rows`) and one broadcast
dependency (`threshold`); Rubedo resolves `threshold` from its single
materialization and applies it to every row, instead of requiring a
matching per-row coordinate. This works for any dependency whose entire
chain can never fan out — a source-less root, an unaggregated
(`group_key=None`) `aggregate`/`fold` result, or a `map` chain built only
from those — not only a direct root.

Mixing two *real* multi-lane dependencies that don't share a coordinate
lineage is still an error (`parents produce disjoint lane sets` — see
[`join`](#join-n-way-equijoin) below, or restructure so they share a
root). The distinction is whether a dependency *can* ever have more than
one coordinate for the run, not whether it happens to have one this time.

## `aggregate` — N:1 (fan-in)

Fans in over a parent's *surviving* lanes. A plain aggregate (`group_key=None`)
receives every lane as one `{coordinate: value}` dict and returns a single
output at the fixed coordinate `@all`:

```python
@p.step(in_shape="aggregate")
def total_lines(count_lines: dict):
    return sum(v["line_count"] for v in count_lines.values())
```

(A plain `@all` aggregate is the one shape that's always explicit: nothing
in the code implies it. The parent comes from the parameter name, like
any other step. Pass `in_shape="aggregate"` explicitly.)

A plain (`group_key=None`) aggregate's result is itself always one value
for the whole run, so it can be named alongside a real per-row dependency
in a downstream step the same way a source-less root can — see
[broadcasting a single value](#broadcasting-a-single-value-into-per-row-steps)
above. A `group_key`'d aggregate can't: it produces one output per group,
so it needs the same aligned-coordinate handling as any other multi-lane
producer.

Add `group_key="field"` to fan in **per group** instead of all at once — one
output per distinct value of a field, read from the parent output struct
at plan time (so planning stays value-free).
`group_key=` implies `in_shape="aggregate"` on its own:

```python
@p.step(group_key="region")
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}
```

A lane that carries no value for `group_key` raises — the field must be
present in the parent step's output. A lane with several values for the
field (a list-valued field) joins every one of those groups.

By default, `on_failed="use_passed"`: if a parent lane failed or was
blocked, `aggregate` drops it and proceeds with whatever survived (firing a
`partial_fan_in` warning), rather than stalling the whole aggregate on one
bad upstream item. Pass `on_failed="block"` when every parent lane must be
present for the aggregate to mean anything — then a single missing lane
blocks the entire aggregate (or the entire group, under `group_key`).

Reach for `aggregate` for aggregation, rollups, and "sort these back into one
document" reassembly (the `expand` → `aggregate` round trip in
`examples/pdf_digest`, see [`../examples.md`](../examples.md), is exactly
that: split a PDF into chunks, process each independently, fold back into a
whole document).

An aggregate step can also request its fan-in as a single Arrow table
instead of a `{coordinate: value}` dict — `@p.step(in_shape="aggregate",
arrow_aggregate=True)` hands the function a `pa.Table` built from the
parent's surviving lanes, skipping the Python-dict round trip. It requires
`in_shape="aggregate"` (an `arrow_aggregate=True` map or expand step raises
at build time).

## `fold` — N:1 (sequential fan-in with accumulator)

Like `aggregate`, `fold` is an N:1 fan-in (`out_shape="one"`, exactly one parent), but instead of receiving all lanes at once as a dict, the step function receives an **accumulator** and one parent value at a time. The accumulator is initialized to `fold_init` and passed from lane to lane. 

```python
@p.step(in_shape="fold", fold_init=0)
def total_lines(accum: int, count_lines: dict):
    return accum + count_lines["line_count"]
```

`fold` supports `group_key` exactly like `aggregate`: if specified, the fold is performed independently per group, and the accumulator resets to `fold_init` for each group. 

`fold` is evaluated incrementally. Use it when an aggregate would run out of memory loading all lanes into a single dictionary, or when the logic naturally fits a rolling update.

## `expand` — 1:N (fan-out)

The step is a generator: it `yield`s a payload per item, and each yielded
value mints its own content-addressed downstream lane
(`row-<hash(value)>`) — identical yielded values collapse to one lane, just
like a source row:

```python
@p.step
def articles(fetch: list):
    for art in fetch:      # 1:N — one lane per article
        yield art

@p.step
def headline(articles: dict) -> str:
    return articles["title"].upper()
```

(`articles` is a generator, so its `out_shape="many"` (the `shape="expand"`
alias) is inferred; its
`fetch` parameter names the parent step.)

**The caching insight is the reason `expand` exists.** An expand can't cache
by a single output address the way `map` does — it's 1:N. Instead, the whole
expansion is stored as a **cache anchor**: one small materialization
addressed by the *parent's* content (`hash(step, version, parent_content)`),
holding just the child content hashes. On the next run, if the parent lane
is unchanged, planning finds the anchor live and replays the child lanes as
`reuse` decisions **without calling the function at all**
(`_plan_expand_reuse` in `planning.py`). The anchor itself isn't a lane — no
status, no count, no lineage edge of its own — it exists purely so a
non-idempotent fan-out (scrape a feed, paginate an API) runs exactly once
per distinct parent, ever. `stale_after` on the `expand` step gives you
periodic re-scrape on top of that.

### Expand roots (sources)

An `expand` step with **no** `depends_on` is a root — it yields the
pipeline's initial lanes. There is no separate ingestion concept: a
parentless generator infers `out_shape="many"` (the `shape="expand"`
alias) automatically and *is* the source.

Root expands are **anchor-cached** like any other expand. With no parent
lane to key on, the anchor is addressed against the constant `@root`
(`ROOT_LANE` in `planning.py`): the generator runs once, then planning
replays its children as `reuse` until the step's identity (code version,
params) changes. That is right for a fixed in-code list; wrong for a
folder or table you expect to change. Sources that watch external state
declare `check_cache=False` so the generator re-runs every `p.run()` —
lanes stay content-addressed, so a rescan that finds nothing new still
reuses everything downstream. See [sources.md](sources.md).

```python
from rubedo import step

@step
def hn_top():
    for sid in fetch_top_ids():
        yield fetch_story(sid)
```

Drop it straight into `pipeline(steps=[...])` — nothing else needed. See
[sources.md](sources.md) for the folder/CSV/table/cloud recipes.

!!! note "`@root` in plan output"
    A dry-run often prints `execute scan @root` for an expand root. That
    `@root` is the **anchor slot** for the source enumeration — not a
    minted file lane. Content-addressed child lanes (`row-<hash>`) appear
    only after the generator runs. A source-less **map** root is different:
    it mints a real single `@root` lane whose input is the step's params.

Reach for `expand` whenever the *number* of downstream items isn't known
until you've fetched something — RSS feeds, paginated APIs, multi-page
documents, search results.

### Table-return expand (bulk fan-out)

Instead of `yield`-ing N payloads in a Python loop, an expand step can
**return an Arrow table** (`pa.Table`, polars DataFrame, or pandas
DataFrame). Each row becomes a content-addressed lane — the table IS the
fan-out. This lets you go straight from `pl.read_csv("data.csv")`, a
DuckDB query, or any Arrow producer to lanes, with no Python iteration:

```python
import pyarrow as pa
from rubedo import step

@step(out_shape="many")
def load_csv():
    return pa.table({
        "name": ["alice", "bob", "carol"],
        "score": [100, 200, 300],
    })
```

Each row becomes a `row-<hash>` lane whose output is a dict (the row's
values). Downstream steps receive it as a `dict` parameter, just like a
yielded payload. Identical rows collapse to one lane (same content → same
hash). The anchor caching works identically to yield-based expand — on
re-run, if the parent is unchanged, the expand fn is not called and
children are reused.

Declare `out_shape="many"` (or `shape="expand"`) explicitly for table-return expand (a
non-generator function doesn't auto-infer the shape).

## `join` — N-way equijoin

Combines lane sets from **different roots** on a shared field value.
Unlike a multi-parent `map` (a "diamond"), which pairs parents by
*inherited* coordinate equality because they share a lineage, `join`
matches lanes whose coordinates are otherwise unrelated. It buckets each
side by a declared field, then mints one pair lane per combination —
coordinate `a|b|…` (absent sides under outer join use the reserved
`@missing` segment).

Reach for `join` when two (or more) lane sets come from genuinely
independent roots and need matching by *value* — orders with customers,
feeds with publishers. If both sides already descend from the same
source, you almost certainly want a plain multi-parent `map` instead;
see
[`../development/producer-model.md`](../development/producer-model.md#the-distinction-that-matters-most-diamond-join).

### `join_mode`: intersect vs union

All `join_on` sides are equal — there is no left/right bias. The mode
only chooses which key universe emits lanes:

| `join_mode` | Key universe | Absent sides |
|-------------|--------------|--------------|
| `"intersect"` (default) | ∩ of per-side keys | never (inner join) |
| `"union"` | ∪ of per-side keys | bound as Python `None` |

```python
@p.step(join_on={"order": "cust", "customer": "cid"})          # intersect
def enrich_inner(order, customer):
    return {"oid": order["oid"], "name": customer["name"]}

@p.step(join_on={"order": "cust", "customer": "cid"},
        join_mode="union")
def enrich_outer(order, customer):                             # customer may be None
    return {
        "oid": order["oid"] if order else None,
        "name": None if customer is None else customer["name"],
    }
```

`join_on=` implies `in_shape="join"` / `out_shape="many"`; its keys are
the parents (and must match the function's parameter names). N-way stars
are first-class — `join_on={a: "uid", b: "uid", c: "uid"}` matches every
side on the same field value. Different pairwise keys compose by chaining
join steps. Non-equi predicates and anti-joins are a `Filtered` step
afterward (`join_mode="union"` + filter where a side is `None`).

### Keys, duplicates, and nulls

- **Null / missing join-field values raise** at plan time. They never
  share a null bucket (so messy tables cannot silently cartesian-match
  on `None`). Clean or drop them upstream.
- **Duplicate keys** on any side produce a cartesian product within that
  key (SQL equijoin multiplicity). Many-to-one enrichment is intentional;
  duplicate rows on a *lookup* side are the usual footgun — plan emits a
  `UserWarning` when any key fans out. Dedupe before the join if you
  expected one output per key.
- Under `join_mode="union"` with `on_failed="use_passed"`, a failed parent
  lane is dropped from its bucket, so **failed ≈ unmatched** (other sides
  may still emit with `None`). Plan warns; use `on_failed="block"` to
  refuse instead.

For normalize → dedupe → join recipes, see
[Enrich and join tables](../guides/data-enrichment.md).

### Caching

Join identity is the parents' content hashes (every `depends_on` slot;
absent outer sides use an internal `@missing` sentinel). `join_mode` is
**not** part of the address: flipping intersect↔union reuses matched
pairs and only adds/drops unmatched lanes. When a previously unmatched
lane later finds a match, the `@missing` coordinate is **removed** and a
new pair coordinate is **added** — not an in-place update.

### Declarative join

`p.join(name=..., join_on=..., join_mode=...)` builds the nested struct
`{"orders": {...}, "customers": {...}}` with no function body (absent
sides are `None` under union). Same caching rules.

## Putting it together

`join` → `expand` → `aggregate` compose the way you'd expect: two sources
join to enrich each feed with its publisher's region, each feed expands
into a lane per article (cached, so a re-run re-scrapes nothing), and the
articles aggregate by region into one digest per region. See
`examples/newsroom` in [`../examples.md`](../examples.md) for the runnable
pipeline, and [Enrich and join tables](../guides/data-enrichment.md) for
normalize / dedupe / outer-join practices around the join itself.
