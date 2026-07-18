# Shapes

A step's `in_shape`/`out_shape` decide how many output lanes it produces
from its input lanes. There are four conceptual shapes: `map` (1:1),
`aggregate` (N:1), `expand` (1:N), and
`join` (N-way, minting pair lanes). Every shape is a special case of the same
underlying idea — a producer that takes some input lanes and emits some
output lanes — but each has a distinct planning and caching story worth
knowing on its own. See [`../notes/producer-model.md`](../notes/producer-model.md)
for the design behind the taxonomy.

The five conceptual shapes map to `in_shape`/`out_shape` pairs: `map` (`one`/`one`),
`aggregate` (`aggregate`/`one` — was "reduce"), `fold` (`fold`/`one`), `expand` (`one`/`many`),
`join` (`join`/`many`). The legacy `shape=` kwarg is kept as an alias:
`shape="map"`/`shape="reduce"`/`shape="expand"`/`shape="join"` each translate
to the corresponding pair and are never stored on the spec.

Most of the time you don't pass `shape=` (or `in_shape=`/`out_shape=`) at
all: it's inferred from what the code already says — a generator function
defaults to `expand` (`out_shape="many"`), `join_on=` defaults it to `join`,
`group_key=` defaults it to `aggregate` (`in_shape="aggregate"`),
and anything else is `map` (`one`/`one`, the default). An explicit `shape=`
(or `in_shape=`/`out_shape=`) always overrides the
inference, and an explicit value that contradicts the code (a generator
decorated `shape="map"`, say) raises rather than silently misbehaving. See
[API reference: `@step`](../reference/api.md#shape-and-depends_on-inference)
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
any other step. `shape="reduce"` is the alias and still works.)

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
pipeline's initial lanes and, having no parent to cache against, **always
re-executes**, every run (no anchor is written or checked). This *is* how
ingestion works — there's no separate source concept, just this shape used
with no parent. A parentless generator function infers `out_shape="many"`
(the `shape="expand"` alias)
automatically:

```python
from rubedo import step

@step
def hn_top():
    for sid in fetch_top_ids():
        yield fetch_story(sid)
```

Drop it straight into `pipeline(steps=[...])` — nothing else needed. See
[sources.md](sources.md) for the folder/CSV/table/cloud recipes.

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

Combines lane sets from **different roots** on a shared, indexed value —
unlike a multi-parent `map`, which joins parents by *inherited* coordinate
because they share a lineage (a "diamond"), `join` matches lanes whose
coordinates are otherwise unrelated. It buckets each side by its declared
field, intersects on shared values, and mints one pair lane per matched
tuple — coordinate `a|b|…`, the members' coordinates joined by `|`:

```python
p = pipeline(name="enrich")

@p.step
def orders_src():
    with open("orders.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def customers_src():
    with open("customers.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def order(orders_src): return {"oid": orders_src["oid"], "cust": orders_src["cust"]}

@p.step
def customer(customers_src): return {"cid": customers_src["cid"], "name": customers_src["name"]}

@p.step(
        join_on={"order": "cust", "customer": "cid"})
def enrich(order, customer):        # one lane per matched pair
    return {"oid": order["oid"], "name": customer["name"]}
```

(`join_on=` implies `in_shape="join"`/`out_shape="many"`; the parents are the `join_on` keys,
confirmed by the function's parameters at build time.)

Every side named in `join_on` must carry the field it's matched by in its
output struct, and `join_on`'s keys must exactly match the
function's parameter names (which become the parents). `join` accepts two
or more parents — `join_on={a:"uid",
b:"uid", c:"uid"}` is a valid N-way star, all matched on the same field
name. Sides joined on *different* pairwise keys compose by chaining `join`
steps; an arbitrary pair predicate isn't part of `join` itself — express it
as a `filter`-style step (return `Filtered(...)`) immediately after.

Like `aggregate`, `join` honors `on_failed` (default `"use_passed"`): a failed
or blocked lane on one side just drops out of that side's bucket instead of
blocking every match.

Reach for `join` only when two lane sets come from genuinely independent
roots and need matching by *value* — enriching orders with customer
records, feeds with publisher metadata. If two steps already share a
lineage (both descend from the same source), you almost certainly want a
plain multi-parent `map`, not a `join` — see
[`../notes/producer-model.md`](../notes/producer-model.md#the-distinction-that-matters-most-diamond-join)
for why a diamond isn't a join.

## Putting it together

`join` → `expand` → `aggregate` compose in one pipeline the way you'd expect:
two sources join to enrich each feed with its publisher's region, each feed
expands into a lane per article (cached, so a re-run re-scrapes nothing),
and the articles aggregate by region into one digest per region. See
`examples/newsroom` (listed in [`../examples.md`](../examples.md)) for the
whole thing, runnable end to end.

## Next

- [sources.md](sources.md) — where a `map`/`join`/`expand` root's lanes
  actually come from.
- [model.md](model.md) — the addressing and ledger mechanics every shape
  shares.
- [versioning.md](versioning.md) — how `version`/`code` interact with each
  shape's cache identity.
