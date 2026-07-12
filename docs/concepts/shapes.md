# Shapes

A step's `shape` decides how many output lanes it produces from its input
lanes. There are four: `map` (1:1), `reduce` (N:1), `expand` (1:N), and
`join` (N-way, minting pair lanes). Every shape is a special case of the same
underlying idea — a producer that takes some input lanes and emits some
output lanes — but each has a distinct planning and caching story worth
knowing on its own. See [`../notes/producer-model.md`](../notes/producer-model.md)
for the design behind the taxonomy.

## `map` — 1:1 (the default)

The default. One input lane in, one output lane out, same coordinate. This
is almost every step you'll write:

```python
from rubedo import ProcessResult, step, pipeline, run

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

p = pipeline(id="count-lines", name="Count Lines", folder="input",
             steps=[read_lines, count_lines])
run(p)
```

Reach for `map` whenever a step's output depends on exactly one thing per
item — which is most transformation, extraction, and enrichment logic.

### The source-less `map` root

A root (`no depends_on`) normally reads from the pipeline's `Source`. But a
`map` root with **no source at all** is legal, and mints a single lane whose
input is its `params` (or a constant, if the function takes none):

```python
@step(name="load_pdf", version="1")          # no depends_on, no source
def load_pdf(params): return split(params["pdf"])   # mints the single '@root' lane

pipeline(id="pdf", name="PDF", steps=[load_pdf, ...])   # no source= needed

run(pipeline_obj, params={"pdf": "report.pdf"})
```

The lane's coordinate is the fixed constant `@root`, so its address reduces
to `hash(step, version, "@root", params_hash)`: same `params=` reuses the
cached output, a changed `params=` makes a new generation. It's the
everyday counterpart to an `expand` root, which mints N lanes instead of
one — a way to feed a value *into* the head of a pipeline instead of
scanning a `Source` for one. See [`../examples.md`](../examples.md)
(`examples/pdf_digest`) for this feeding an `expand` → vision-LLM →
`reduce` chain end to end.

## `reduce` — N:1 (fan-in)

Fans in over a parent's *surviving* lanes. A plain reduce (`group_key=None`)
receives every lane as one `{coordinate: value}` dict and returns a single
output at the fixed coordinate `@all`:

```python
@step(name="total_lines", version="total-v1",
      depends_on=["count_lines"], shape="reduce")
def total_lines(count_lines: dict):
    return sum(v["line_count"] for v in count_lines.values())
```

Add `group_key="field"` to fan in **per group** instead of all at once — one
output per distinct value of an indexed field, read from
`@step(index=[...])` entries at plan time (so planning stays value-free):

```python
@step(name="digest", version="1", depends_on=["articles"],
      shape="reduce", group_key="region")
def digest(articles: dict) -> dict:
    titles = sorted(a["title"] for a in articles.values())
    return {"count": len(titles), "headlines": titles}
```

A lane that carries no value for `group_key` raises — index it on the parent
step. A lane with several values for the field (a list-valued index) joins
every one of those groups.

By default, `on_failed="use_passed"`: if a parent lane failed or was
blocked, `reduce` drops it and proceeds with whatever survived (firing a
`partial_fan_in` warning), rather than stalling the whole aggregate on one
bad upstream item. Pass `on_failed="block"` when every parent lane must be
present for the aggregate to mean anything — then a single missing lane
blocks the entire reduce (or the entire group, under `group_key`).

Reach for `reduce` for aggregation, rollups, and "sort these back into one
document" reassembly (the `expand` → `reduce` round trip in
`examples/pdf_digest`, see [`../examples.md`](../examples.md), is exactly
that: split a PDF into chunks, process each independently, fold back into a
whole document).

## `expand` — 1:N (fan-out)

The step is a generator: it `yield`s a payload per item, and each yielded
value mints its own content-addressed downstream lane
(`row-<hash(value)>`) — identical yielded values collapse to one lane, just
like a source row:

```python
@step(name="articles", version="1", depends_on=["fetch"], shape="expand")
def articles(fetch: list):
    for art in fetch:      # 1:N — one lane per article
        yield art

@step(name="headline", version="1", depends_on=["articles"])
def headline(articles: dict) -> str:
    return articles["title"].upper()
```

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

An `expand` step with **no** `depends_on` is a root — a *source* in every
sense that matters: it yields the pipeline's initial lanes and, having no
parent to cache against, **always re-executes**, every run (no anchor is
written or checked). `@source` is exactly this, spelled as a decorator:

```python
from rubedo import source

@source(name="hn_top", version="1")
def hn_top():
    for sid in fetch_top_ids():
        yield fetch_story(sid)
```

which is sugar for `@step(shape="expand")` with no `depends_on`. Drop it
straight into `pipeline(steps=[...])` — no `source=` needed. See
[sources.md](sources.md) for how this compares to a `Source` class.

Reach for `expand` whenever the *number* of downstream items isn't known
until you've fetched something — RSS feeds, paginated APIs, multi-page
documents, search results.

## `join` — N-way equijoin

Combines lane sets from **different roots** on a shared, indexed value —
unlike a multi-parent `map`, which joins parents by *inherited* coordinate
because they share a lineage (a "diamond"), `join` matches lanes whose
coordinates are otherwise unrelated. It buckets each side by its declared
field, intersects on shared values, and mints one pair lane per matched
tuple — coordinate `a|b|…`, the members' coordinates joined by `|`:

```python
@step(name="order", version="1", source="orders", index=["cust"])
def order(row): return {"oid": row["oid"], "cust": row["cust"]}

@step(name="customer", version="1", source="customers", index=["cid"])
def customer(row): return {"cid": row["cid"], "name": row["name"]}

@step(name="enrich", version="1", shape="join",
      depends_on=["order", "customer"],
      join_on={"order": "cust", "customer": "cid"})
def enrich(order, customer):        # one lane per matched pair
    return {"oid": order["oid"], "name": customer["name"]}

p = pipeline(id="enrich", name="Enrich",
             sources={"orders": CsvSource("orders.csv"),
                      "customers": CsvSource("customers.csv")},
             steps=[order, customer, enrich])
```

Every side named in `join_on` must be indexed on the field it's matched by
(`@step(index=[...])`), and `join_on`'s keys must exactly match
`depends_on`. `join` accepts two or more parents — `join_on={a:"uid",
b:"uid", c:"uid"}` is a valid N-way star, all matched on the same field
name. Sides joined on *different* pairwise keys compose by chaining `join`
steps; an arbitrary pair predicate isn't part of `join` itself — express it
as a `filter`-style step (return `Filtered(...)`) immediately after.

Like `reduce`, `join` honors `on_failed` (default `"use_passed"`): a failed
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

`join` → `expand` → `reduce` compose in one pipeline the way you'd expect:
two sources join to enrich each feed with its publisher's region, each feed
expands into a lane per article (cached, so a re-run re-scrapes nothing),
and the articles reduce by region into one digest per region. See
`examples/newsroom` (listed in [`../examples.md`](../examples.md)) for the
whole thing, runnable end to end.

## Next

- [sources.md](sources.md) — where a `map`/`join`/`expand` root's lanes
  actually come from.
- [model.md](model.md) — the addressing and ledger mechanics every shape
  shares.
- [versioning.md](versioning.md) — how `version`/`code` interact with each
  shape's cache identity.
