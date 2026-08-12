# Data enrichment

The common pattern: two independent tables, match on a business key, carry
fields from one side onto the other — orders with customers, events with
accounts, feeds with publishers. In Rubedo that is a multi-root pipeline
ending in a [`join`](../concepts/shapes.md#join-n-way-equijoin), not a
multi-parent `map` (a map only pairs lanes that already share a coordinate
lineage — a diamond).

This page is the practical companion to the join shape: how to normalize
keys, dedupe before you fan out, choose inner vs outer, and read the
warnings join will give you.

## The skeleton

```python
import csv
from rubedo import Filtered, pipeline

p = pipeline(name="enrich-orders")

@p.step(check_cache=False)
def orders_src():
    with open("orders.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step(check_cache=False)
def customers_src():
    with open("customers.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def order(orders_src: dict) -> dict:
    return {
        "oid": orders_src["oid"].strip(),
        "cust": orders_src["cust"].strip().lower(),
    }

@p.step
def customer(customers_src: dict) -> dict:
    return {
        "cid": customers_src["cid"].strip().lower(),
        "name": customers_src["name"].strip(),
    }

@p.step(join_on={"order": "cust", "customer": "cid"})
def enrich(order: dict, customer: dict) -> dict:
    return {"oid": order["oid"], "name": customer["name"]}

p.run()
```

Two roots → normalize maps → equijoin. The join keys live on the
*normalized* outputs (`cust` / `cid`), not on the raw CSV rows.

## Normalize before you join

Join matching is exact string equality on the field you name in
`join_on`. `"Acme"`, `"acme"`, and `" Acme "` are three different keys.
Put stripping, case-folding, and id formatting in a cheap `map` on each
side **before** the join so:

- the join field is the only spelling that matters,
- cache identity follows the cleaned value (an upstream typo fix
  recomputes only the lanes whose cleaned key changed),
- null / blank keys can be declined early (see below).

Keep the raw columns if you still need them for audit — just don't join
on them.

```python
@p.step
def customer(customers_src: dict):
    cid = (customers_src.get("cid") or "").strip().lower()
    if not cid:
        return Filtered(reason="missing cid")
    return {"cid": cid, "name": (customers_src.get("name") or "").strip()}
```

`Filtered` drops that lane from every downstream fan-in, including join
buckets — so blank keys never reach the join (which would otherwise
**raise** on a null/missing join field).

## Dedupe the lookup side

Join is an equijoin: within one shared key it emits the **cartesian
product** of the sides. Two customer rows for `cid=c1` and three orders
for `cust=c1` → six enrich lanes. Many-to-one (many orders, one customer)
is usually what you want; **many-to-many** or a duplicated dimension row
is usually a data bug. Plan warns whenever any matched key has duplicate
lanes on a side — treat that warning as a signal to fix the lookup table,
not as noise.

### Collapse duplicates with `group_key`

If the dimension *should* be unique on the join key, aggregate it first:

```python
@p.step(group_key="cid")
def customer_unique(customer: dict) -> dict:
    rows = list(customer.values())
    if len(rows) > 1:
        # pick a deterministic winner, or raise to fail the group
        rows = sorted(rows, key=lambda r: r["name"])
    return rows[0]

@p.step(join_on={"order": "cust", "customer_unique": "cid"})
def enrich(order: dict, customer_unique: dict) -> dict:
    return {"oid": order["oid"], "name": customer_unique["name"]}
```

`group_key="cid"` emits one lane per distinct `cid` (coordinate = the key
value). Sorting before picking keeps the winner stable across runs so the
join's cache identity does not flap.

### Prefer fixing the source

When the CSV itself has duplicate ids, fixing upstream (or filtering in
the source generator) is better than a silent "first row wins" in the
pipeline. Use the aggregate collapse when you need the run to proceed and
the rule for picking a winner is part of the pipeline's contract.

## Choose intersect vs union

| Goal | `join_mode` |
|------|-------------|
| Only rows that match on both sides | `"intersect"` (default) |
| Keep every key from every side; pad absences with `None` | `"union"` |
| Rows on A with no match on B (anti-join) | `"union"`, then `Filtered` when B is `None` |

```python
@p.step(join_on={"order": "cust", "customer": "cid"}, join_mode="union")
def enrich(order, customer):
    return {
        "oid": None if order is None else order["oid"],
        "name": None if customer is None else customer["name"],
        "matched": order is not None and customer is not None,
    }

@p.step
def unmatched_orders(enrich: dict):
    if enrich["name"] is not None:
        return Filtered(reason="has customer")
    return {"oid": enrich["oid"]}
```

Under union, a later-appearing match is **remove + add**: the old
`…|@missing` lane orphans and a new pair lane is minted. That is the
content-addressed lane story — do not expect an in-place update of the
unmatched enrich row. See [shapes: join caching](../concepts/shapes.md#caching).

!!! note "Failed ≈ unmatched under union"
    With the default `on_failed="use_passed"`, a failed parent lane is
    dropped from its join bucket. Under `join_mode="union"` the other
    sides may still emit with `None` on the failed side — the same shape
    as a true miss. Plan warns when that happens. Use `on_failed="block"`
    on the join when a failed lookup should refuse the whole match set.

## Diamond ≠ join

If both parents already share a root (parse a file into `{doc, customer}`,
then fetch per customer and recombine), pair them with a **multi-parent
`map`** on the shared coordinate — not `join`. Join is for roots that
never shared a lineage. Mixing two unrelated multi-lane parents in a map
raises `parents produce disjoint lane sets`; that error is telling you to
reach for `join_on=` (or to restructure so they share a root). Details:
[producer model](../development/producer-model.md#the-distinction-that-matters-most-diamond-join).

## Checklist

1. **Two roots?** → `join`. Same lineage? → multi-parent `map`.
2. **Normalize** join keys in a map (strip / case / format); `Filtered` blanks.
3. **Dedupe** the lookup side if the key must be unique (`group_key` or fix source).
4. **Pick** `intersect` vs `union` deliberately; handle `None` in the fn for union.
5. **Heed** the duplicate-key `UserWarning` — cartesian fan-out is real.
6. **Expect** remove+add when an outer match appears or disappears.

## See also

- [Shapes: join](../concepts/shapes.md#join-n-way-equijoin) — `join_mode`, caching, declarative `p.join`.
- [Sources](../concepts/sources.md) — multi-root CSV / folder recipes.
- [`examples/newsroom`](../examples.md) — join → expand → `group_key` aggregate end to end.
- [Execution policies: Filtered](execution-policies.md) — declining a lane as a cached verdict.
