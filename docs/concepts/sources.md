# Sources: recipes, not classes

There's no `Source` protocol and no source classes to import. Ingestion is
just a step: a parentless generator — its `shape="expand"`
(`out_shape="many"`) inferred, the
same producer shape `expand` and `join` already use downstream — that
`yield`s one payload per item. Each yielded payload mints its own
content-addressed lane (`row-<hash>`); the engine never imports your code,
so there's nothing to subclass or register.

```python
from rubedo import pipeline

p = pipeline(name="doubler")

@p.step
def rows():
    yield {"n": 1}
    yield {"n": 2}

@p.step
def double(rows):
    return rows["n"] * 2
```

A source watching the outside world wants `check_cache=False`. By default
a root generator's fan-out is **anchor-cached** like any expand (see
[shapes.md](shapes.md#expand-1n-fan-out)): the generator runs once and is
then skipped until its identity (code version, params) changes — right for
a fixed in-code list like `rows` above, wrong for a folder or table you
expect to change, where only a fresh enumeration can notice new, edited,
or deleted items. `check_cache=False` re-runs the generator on every
`p.run()`; the lanes it mints stay content-addressed, so a rescan that
finds nothing new still reuses everything downstream. The recipes below
all watch external state, so they all declare it. (The definition snapshot
records the step's source text for display; the engine never imports or
executes anything from a snapshot.)

**Yield content, not references.** Whatever the generator yields is what
gets hashed to mint the lane. A recipe that yielded a path or a row number
instead of the row's own data would pin lanes to names, not content, and
lose the properties below. The folder recipe below reads every file it
lists for exactly this reason — it was already doing that I/O to hash it,
so recipes don't add any.

## Folder

Each file under a folder is a lane. A three-line `pathlib`/`os` generator:

```python
import os
from rubedo import pipeline

p = pipeline(name="docs")

@p.step(check_cache=False)   # rescan the folder every run
def scan():
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def process(scan):
    return scan["text"].upper()
```

`sorted()` keeps scan order deterministic (harmless either way — lanes are
content-addressed, not positional — but it makes `p.describe()`/logs
reproducible run to run). Read every file's bytes into the payload, not
just its path: the payload *is* what gets hashed into the lane's identity.

## CSV

Each row of a CSV is a lane. A `csv.DictReader` loop:

```python
import csv
from rubedo import pipeline

p = pipeline(name="enrich-leads")

@p.step(check_cache=False)   # re-read the CSV every run
def leads():
    with open("data/leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step
def enrich(leads: dict):
    return {"email": leads["email"], "summary": call_llm(leads["notes"])}
```

## SQL table

Each row of a SQL table is a lane. A plain `SELECT` loop:

```python
from sqlalchemy import create_engine, text
from rubedo import pipeline

p = pipeline(name="orders-rollup")

@p.step(check_cache=False)   # re-query the table every run
def orders():
    engine = create_engine("postgresql://...")
    with engine.connect() as conn:
        for row in conn.execute(text("SELECT * FROM orders")).mappings():
            yield dict(row)

@p.step
def classify(orders: dict): ...
```

**Buffering is accepted for v1.** The generator above (and `expand` in
general) buffers every yielded row before the run commits anything — fine
for tables that fit comfortably in memory, and the whole table is one
query. A table too large to hold at once needs a streaming variant: page
through it in the generator (`LIMIT`/`OFFSET` or a server-side cursor) and
`yield` each row as you go — the buffering is in `expand`'s planning, not
in how you write the loop, so a streaming recipe still buffers all rows
before the corresponding run commits. A true streaming *commit* path (rows
land as they're read, not after the whole scan) is Parked for now.

## Cloud object storage (S3/GCS)

Hashing an object means downloading it, so a cloud recipe must not fetch
object bytes just to enumerate — LIST calls only, zero `GetObject`. Yield
a cheap change token (`key`, `etag`, `size`) from the source step, and let
a downstream **cached** step do the actual fetch:

```python
import boto3
from rubedo import pipeline

p = pipeline(name="ingest")

@p.step(check_cache=False)   # re-LIST the bucket every run
def objects():
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="my-bucket"):
        for obj in page.get("Contents", []):
            yield {"key": obj["Key"], "etag": obj["ETag"], "size": obj["Size"]}

@p.step
def fetch(objects: dict) -> bytes:
    client = boto3.client("s3")
    return client.get_object(Bucket="my-bucket", Key=objects["key"])["Body"].read()
```

The token (not the bytes) is what mints `objects`'s lane, so an unchanged
object costs one LIST entry and a cache hit on `fetch` — never a
re-download. An object whose etag churns without a real content change
still costs exactly one re-download and one `fetch` re-run, nothing more:
`fetch`'s own cache boundary contains the cost to that one lane, so nothing
downstream of `fetch` re-runs unless `fetch`'s output actually changes.
Prefer `etag` over `mtime` as the token — it's content-derived and stable
across identical re-uploads; `mtime` bumps on every upload even when the
bytes didn't change.

## Content-addressed lanes

Every recipe above mints lanes the same way: `row-<hash>` where the hash is
over the yielded payload's JSON-canonical form. Two consequences follow
directly:

- **Identical payloads collapse to one lane.** A duplicate row anywhere in
  the file or table — or two files with identical bytes — is simply the
  same unit of work, deduplicated for free.
- **An edited item reads as removed + created, not changed.** Same logical
  row or file, different bytes → a different coordinate. The old lane's
  materialization stays live but unreferenced (harmless — see
  [model.md](model.md) on generations); a new coordinate creates fresh.

To find or track an item by a human field — email, order id, file name,
whatever a person would call it — **query the step's output struct**,
don't rely on the coordinate:

```python
@p.step
def scan(): ...
```

then `Selection(index={"path": "a.txt"})`. The coordinate is engine
plumbing, not a search key — see [model.md](model.md) for the full "what a
coordinate is/isn't" distinction, and
[`../guides/search-and-invalidation.md`](../guides/search-and-invalidation.md)
for querying by output fields.

### Why this makes incrementality survive reordering and appends

Because a lane's identity is its content, not its position, a source
generator's *yield order* is irrelevant to caching — reshuffle a CSV and
every row still content-addresses to the coordinate it had before, so
every lane still reuses. Appending new rows only mints coordinates for the
new content; existing rows are untouched. Compare this to a positional or
line-number-based cache, which would treat a reordered file as entirely
changed. Content addressing is what lets "just append to the CSV" or
"re-export the table with rows in a different order" stay a no-op for
everything that hasn't actually changed.

## Multiple sources

A pipeline can declare more than one source-shaped root — the shape a
`join` needs, since a join combines lane sets from independent roots. Each
is just another parentless generator step; nothing extra to declare:

```python
p = pipeline(name="enrich")

@p.step(check_cache=False)
def orders_src():
    with open("orders.csv", newline="") as f:
        yield from csv.DictReader(f)

@p.step(check_cache=False)
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

See [shapes.md](shapes.md) for the `join` step this setup feeds, and
`examples/newsroom` (see [`../examples.md`](../examples.md)) for a full
join → expand → `group_key` pipeline over two CSV sources.

A pipeline needs no source-shaped root at all, as long as some root
originates lanes — either an `expand` root (a generator that mints N
lanes, as above) or a source-less `map` root (mints one
lane from its `params`). See
[shapes.md](shapes.md#the-source-less-map-root).

## Next

- [shapes.md](shapes.md) — what steps do with the lanes a source-shaped
  root produces.
- [model.md](model.md) — how a lane's coordinate feeds into a step's output
  address.
- [`../guides/search-and-invalidation.md`](../guides/search-and-invalidation.md)
  — finding rows by what they *are*, not by coordinate.
