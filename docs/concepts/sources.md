# Sources

A `Source` is where a pipeline's initial lanes come from: anything that can
enumerate `(coordinate, content_hash)` pairs and load the payload behind
each one. It's the nullary case of the same producer idea as `expand` and
`join` — see [`../notes/producer-model.md`](../notes/producer-model.md) —
but you rarely need to think of it that way day to day: pick a built-in
`Source`, or write a small class, and move on.

## The `Source` protocol

```python
class Source(ABC):
    @property
    def id(self) -> str: ...          # stable identity, recorded on every run

    def scan(self) -> List[SourceItem]: ...   # snapshot enumeration
    def load(self, item: SourceItem) -> Any: ...  # payload for a root step
```

`scan()` is called once per run and returns every current `SourceItem`
(`coordinate`, `content_hash`, an opaque `ref` the source uses internally,
and `metadata`). Planning uses `coordinate` and `content_hash` only —
`ref` never participates in cache identity, it's purely `load()`'s business.
`load()` is called when a root step's lane actually needs to execute, and
hands back whatever payload the step function receives.

A coordinate **must be stable across scans**: the same logical item keeps
the same coordinate even when its content changes, so the engine can tell
"changed" (same coordinate, new hash) apart from "removed, then a different
item added." The three built-in sources all satisfy this by construction.

## `FolderSource`

Each file under a folder is a lane. Coordinate is the file's path relative
to the folder (forward-slash normalized); payload is the absolute path.

```python
from rubedo import FolderSource, pipeline

pipeline(id="docs", name="Docs", source=FolderSource("input"), steps=[...])
```

`folder="input"` on `pipeline()` is sugar for exactly this — `FolderSource`
is the one case where the coordinate is a legible, stable name rather than a
content hash, because a file path already *is* a stable per-item handle.

## `CsvSource`

Each row of a CSV is a lane. Payload is the row as a `dict`.

```python
from rubedo import CsvSource, step, pipeline

@step(name="enrich", version="1")
def enrich(row: dict):
    return {"email": row["email"], "summary": call_llm(row["notes"])}

leads = pipeline(id="enrich-leads", name="Enrich Leads",
                 source=CsvSource("data/leads.csv"),
                 steps=[enrich])
```

Rows are **content-addressed** by default — see below.

## `TableSource`

Each row of a SQL table is a lane, same shape as `CsvSource`. By default the
whole table is read once during `scan()` and each row's payload rides along
in the `SourceItem`, so `load()` is just a passthrough — one query total.

```python
from rubedo import TableSource

TableSource("postgresql://...", table="leads")
```

Pass `batch_size=N` to stream instead, for tables too large to hold in
memory:

```python
TableSource("postgresql://...", table="leads", key="id", batch_size=5000)
```

In streaming mode, `scan()` reads the table in server-side chunks of `N`,
keeping only `(key, content_hash)` per row and discarding the payload;
`load()` re-fetches a single row by `key` when that lane's step actually
runs. This bounds memory to roughly `N` payloads at a time, at the cost of
one query per lane and the requirement that the row still exist at load
time — the same exposure `FolderSource` has if a file disappears between
scan and load. `batch_size` is purely operational: it changes *how* rows
are read, not which rows or content exist, so it's absent from `id` and
toggling it never invalidates the cache.

!!! warning "`key=` is not the lane key"
    `TableSource.key=` names the column(s) `load()` uses to re-fetch a
    streamed row. It is **only** the streaming re-fetch handle — required
    when `batch_size` is set, unused otherwise, and it **never** determines
    the coordinate. `TableSource` lanes are content-addressed (`row-<hash>`)
    exactly like `CsvSource`, `key=` or not. Don't reach for `key=` to get a
    "nice" lane key; there isn't one to get — see below for why that's the
    point, not a gap.

## Content-addressed lanes

`CsvSource` and `TableSource` rows are lanes keyed by their content:
`row-<hash>` where the hash is over the row's JSON-canonical form. Two
consequences follow directly:

- **Identical rows collapse to one lane.** A duplicate row anywhere in the
  file or table is simply the same unit of work, deduplicated for free.
- **An edited row reads as removed + created, not changed.** Same logical
  row, different bytes → a different coordinate. The old lane's
  materialization stays live but unreferenced (harmless — see
  [model.md](model.md) on generations); a new coordinate creates fresh.

To find or track a row by a human field — email, order id, whatever a
person would call it — **index it downstream**, don't rely on the
coordinate:

```python
@step(name="enrich", version="1", index=["email"])
def enrich(row: dict): ...
```

then `Selection(index={"email": "a@example.com"})`. The coordinate is
engine plumbing, not a search key — see [model.md](model.md) for the full
"what a coordinate is/isn't" distinction, and
[`../guides/search-and-invalidation.md`](../guides/search-and-invalidation.md)
for querying by indexed fields.

### Why this makes incrementality survive reordering and appends

Because a lane's identity is its content, not its position, a source scan's
*order* is irrelevant to caching — reshuffle a CSV and every row still
content-addresses to the coordinate it had before, so every lane still
reuses. Appending new rows only mints coordinates for the new content;
existing rows are untouched. Compare this to a positional or
line-number-based cache, which would treat a reordered file as entirely
changed. Content addressing is what lets "just append to the CSV" or
"re-export the table with rows in a different order" stay a no-op for
everything that hasn't actually changed.

## Multi-source pipelines

A pipeline can declare more than one root producer — the shape a `join`
needs, since a join combines lane sets from independent roots:

```python
from rubedo import CsvSource, pipeline

p = pipeline(
    id="enrich", name="Enrich",
    sources={"orders": CsvSource("orders.csv"),
             "customers": CsvSource("customers.csv")},
    steps=[order, customer, enrich],
)
```

Each root step then names which source it reads with `@step(source="name")`
(a root step with no `source=` on a single-source pipeline reads the sole
source implicitly). `source=`/`folder=` on `pipeline()` remain the
single-source sugar — `sources={DEFAULT_SOURCE: ...}` under the hood — and
are mutually exclusive with `sources=`. See [shapes.md](shapes.md) for the
`join` step this setup feeds, and `examples/newsroom` (see
[`../examples.md`](../examples.md)) for a full join → expand → `group_key`
pipeline over two CSV sources.

A pipeline needs no `Source` at all, as long as some root originates lanes —
either an `expand` root (`@source`, a generator that mints N lanes and
re-runs every run) or a source-less `map` root (mints one lane from its
`params`). See [shapes.md](shapes.md#the-source-less-map-root).

## Next

- [shapes.md](shapes.md) — what steps do with the lanes a source produces.
- [model.md](model.md) — how a lane's coordinate feeds into a step's output
  address.
- [`../guides/search-and-invalidation.md`](../guides/search-and-invalidation.md)
  — finding rows by what they *are*, not by coordinate.
