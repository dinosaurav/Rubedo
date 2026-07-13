# Tutorial

This walks through a small, real pipeline end to end: a folder of review
files gets read and classified as positive/negative/neutral. Along the way
you'll index an output field and query it, edit an input and watch surgical
recompute, bump a step's `version` and see what that invalidates, decline an
input with `Filtered`, and finish by hand-invalidating a selection and
re-running.

Every command below was actually run to produce the output shown — copy the
code blocks into a real directory and you'll see the same shapes (exact hash
prefixes and coordinates will differ, since they're content-addressed).

## Setup

```bash
mkdir -p reviews-demo/input && cd reviews-demo
```

```python title="input/review1.txt"
This product is absolutely amazing and wonderful, I love it so much!
```

```python title="input/review2.txt"
Terrible awful bad experience, I hate this garbage product.
```

```python title="input/review3.txt"
It's okay, nothing special, does the job.
```

```python title="input/review4.txt"
meh
```

(`review4.txt` is deliberately too short to classify — that's the
`Filtered` case below.)

## An expand root and a map step over a folder

```python title="pipeline.py"
from rubedo import Filtered, ProcessResult, step, pipeline

POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent"}
NEGATIVE = {"terrible", "awful", "bad", "hate", "garbage", "poor"}


@step(name="scan", version="v1", shape="expand")
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


@step(name="classify", version="v1", depends_on=["scan"], index=["rating"])
def classify(scan: dict) -> ProcessResult:
    words = scan["text"].lower().split()
    if len(words) < 3:
        return Filtered(reason="too short to classify")
    pos = sum(1 for w in words if w.strip(".,!'\"") in POSITIVE)
    neg = sum(1 for w in words if w.strip(".,!'\"") in NEGATIVE)
    rating = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    return ProcessResult(value={"rating": rating, "word_count": len(words)})


p = pipeline(name="reviews", steps=[scan, classify])

if __name__ == "__main__":
    print(p.describe())
    print()
    print(p.plan())
    print()
    summary = p.run()
    print(
        f"\ncreated={summary.created_count} reused={summary.reused_count} "
        f"filtered={summary.filtered_count}"
    )
```

There's no `folder=` kwarg — ingestion is just a step. `scan` is a
parentless `@step(shape="expand")`: it walks `./input` and `yield`s each
file's own content (not just its path — the yielded payload is what gets
hashed into the lane's identity), and each yield mints its own
content-addressed lane. `classify` is an ordinary dependent `map` step.
`index=["rating"]` extracts the `rating` field of its output into the
search index at commit time — that's what makes it queryable by content
later, not just by which file produced it.

Run it:

```bash
uv run python pipeline.py
```

```text
Pipeline 'reviews' — roots: scan
  scan (v1) (root)
  classify (v1) <- scan

Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=7 reused=0 filtered=1
```

`scan` plans as a single `execute` — an `expand` root has no parent to
cache its enumeration against, so its actual lanes (one per file) are
unknowable until it runs. `classify` shows `pending`, not `execute`: its
output address depends on lanes `scan` hasn't minted yet. `p.run()` resolves
both: 7 materializations get created (4 `scan` file-lanes + 3 `classify`
lanes) and `review4.txt` — "meh", one word — gets **filtered**: its step
returned `Filtered(reason=...)` instead of a `ProcessResult`. That verdict
is cached like any other output; it isn't an error, and it isn't
re-decided every run.

Run it again, unchanged:

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=0 reused=7 filtered=1
```

`p.plan()` prints the exact same coarse shape as the first run — an `expand`
root always plans as `execute` (it never caches its own enumeration to
preview against) and everything downstream stays `pending`, even
immediately after a completed run. This is deliberate: `p.plan()` is a pure
dry-run and can't reach into a hypothetical future execution to say what an
unexecuted generator would yield. `p.run()`'s summary is where the real
story shows: `created=0 reused=7` — every lane, including the filtered
one, was a cache hit.

## Querying by an indexed field

`classify`'s `index=["rating"]` makes `rating` a queryable field of its
output, independent of which file produced it — useful precisely because
coordinates are content hashes (`row-<hash>`), not file names. `trace()`
takes a `Selection` and follows lineage from whatever matches — it doubles
as a read-only query tool:

```python title="query.py"
from rubedo import Selection, trace

result = trace(Selection.parse("step:classify rating:positive"))
print(result)
```

```text
Trace: 1 seed, 1 upstream, 0 downstream
  upstream   scan                 row-76410e514a8e             @ d0c553c86373  value={'path': 'review1.txt', 'text': 'This product is absolutely …
  seed       classify             row-76410e514a8e             @ d6e7fbcd2e3f
```

`step:` and other `key:value` terms before the colon are reserved engine
facts (`step`, `coord`, `version`, `live`, ...); anything else — here
`rating` — matches an indexed field. `trace()` walks the matched
`classify` output back to the `scan` output it came from, resolving the
root's stored payload so you can see *which file* — `review1.txt` — and
what text actually produced the verdict, since the coordinate itself
(`row-76410e514a8e`) doesn't say. See
[Guide: search and invalidation](guides/search-and-invalidation.md) for the
full selection language.

## Editing an input: surgical recompute

Edit `review2.txt` to flip its sentiment:

```python title="input/review2.txt"
Actually this turned out great, good value, I love it now.
```

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=2 reused=5 filtered=1
```

`p.plan()`'s coarse shape never changes — but `p.run()`'s summary shows only
`review2.txt`'s two lanes recomputed (`created=2`), while the other three
files' lanes reused (`reused=5`, including the filtered `review4.txt`).
Rubedo didn't diff the file or track which line changed: `scan` yields the
file's full content, that content is what gets hashed into its lane's
coordinate and address, and a different hash is a different lane entirely
— the old `review2.txt` lane simply isn't visited this run (an edited file
reads as removed + added, not changed). `classify`'s address in turn
depends on `scan`'s output content hash, so only the new lane's `classify`
recomputes; the other three files' content hashes are untouched, so their
addresses — and everything that consumed them — are still valid.

## Bumping a step's version

Widen the positive-word list and bump `classify`'s `version` to mark it a
deliberate behavior change:

```python
POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent", "value"}
```

```python
@step(name="classify", version="v2", depends_on=["scan"], index=["rating"])
```

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=3 reused=4 filtered=1
```

`scan` is completely untouched — its `version` and yielded content didn't
change, so all four of its lanes reuse. `classify` recomputes for **every**
lane (`created=3` live classifications + the 1 filtered lane, which still
counts as a fresh `Filtered()` this run, not a reused one — `filtered=1`
stays 1 both before and after the bump), regardless of whether that lane's
actual verdict changed, because `version` is folded into every lane's
output address: bumping it mints a whole new set of addresses for that
step. This is the deliberate, coarse-grained lever — contrast it with
[`code="auto"`](concepts/versioning.md), which recomputes only where the
function's source actually changed.

## Invalidating a selection and re-running

Say you want to force `classify` to re-check every currently-positive
verdict from the pipeline as it stands now (`version="v2"`) — maybe you
changed something upstream of this doc and want a clean re-verification.
`invalidate()` takes the same `Selection` language as `trace()`:

```python title="invalidate_positive.py"
from rubedo import Selection, invalidate

result = invalidate(
    Selection.parse("step:classify version:v2 rating:positive"),
    reason="re-checking positive calls",
)
print(result)
```

```text
{'run_id': 'run_997f9495c519', 'invalidated_count': 2, 'seed_count': 2, 'downstream_count': 0, 'materialization_ids': [11, 13]}
```

!!! note "Why `version:v2` is in the query"
    Rubedo never deletes a superseded or orphaned generation's ledger row —
    invalidation and version bumps are both liveness changes, not deletes
    (see [notes/invariants.md](notes/invariants.md)). After the version
    bump above, both the old `v1` classify outputs *and* the new `v2` ones
    are still live materializations, so a bare `rating:positive` selection
    would match generations from both versions. Scoping the query with
    `version:v2` selects only the current generation — a good habit any time
    a step has been bumped more than once.

Invalidation is a logical tombstone: `is_live` flips off, a
`materialization_lifecycle` row records why, and nothing is deleted. The
next `p.run()` sees those two lanes have no live output and recomputes them:

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=2 reused=5 filtered=1
```

Exactly the two invalidated lanes recompute (`created=2`); the rest —
including the neutral and filtered reviews — reuse. `p.plan()` can't preview
which two those'll be (it never sees past the `scan` root — see above),
but `trace()` can, both before invalidating (to see the blast radius) and
after (to confirm what actually moved): run `trace()` with the same
`Selection` and read the counts, exactly as
[Guide: search and invalidation](guides/search-and-invalidation.md) covers.

## Where to go next

- [Concepts: shapes](concepts/shapes.md) — `reduce`, `expand`, and `join`,
  the three shapes beyond the `map` step used here.
- [Concepts: sources](concepts/sources.md) — the ingestion recipes: folder,
  CSV, SQL table, cloud object storage.
- [Guide: search and invalidation](guides/search-and-invalidation.md) — the
  full `Selection` query language, `downstream=True`, and the CLI
  equivalents.
- [Guide: execution policies](guides/execution-policies.md) — retries, rate
  limits, `stale_after`, and assertions for flaky or expensive steps.
- [Guide: inspecting runs](guides/inspecting-runs.md) — `trace()`, the CLI,
  and the web dashboard.
- [Examples](examples.md) — the same ideas over real services: Hacker News,
  GitHub, an LLM, a SQL table.
