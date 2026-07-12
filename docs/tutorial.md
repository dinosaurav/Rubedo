# Tutorial

This walks through a small, real pipeline end to end: a folder of review
files gets read and classified as positive/negative/neutral. Along the way
you'll index an output field and query it, edit an input and watch surgical
recompute, bump a step's `version` and see what that invalidates, decline an
input with `Filtered`, and finish by hand-invalidating a selection and
re-running.

Every command below was actually run to produce the output shown — copy the
code blocks into a real directory and you'll see the same shapes (exact hash
prefixes will differ, since they're content-addressed).

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

## Two map steps over a folder

```python title="pipeline.py"
from rubedo import Filtered, ProcessResult, describe, plan, run, step, pipeline

POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent"}
NEGATIVE = {"terrible", "awful", "bad", "hate", "garbage", "poor"}


@step(name="read_review", version="v1")
def read_review(path: str):
    return {"text": open(path).read()}


@step(name="classify", version="v1", depends_on=["read_review"], index=["rating"])
def classify(read_review: dict) -> ProcessResult:
    words = read_review["text"].lower().split()
    if len(words) < 3:
        return Filtered(reason="too short to classify")
    pos = sum(1 for w in words if w.strip(".,!'\"") in POSITIVE)
    neg = sum(1 for w in words if w.strip(".,!'\"") in NEGATIVE)
    rating = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    return ProcessResult(value={"rating": rating, "word_count": len(words)})


p = pipeline(id="reviews", name="Review Classifier", folder="input", steps=[read_review, classify])

if __name__ == "__main__":
    print(describe(p))
    print()
    print(plan(p))
    print()
    summary = run(p)
    print(
        f"\ncreated={summary.created_count} reused={summary.reused_count} "
        f"filtered={summary.filtered_count}"
    )
```

`read_review` is a source-backed `map` root (`folder="input"`, no
`depends_on`): one lane per file, keyed by relative path, payload = absolute
path. `classify` is an ordinary dependent `map` step. `index=["rating"]`
extracts the `rating` field of its output into the search index at commit
time — that's what makes it queryable by content later, not just by
filename.

Run it:

```bash
uv run python pipeline.py
```

```text
Pipeline 'reviews' over folder:input
  read_review (v1) (root)
  classify (v1) <- read_review

Plan for 'reviews' over folder:input: 4 execute, 4 pending
  execute  read_review          review3.txt @ 6bba23b90110
  execute  read_review          review2.txt @ a5a048b43aa8
  execute  read_review          review1.txt @ c9aa2d8b820b
  execute  read_review          review4.txt @ c93d2628ed03
  pending  classify             review1.txt
  pending  classify             review2.txt
  pending  classify             review3.txt
  pending  classify             review4.txt

created=7 reused=0 filtered=1
```

`plan()` shows `classify` as `pending`, not `execute` — its output address
depends on `read_review`'s output, which doesn't exist yet at plan time.
`run()` resolves that: 7 materializations get created (4 `read_review` + 3
`classify`) and `review4.txt` — "meh", one word — gets **filtered**: its step
returned `Filtered(reason=...)` instead of a `ProcessResult`. That verdict is
cached like any other output; it isn't an error, and it isn't re-decided
every run.

Run it again, unchanged:

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over folder:input: 8 reuse
  ...
created=0 reused=7 filtered=1
```

Every lane's `plan()` action reads `reuse` — including `review4.txt`'s
`classify` lane, whose *cached filter decision* is what's being reused. But
notice the run summary still reports it under `filtered_count`, not
`reused_count`: a filtered marker isn't a "usable output" in the sense
`reused_count` means, so the ledger keeps counting it as `filtered` on every
run it's read back, even though nothing executed.

## Querying by an indexed field

`classify`'s `index=["rating"]` makes `rating` a queryable field of its
output, independent of which file produced it. `trace()` takes a
`Selection` and follows lineage from whatever matches — it doubles as a
read-only query tool:

```python title="query.py"
from rubedo import Selection, trace

result = trace(Selection.parse("step:classify rating:positive"))
print(result)
```

```text
Trace: 1 seed, 1 upstream, 0 downstream
  upstream   read_review          review1.txt                  @ c9aa2d8b820b  value={'text': 'This product is absolutely amazing and wonderful, …
  seed       classify             review1.txt                  @ 88ca616d682d
```

`step:` and other `key:value` terms before the colon are reserved engine
facts (`step`, `coord`, `version`, `live`, ...); anything else — here
`rating` — matches an indexed field. `trace()` walks `review1.txt`'s
`classify` output back to the `read_review` output it came from, resolving
the root's stored payload so you can see what text actually produced the
verdict. See [Guide: search and invalidation](guides/search-and-invalidation.md)
for the full selection language.

## Editing an input: surgical recompute

Edit `review2.txt` to flip its sentiment:

```python title="input/review2.txt"
Actually this turned out great, good value, I love it now.
```

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over folder:input: 1 execute, 1 pending, 6 reuse
  reuse    read_review          review3.txt @ 6bba23b90110
  execute  read_review          review2.txt @ 273c02673025
  reuse    read_review          review1.txt @ c9aa2d8b820b
  reuse    read_review          review4.txt @ c93d2628ed03
  pending  classify             review2.txt
  reuse    classify             review1.txt @ 88ca616d682d
  reuse    classify             review3.txt @ e1b94b177e71
  reuse    classify             review4.txt @ 1c766d499336

created=2 reused=5 filtered=1
```

Only `review2.txt`'s two lanes recompute. Rubedo didn't diff the file or
track which line changed — `read_review`'s output address is
`hash(step, version, input_hash)`, `input_hash` folds in the file's content
hash, and a different hash is a different address. The other three files'
content hashes are untouched, so their addresses — and every step that
consumed them — are still valid.

## Bumping a step's version

Widen the positive-word list and bump `classify`'s `version` to mark it a
deliberate behavior change:

```python
POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent", "value"}
```

```python
@step(name="classify", version="v2", depends_on=["read_review"], index=["rating"])
```

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over folder:input: 4 execute, 4 reuse
  reuse    read_review          review3.txt @ 6bba23b90110
  reuse    read_review          review2.txt @ 273c02673025
  reuse    read_review          review1.txt @ c9aa2d8b820b
  reuse    read_review          review4.txt @ c93d2628ed03
  execute  classify             review1.txt @ f9f01d8f1da1
  execute  classify             review2.txt @ 0a95750802f8
  execute  classify             review3.txt @ 8f6c20974f23
  execute  classify             review4.txt @ 3c11179dc494

created=3 reused=4 filtered=1
```

`read_review` is completely untouched — its `version` and inputs didn't
change. `classify` recomputes for **every** lane, regardless of whether that
lane's actual verdict changed, because `version` is folded into every
lane's output address: bumping it mints a whole new set of addresses for
that step. This is the deliberate, coarse-grained lever — contrast it with
[`code="auto"`](concepts/versioning.md), which recomputes only where the
function's source actually changed.

`review4.txt` ("meh") is still `Filtered` after the recompute — its word
count is still under the threshold; `filtered_count` stays 1 both before and
after the bump, it just reflects a freshly-computed `Filtered()` this time
instead of a reused one.

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
{'run_id': 'run_5003ceaa8c88', 'invalidated_count': 2, 'seed_count': 2, 'downstream_count': 0, 'materialization_ids': [11, 12]}
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
next `run()` sees those two lanes have no live output and recomputes them:

```bash
uv run python pipeline.py
```

```text
Plan for 'reviews' over folder:input: 2 execute, 6 reuse
  reuse    read_review          review3.txt @ 6bba23b90110
  reuse    read_review          review2.txt @ 273c02673025
  reuse    read_review          review1.txt @ c9aa2d8b820b
  reuse    read_review          review4.txt @ c93d2628ed03
  execute  classify             review1.txt @ f9f01d8f1da1
  execute  classify             review2.txt @ 0a95750802f8
  reuse    classify             review3.txt @ e1b94b177e71
  reuse    classify             review4.txt @ 3c11179dc494

created=2 reused=5 filtered=1
```

Exactly the two invalidated lanes recompute — `review3.txt` (neutral) and
`review4.txt` (filtered) were never touched, so they reuse. `plan()` before
`run()` throughout this tutorial is exactly the habit worth keeping: it's
the same decision engine `run()` uses, so it tells you precisely what's
about to happen — and what it costs — before anything executes or gets
billed.

## Where to go next

- [Concepts: shapes](concepts/shapes.md) — `reduce`, `expand`, and `join`,
  the three shapes beyond the `map` steps used here.
- [Guide: search and invalidation](guides/search-and-invalidation.md) — the
  full `Selection` query language, `downstream=True`, and the CLI
  equivalents.
- [Guide: execution policies](guides/execution-policies.md) — retries, rate
  limits, `stale_after`, and assertions for flaky or expensive steps.
- [Guide: inspecting runs](guides/inspecting-runs.md) — `trace()`, the CLI,
  and the web dashboard.
- [Examples](examples.md) — the same ideas over real services: Hacker News,
  GitHub, an LLM, a SQL table.
