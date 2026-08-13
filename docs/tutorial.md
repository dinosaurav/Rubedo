# Tutorial

Install, build a small pipeline, actually use it. A folder of reviews,
classified as positive / negative / neutral. Along the way you query an
output field by content, edit an input and watch only that file recompute,
bump a step's `version`, decline an input with `Filtered`, and
hand-invalidate a selection.

Every command below was actually run to produce the output shown — copy the
code blocks into a real directory and you'll see the same shapes (exact hash
prefixes and coordinates will differ, since they're content-addressed).

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only dashboard.
The `s3` extra adds the S3-compatible cloud store. To hack on Rubedo
itself, clone the repo and `uv sync`.

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

!!! warning "`.rubedo/` follows the working directory"
    This tutorial `cd`s into `reviews-demo/`, so state lands in
    `reviews-demo/.rubedo/` — not in whatever repo you cloned. `rubedo ls`
    from the parent directory will show nothing. To pin it:
    `export RUBEDO_HOME=/path/to/.rubedo`. The Python API takes a `Home`
    instance (`pipeline(name="...", home=Home("/path"))`), not a path
    string.

## An expand root and a map step over a folder

```python title="pipeline.py"
from rubedo import Filtered, step, pipeline

POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent"}
NEGATIVE = {"terrible", "awful", "bad", "hate", "garbage", "poor"}


@step(check_cache=False)
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}


@step
def classify(scan: dict):
    words = scan["text"].lower().split()
    if len(words) < 3:
        return Filtered(reason="too short to classify")
    pos = sum(1 for w in words if w.strip(".,!'\"") in POSITIVE)
    neg = sum(1 for w in words if w.strip(".,!'\"") in NEGATIVE)
    rating = "positive" if pos > neg else "negative" if neg > pos else "neutral"
    return {"rating": rating, "word_count": len(words)}


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

There's no `folder=` kwarg — ingestion is a parentless generator that
yields each file's content (that payload is what gets hashed).
`check_cache=False` re-lists the folder every run so edits show up.
`classify`'s argument name `scan` is the parent; `name` defaults to the
function name and `version` to `"0"`. A generator infers `expand`; a
plain `def` infers `map`. See [How it works](concepts/model.md).

Run it:

```bash
uv run python pipeline.py
```

```text
Pipeline 'reviews' — roots: scan
  scan (0) (root)
  classify (0) <- scan

Plan for 'reviews' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  classify             @root

created=7 reused=0 filtered=1
```

`scan` plans as a single `execute` — this tutorial declares
`check_cache=False`, so the source re-lists the folder every run and the
dry-run has no cached enumeration to preview against. Its actual lanes
(one per file) are unknowable until it runs. `classify` shows `pending`,
not `execute`: its output address depends on lanes `scan` hasn't minted
yet. `p.run()` resolves both: 7 materializations get created (4 `scan`
file-lanes + 3 `classify` lanes) and `review4.txt` — "meh", one word —
gets **filtered**: its step returned `Filtered(reason=...)` instead of a
value. That verdict is cached like any other output; it isn't an error,
and it isn't re-decided every run.

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

`p.plan()` prints the exact same coarse shape as the first run — a
`check_cache=False` `expand` root always plans as `execute` (it never
caches its own enumeration to preview against, by design: that's what lets
it notice a folder edit) and everything downstream stays `pending`, even
immediately after a completed run. (A root *without* `check_cache=False`
is anchor-cached like any `expand` and would instead plan every lane as
`reuse` here — see [How it works](concepts/model.md).) This is
deliberate: `p.plan()` is a pure
dry-run and can't reach into a hypothetical future execution to say what an
unexecuted generator would yield. `p.run()`'s summary is where the real
story shows: `created=0 reused=7` — every lane, including the filtered
one, was a cache hit.

## Querying by an output field

`classify`'s `rating` output field is queryable, independent of which file
produced it — useful precisely because coordinates are content hashes
(`row-<hash>`), not file names. `trace()` takes a `Selection` and follows
lineage from whatever matches — it doubles as a read-only query tool:

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
`rating` — matches a field of the step's output struct. `trace()` walks the matched
`classify` output back to the `scan` output it came from, resolving the
root's stored payload so you can see *which file* — `review1.txt` — and
what text actually produced the verdict, since the coordinate itself
(`row-76410e514a8e`) doesn't say. See
[Find and invalidate a row](guides/search-and-invalidation.md) for the
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
deliberate behavior change — the first time this pipeline passes `version=`
explicitly (it's defaulted to `"0"` until now):

```python
POSITIVE = {"amazing", "wonderful", "love", "great", "good", "excellent", "value"}
```

```python
@step(version="v2")
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
[`code="auto"`](concepts/model.md#when-code-changes), which recomputes only where the
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
{'run_id': 'run_ab4f8e76c803', 'invalidated_count': 2, 'seed_count': 2, 'downstream_count': 0, 'addresses': ['07ced3b0180fd64ce423a9b3d79e438f5fa7c525e66090ce9886093f708bf926', '7ded8265983de02cb923a7bc68d5c812e1a94978a9c73512d7fc17dada6cb64b']}
```

!!! note "Why `version:v2` is in the query"
    Rubedo never deletes a superseded or orphaned generation's ledger row —
    invalidation and version bumps are both liveness changes, not deletes
    (see [development/invariants.md](development/invariants.md)). After the version
    bump above, both the old default-version (`"0"`) classify outputs *and*
    the new `v2` ones are still live materializations, so a bare
    `rating:positive` selection would match generations from both versions.
    Scoping the query with
    `version:v2` selects only the current generation — a good habit any time
    a step has been bumped more than once.

Invalidation is a logical tombstone: it flips `input_hash_usages.fulfilled`
to `False` for the two matched addresses, and nothing is deleted — the
Arrow lane-store rows stay as history. The
next `p.run()` sees those two lanes have no live (fulfilled) output and
recomputes them:

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
[Find and invalidate a row](guides/search-and-invalidation.md) covers.

## Next

Look at the run in a browser:

```bash
pip install "rubedo[server]"
rubedo serve                    # http://127.0.0.1:8000
```

The dashboard is read-only — [Inspect a run](guides/inspecting-runs.md)
covers `serve`, `p.plan()`, `Home`, and retention.

- [How it works](concepts/model.md) — lanes, addresses, shapes, the ledger.
- [Examples](examples.md) — the same ideas over real services.
