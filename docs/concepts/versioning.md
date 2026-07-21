# Code Changes and Caching

A cached output is only useful if you can trust *why* it's being reused.
Rubedo gives you two independent axes on `@step` for that — `version` and
`code` — plus two policies for outputs that shouldn't live forever
(`stale_after`) or shouldn't be materialized at all (`skip_cache`). None of
these overlap: each answers a different question about when a step's output
stops being valid.

## `version` — the semantic identity

`version` is a plain string you own, and it's the first segment of every
output address (`hash(step, version, input_hash, ...)` — see
[model.md](model.md)). Bump it whenever you deliberately change what a step
*means* — a new prompt, a different algorithm, a schema change to the
output. Nothing about a version bump is automatic: you decide when the old
cached generation is no longer valid.

`name` and `version` both have defaults: `name` defaults to the decorated
function's `__name__`, and `version` defaults to `"0"`. Omitting them is
fine to start — `code="warn"` (the default either way, see below) already
means an unbumped default version never silently recomputes on a code
edit, it warns instead — so `@step()` with nothing else is exactly as safe
as spelling out `version="1"` by hand. Reach for an explicit `version=`
the moment you make a deliberate behavior change, same as you always would.

```python
@step()
def parse(row: dict): ...   # name="parse", version="0", code="warn"
```

Two steps whose names collide — most commonly two same-named functions
defined in different modules — fail loudly at pipeline-construction time
regardless of whether either name was explicit or defaulted; the error
names both functions so you can tell the two apart. `@step` also works
bare, with no parens, if every other argument is staying at its default.

`version` is also the escape hatch for edits the engine has no way to see.
If a step calls a helper function, imports different data, or depends on
some external config that isn't hashed into its identity, changing that
helper doesn't move any hash on its own — the step's source code, as far as
`code="warn"` is concerned, hasn't visibly changed either. Bump `version`
manually in that case, exactly as you would for a deliberate behavior
change, because from the cache's point of view they're the same kind of
event.

```python
@step(version="1.0.0")
def enrich(row: dict): ...
```

(`version="auto"` is rejected outright — it collides with the *unrelated*
`code="auto"` axis below, and the error message says so.)

## `code` — what a source edit means

`code` decides, independently of `version`, what happens when the step
function's *source text* changes between runs. Two modes:

- **`code="warn"` (default).** Editing the function body never triggers a
  recompute on its own — the cached output for an unchanged `version` stays
  live. But if planning reuses an output whose recorded `code_hash` no
  longer matches the function's current source, it's **code drift**, and
  Rubedo tells you loudly, in three places at once:
    - a `UserWarning` raised during the run (`warnings.warn`, visible in
      run output);
    - a `code_drift_detected` row in the run's event log;
    - a `p.plan()` dry-run's warnings list, so you can catch it *before*
      committing to a run.

  This is the right default for anything expensive or non-deterministic —
  an LLM call, a scrape — where "the code changed, so let's just re-run it"
  would silently re-spend money or re-roll a non-deterministic result you
  didn't ask to replace.

- **`code="auto"`.** The function's source hash (`inspect.getsource`,
  SHA-256'd) folds directly into the output address as an extra segment.
  Any edit to the function body is then, structurally, a different address
  — the old generation simply stops being addressed, no version bookkeeping
  needed, no warning either (there's nothing to warn about: the recompute
  already happened). Right for cheap, deterministic steps you edit often —
  parsing, formatting, small transforms — where re-running on every tweak
  is exactly what you want and costs nothing.

```python
@step(code="auto")
def parse(row: dict): ...   # any edit here recomputes automatically
```

`code="auto"` requires an inspectable function source (a real `def`, not
something dynamically generated) — the decorator raises at definition time
if `inspect.getsource` can't find it.

!!! note "These two axes are genuinely independent"
    `version` is a fact you assert; `code` is a policy for facts the engine
    can detect on its own. You can bump `version` on a `code="auto"` step
    (rare — the source hash already forces a recompute on any edit) or run
    a `code="warn"` step for years without ever touching `version` if the
    body never changes. Neither implies the other.

## `stale_after` — TTL expiry

`stale_after` puts a wall-clock TTL on an output, independent of both axes
above: `stale_after="24h"` (also `"30min"`, `"7d"`, …). Past the TTL —
measured from `refreshed_at` if set, else `created_at` — the next `p.run()`
re-executes the step for that lane even though the cached generation is
otherwise still address-valid.

What happens to the cache after that recompute follows the same generations
rule as everything else (see [model.md](model.md)):

- **Different bytes** → the old generation is superseded; downstream
  recomputes, because the new output has a new content hash.
- **Identical bytes** → the generation is `refreshed`: the freshness clock
  resets to now, but nothing downstream sees a change (same content hash,
  same input to consumers).

This is the natural policy for scraped or otherwise time-sensitive data —
"periodically re-check, but don't force a full re-scrape's worth of
downstream recomputation unless the world actually changed." `expand` steps
use it the same way: `stale_after` on an `expand` gives you periodic
re-scrape of the whole fan-out, since the TTL is checked against the cache
anchor (see [shapes.md](shapes.md#expand-1n-fan-out)).

```python
@step(stale_after="24h")
def enrich(row: dict): ...
```

## `skip_cache` — inline utils

`skip_cache=True` marks a step as an **inline util**: a quick, deterministic
helper you factor out purely to keep another step's code readable, not
because its output deserves its own row in the ledger. A `skip_cache` step:

- **Is never materialized or recorded.** No Arrow lane-store row, no
  lineage edge, nothing to `trace()` or search.
- **Fuses its identity into its consumers' cache keys instead.** A
  consumer's `input_hash` is computed from the util's `(step, version,
  parent content[, params][, code])` — an `EphemeralRef`'s
  `output_content_hash` is that identity hash, not a real output hash — so
  changing the util's version, code (under `code="auto"`), or upstream
  input still correctly changes what its consumers cache under, without the
  util ever being stored.
- **Executes lazily, memoized per run.** It only actually runs the first
  time a consumer needs its value for a given coordinate in a given run
  (`_RunMemo`, reentrant across chained `skip_cache` steps) — a fully
  cached run where every consumer reuses skips the util entirely, and a run
  where three different steps all depend on the same util computes it once
  and reuses the in-memory result for the rest.
- **Skips serialization.** Values pass directly in memory between the util
  and its consumer, with no store round-trip.

```python
@step(skip_cache=True)
def normalize(row: dict) -> dict:
    return {k: v.strip().lower() for k, v in row.items()}
```

A `skip_cache` step must have a consumer — the constructor rejects one with
no downstream step, since its output would never be computed or stored at
all. It also can't be an `aggregate` (a reduction's whole point is to be
materialized), an `expand` (nothing to anchor an ungrounded fan-out
against), or a `join` parent (a join needs its sides' output fields
committed to match on), and none of `stale_after` or the retry/
rate-limit policies apply to it — there's no stored output for a TTL to
attach to, and no materialization step for a retry to wrap.

### When *not* to use it

`skip_cache` trades durability for zero storage and zero ledger overhead —
worth it only when the step is genuinely cheap, fast, and deterministic. If
a step is:

- **expensive** (an LLM call, a paid API, a slow computation),
- **flaky** (network calls, anything that benefits from `retries`/
  `rate_limit`), or
- **non-deterministic** (scraping, sampling, anything where "what did this
  actually return" is a fact worth keeping),

it deserves materialization, not `skip_cache` — you want that output
addressed, cached, retried, rate-limited, searchable, and inspectable via
`trace()`, all of which `skip_cache` deliberately gives up. Reach for
`skip_cache` only for the boring glue code you'd otherwise inline directly
into a bigger step just to avoid the ledger noise of one more row per lane.

## Next

- [model.md](model.md) — how `version`/`code`/`stale_after` feed into the
  output address and the generations protocol.
- [shapes.md](shapes.md) — `stale_after` on `expand`'s cache anchor, and
  where `skip_cache` can and can't appear per shape.
- [`../guides/inspecting-runs.md`](../guides/inspecting-runs.md) — reading
  code-drift warnings and stale/reuse decisions out of `p.plan()` and run
  output.
