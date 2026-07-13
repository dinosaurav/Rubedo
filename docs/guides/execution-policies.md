# Execution Policies

A step is a Python function plus a set of policies declared as `@step(...)`
keyword arguments — how it retries, how fast it's allowed to run, what it's
allowed to produce, and which pool runs it. None of these affect cache
identity (the output address doesn't encode `retries=3` or `rate_limit=`);
they only govern *how* a step gets from "needs to execute" to "committed."
This page covers each one, plus the two things that govern a whole
pipeline's runs: `schedule` and `Filtered`.

## Retries

```python
@step(name="enrich", version="1.0.0",
      retries=3, retry_on=(TimeoutError, ConnectionError),
      retry_delay=1, retry_backoff=2)
def enrich(row: dict): ...
```

`retries` (default `0`) is the number of *extra* attempts after the first —
`retries=3` means up to 4 total calls to the step function for one lane.
`retry_delay` is the pause in seconds before the next attempt; `retry_backoff`
multiplies that delay after every retry (`1 → 2 → 4` seconds for
`retry_delay=1, retry_backoff=2`).

Only exceptions matching `retry_on` are retried — anything else fails the
lane on the first attempt.

!!! warning "`retry_on` defaults to `Exception` — narrow it explicitly"
    `@step`'s `retry_on` parameter defaults to plain `Exception`, so setting
    `retries=3` with no `retry_on` retries *everything*: a malformed row, a
    bug in your parsing code, a `KeyError` — same as a flaky timeout. On a
    paid API that just multiplies the bill for a failure that was never
    going to succeed. Always pair `retries` with a narrow `retry_on` on
    anything that costs money or time per call:

    ```python
    retry_on=(TimeoutError, ConnectionError)   # transient — retry
    # not: a ValueError from a bad prompt, a KeyError from a malformed row
    ```

    [Assertions](#assertions) run inside the same try/except as the step
    call, so a failed assertion is also just an exception — it's retried
    only if its exception type is in `retry_on`. A narrow `retry_on` also
    keeps assertion failures from being retried pointlessly.

Every attempt — successful or not — lands in the run event log: a failed
attempt that goes on to retry is recorded as `step_attempt_failed` with
`{"attempt": i, "max_attempts": retries + 1}`; the final outcome (whichever
attempt it lands on) carries the attempt count too. `rubedo show <run_id>
--failed` and `RunSummary.failures()` surface the last attempt's error.

## Rate limiting

```python
@step(name="enrich", version="1.0.0", rate_limit="30/min")
def enrich(row: dict): ...
```

`rate_limit` takes `"<count>/<unit>"` (`s`/`sec`/`second`, `m`/`min`/`minute`,
`h`/`hour` — e.g. `"10/min"`, `"2/s"`, `"500/hour"`). One `_RateLimiter`
instance is created per step per run and shared across *every* worker and
every retry attempt for that step: it paces calls to an even interval
(`period / count` seconds apart) rather than letting a burst through at the
top of every window and stalling for the rest. A `workers=8` step with
`rate_limit="30/min"` still averages 30 calls/min in total, not 30 per
worker — and a retried call waits its turn in the same limiter as a fresh
one.

## Assertions

```python
def check_price_positive(val: dict):
    if val["price"] < 0:
        raise ValueError("Negative price")

@step(name="enrich", version="1.0.0", assertions=[check_price_positive])
def enrich(row: dict): ...
```

`assertions` is a list of callables, each given the step's returned value.
They run **after** the step function returns and **before** the output
commits to the ledger — raise (or fail an `assert`) in any of them and the
lane fails instead of materializing: bad data never reaches downstream
steps or the object store. Since this check happens inside the same
try/except the retry loop uses, an assertion failure is subject to the same
`retry_on` filtering as any other exception (see the warning above).

Use assertions for data-quality gates you want enforced every time a lane
*actually executes* (skip_cache steps don't support them, and a `reuse`
decision never re-runs assertions — they ran once, when the output was
first created).

## Process pools for CPU-bound work

```python
@step(name="parse_wordlist", version="1", executor="process")
def parse_wordlist(text: str): ...
```

`executor` is `"thread"` (default — right for I/O-bound work: LLM calls,
HTTP, file reads, where the GIL isn't the bottleneck) or `"process"`. A
process-executor step runs in a [`loky`](https://loky.readthedocs.io/)
process pool, with arguments and results serialized via `cloudpickle` — so,
unlike the stdlib `multiprocessing`, closures and locally-defined functions
work fine. Reach for `executor="process"` when the step itself is CPU-bound
(parsing, hashing, numeric work) and would otherwise contend with every
other thread for the GIL. Retries, rate limiting, and the retry/backoff
delay all still run in the orchestrating thread — the process pool is
purely where the step body executes.

Each step with `executor="process"` gets its own pool, created on first use
and shut down at the end of the segment that runs it — a mixed pipeline
(some steps `"thread"`, some `"process"`) is completely normal.

## `schedule`: execution order, never results

```python
pipeline(name="p", steps=[...], schedule="broad")   # default
pipeline(name="p", steps=[...], schedule="deep")
```

`schedule` is a pipeline-construction setting (alongside `retention=` and
`home=`), not a per-run argument — it applies to every `p.run()`/`p.plan()`
call for that pipeline. It picks the *order* work happens in — it never
changes what gets computed, what the addresses are, or what ends up in the
ledger. Because addresses and cache identity are order-independent,
`"broad"` and `"deep"` runs of the same pipeline against the same state
always produce byte-identical ledger rows; each mode fully reuses whatever
the other one already computed.

- **`"broad"` (default)** completes a step across every lane before the
  next step starts — the classic staged loop: plan the whole step, execute
  every lane that needs it, commit each as it finishes, then move on. This
  gives you a natural inspection checkpoint between stages: you see *all*
  of a paid step's output (and can `Ctrl-C`, or `rubedo show` the partial
  run) before the next stage spends anything.
- **`"deep"`** lets each lane race ahead through a run of consecutive 1:1
  `map` steps as soon as *its own* upstream input has committed, instead of
  waiting for sibling lanes to finish the current stage. First results
  land sooner — useful when one slow lane (a large scrape, a big file)
  would otherwise stall the whole stage under `"broad"`.

`reduce`, `join`, `expand`, and any multi-parent `map` step always
synchronize on their full set of parent lanes in either mode — fan-in and
fan-out are barriers by construction, not something scheduling can pipeline
around.

## Declining an item: `Filtered`

```python
from rubedo import Filtered

@step(name="screen", version="1")
def screen(row: dict):
    if not looks_relevant(row):
        return Filtered(reason="off-topic")
    return {"ok": True, **row}
```

A step can decline a coordinate by returning `Filtered(reason=...)` instead
of a normal value. Downstream steps see that lane with status `filtered`
and skip it — they never execute for it. The verdict itself is a
first-class, cached output: it's committed to the ledger like any other
materialization (`RunCoordinateStatus.status == "filtered"`), so an
expensive LLM-based screening step runs its judgment **once per distinct
input, not once per run** — the next run reuses the filtered verdict just
like it would reuse a normal result. When the input changes, the address
changes too, and the step decides fresh.

`skip_cache=True` steps cannot return `Filtered` — filtering is a cacheable
decision, so a filter step must be materialized.

A `reduce` or `join` step unconditionally drops a filtered parent lane and
proceeds with the survivors — unlike a failed or blocked parent, a filtered
one never triggers `on_failed="block"` (see
[`../concepts/shapes.md`](../concepts/shapes.md)); a decline is not a
failure, so it never blocks a fan-in. See
[`../guides/inspecting-runs.md`](inspecting-runs.md) for how `p.plan()`
reports a `filtered` decision before you ever run anything.
