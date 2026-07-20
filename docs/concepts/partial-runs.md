# Partial runs and sampling

Run an expensive step on a frozen lane cohort, compare, then roll out —
without changing cache identity or replacing the authoritative full-run view.

## The idea

```python
from rubedo import RunScope, pipeline

# After a baseline full run, sample 100 lanes at the classify map step:
candidates = home.select("step:classify", run_id=baseline.run_id)
scope = RunScope.sample_n(
    anchor="classify",
    cells=candidates,
    n=100,
    seed="prompt-v2",
)

trial = p.run(scope=scope, targets=["classify"])  # kind='partial'
# …inspect trial outputs…
full = p.run()  # reuses the 100 classify addresses already computed
```

Two independent restrictions:

- **`scope`** — which lanes flow beyond an anchor map step.
- **`targets`** — where execution stops in the DAG (ancestor closure only).

Neither enters input hashes, output addresses, expand anchors, or
pipeline identity. A sampled map lane and the same lane in a full run
share one address, so the full run reuses the trial's work. A sampled
*aggregate* receives only the sampled parents, so its input hash differs
from the full aggregate and cannot masquerade as it.

## `RunScope`

A frozen public type: an anchor step name plus an exact coordinate set.
`origin` metadata (sampling strategy, seed, …) is diagnostic only —
persisted on the run for reproducibility, never hashed.

Constructors:

| Helper | Behavior |
| --- | --- |
| `RunScope.explicit(anchor, lanes)` | Exact coordinate set |
| `RunScope.from_cells(anchor, cells)` | From `Cell` / coordinate strings |
| `RunScope.sample_n(..., n, seed)` | Exact-N by ascending `hash(seed, coordinate)` |
| `RunScope.sample_fraction(..., fraction, seed)` | Hash-threshold; same seed ⇒ nested cohorts |

Anchor accepts a step name, `StepSpec`, or decorated step callable
(normalized before the engine).

### MVP anchor rules

Anchors must be **non-root**, `in_shape="one"`, `out_shape="one"` map
steps. Aggregate / fold / join / expand / root anchors are rejected —
they mint different coordinate namespaces. They may still appear
*downstream* of an anchor.

**`skip_cache` anchors are rejected.** Those steps are never materialized
or recorded on `RunCoordinateStatus`, so a cohort anchored there would be
invisible in the ledger and unsafe as a durable experiment boundary.
Anchor at a materialized map step instead.

## Partial vs full runs

| | Full | Partial |
| --- | --- | --- |
| `Run.kind` | `process` | `partial` |
| Trigger | default `p.run()` | `scope=` and/or `targets=` |
| Cohort | — | Exact lanes + targets in `Run.selection_json` |
| `home.current()` | Yes (latest completed process) | Never — query by `run_id` |
| Retention | Last-N + always protected as latest full | May sit in last-N; cannot displace latest full |

Out-of-scope lanes at the anchor are **absent**: no `filtered`
`StepDecision`, no `RunCoordinateStatus` row. Missing requested
coordinates (parent vanished since the baseline) produce one clear
warning event and contribute to `scope_requested` / `scope_reached` /
`scope_missing` tallies — never fabricated cell statuses.

`targets` omit downstream steps entirely (no RCS for them). Upstream of
the anchor always runs normally. Broad and deep schedules produce
identical addresses and statuses. `force` / `check_cache` / `params`
semantics are unchanged but apply only to requested cells.

## `plan()`

`p.plan(scope=..., targets=...)` is read-only and mirrors the same
restrictions. The returned `RunPlan` carries `kind`, `scope`, `targets`,
and `scope_counts` when applicable.

## What this is not

Run-to-run / step-version **diff** is a separate follow-up. This feature
is the execution primitive that makes "sample → compare → full rollout"
safe; comparison reads the recorded cohort from `selection_json`.
