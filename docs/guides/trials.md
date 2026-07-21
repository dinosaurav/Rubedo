# Trials: sample, diff, roll out

Run an expensive step on a frozen lane cohort, compare the trial against a
baseline, then roll out — without changing cache identity or replacing the
authoritative full-run view, and without writing anything extra to the
ledger.

## The workflow

```python
from rubedo import Home, RunScope, pipeline

home = Home.default()  # or the Home you injected into the pipeline

# 1. Full baseline on the current step version
baseline = p.run()

# 2. Bump the step version / prompt, sample a frozen cohort, trial it
candidates = home.select("step:classify", run_id=baseline.run_id)
scope = RunScope.sample_n(
    anchor="classify",
    cells=candidates,
    n=100,
    seed="prompt-v2",
)
trial = p.run(scope=scope, targets=["classify"])  # kind='partial'

# 3. Compare only the trial cohort (default when after is scoped at this step)
diff = home.diff(step="classify", before=baseline, after=trial)
print(diff)
# Diff my-pipe/classify: <baseline> → <trial>
#   unchanged=80 changed=18 added=0 removed=2 failed=0
#   changed   row-…
#             .label: 'A' → 'B'

# 4. Full rollout reuses the trial's addresses
p.run()
```

For a complete keyless run against a live API, `examples/paper_scout` fetches
OpenAlex metadata under a `12/min` budget, compares two shortlist-policy
versions on a deterministic cohort, prints the diff, and rolls out v2 while
reusing both sampled assessments and prior API fetches.

`RunSummary.diff` is the same comparison with the summary as `before`:

```python
diff = baseline.diff(trial, step="classify")  # uses the bound Home
```

## Scoping a run

Two independent restrictions on `p.run()` / `p.plan()`:

- **`scope`** — which lanes flow beyond an anchor map step.
- **`targets`** — where execution stops in the DAG (ancestor closure only).

Neither enters input hashes, output addresses, expand anchors, or
pipeline identity. A sampled map lane and the same lane in a full run
share one address, so the full run reuses the trial's work. A sampled
*aggregate* receives only the sampled parents, so its input hash differs
from the full aggregate and cannot masquerade as it.

### `RunScope`

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

### Anchor rules

Anchors must be **non-root**, `in_shape="one"`, `out_shape="one"` map
steps. Aggregate / fold / join / expand / root anchors are rejected —
they mint different coordinate namespaces. They may still appear
*downstream* of an anchor.

**`skip_cache` anchors are rejected.** Those steps are never materialized
or recorded on `RunCoordinateStatus`, so a cohort anchored there would be
invisible in the ledger and unsafe as a durable experiment boundary.
Anchor at a materialized map step instead.

### Partial vs full runs

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

`p.plan(scope=..., targets=...)` is read-only and mirrors the same
restrictions. The returned `RunPlan` carries `kind`, `scope`, `targets`,
and `scope_counts` when applicable.

## Comparing runs

`home.diff(step=..., before=..., after=...)` compares two runs at one
step. Match is by **coordinate** within the requested step's namespace.

| Outcome | Meaning |
| --- | --- |
| `unchanged` | Both sides succeeded (created/reused/filtered) with equal `output_identity` — even if version/address differs |
| `changed` | Both present; status or output identity/value differs |
| `added` | Only in `after` |
| `removed` | Only in `before` (or in the coordinate universe but absent from `after`) |
| `failed` | `after` is `failed` or `blocked` |

Filtered cells are compared honestly (they carry a cached verdict and an
`output_identity`). Value-level detail on `changed` cells:

- Nested **dicts** → dotted paths with `added` / `removed` / `changed`
- Top-level **strings** → unified text diff (`difflib`)
- Lists / scalars → `old` / `new` without invented element semantics

Arrow represents heterogeneous dictionary outputs with one union struct
schema, reading keys absent from an individual lane back as null. Structural
diff treats those null placeholders as absent so added/removed fields remain
useful. Consequently an explicit `None` value and a missing key are
indistinguishable at this presentation layer; `output_identity` remains the
authoritative changed/unchanged test.

### Coordinate universe

1. **`lanes=`** — explicit freeze (order preserved, duplicates dropped).
2. Else if **`after` is a partial** whose persisted `selection_json.anchor`
   equals the requested `step` — the exact `selection_json.lanes` cohort.
   A full baseline vs a 100-lane trial therefore does **not** report every
   unsampled baseline lane as `removed`. Lanes listed in the cohort but
   missing from `after` (scope_missing) stay in the result as `removed`.
3. Else — **union** of coordinates observed at that step in either run
   (typical full-vs-full comparison).

An explicitly empty cohort compares zero lanes and is reported as such; it
does not mean the full baseline was unchanged.

Scope lane keys are only meaningful at the anchor map step. Diffing a
downstream expand / join / aggregate against a partial's cohort is not
attempted automatically — pass `lanes=` or compare at the anchor.

### Guards

- Both runs must exist and share a `pipeline_id`.
- The step must appear in each run's recorded definition or RCS cells.
- Run refs: run-id `str`, `RunSummary`, or `RunListItem`.
- **Read-only**: no Run / RCS / event / IHU writes.

## `Home.runs` — historical lookup

```python
home.runs(pipeline="my-pipe", kind="partial", status="completed", limit=20)
```

Newest first. Filters: `pipeline`, `kind`, `status`, `limit`. Status is
*effective* status (`completed`, `running`, `interrupted`, …) — the same
projection the API/CLI run list uses. Partial runs are included unless
you filter them out. Each item is a `RunListItem` (`.id` is a valid run
ref for `diff`).

## What this is not

No semantic/LLM scoring, no CLI verb, no dashboard UI yet — this is the
programmatic read surface that makes sample → compare → rollout inspectable.
