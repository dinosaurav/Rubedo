# Run history and run-to-run diff

Compare two runs at one step — especially a full baseline against a
scoped `RunScope` trial — without writing anything to the ledger.

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

## `Home.runs` — historical lookup

```python
home.runs(pipeline="my-pipe", kind="partial", status="completed", limit=20)
```

Newest first. Filters: `pipeline`, `kind`, `status`, `limit`. Status is
*effective* status (`completed`, `running`, `interrupted`, …) — the same
projection the API/CLI run list uses. Partial runs are included unless
you filter them out. Each item is a `RunListItem` (`.id` is a valid run
ref for `diff`).

## `Home.diff` — outcomes

Match is by **coordinate** within the requested step's namespace.

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

## Coordinate universe

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

## Guards

- Both runs must exist and share a `pipeline_id`.
- The step must appear in each run's recorded definition or RCS cells.
- Run refs: run-id `str`, `RunSummary`, or `RunListItem`.
- **Read-only**: no Run / RCS / event / IHU writes.

## What this is not

No semantic/LLM scoring, no CLI verb, no dashboard UI yet — this is the
programmatic read surface that makes sample → compare → rollout inspectable.
See also [partial runs](partial-runs.md).
