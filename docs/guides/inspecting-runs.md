# Inspect a run

Read-only ways to look at what a pipeline did or would do, without changing
anything: `p.plan()` (before you run), the `Cell` query surface on `Home` /
`RunSummary` (after you run), `trace()` (lineage from any point), and
`rubedo du` / `storage_report()` (how big the store is and why). Those, plus
the run event log and the web dashboard, are covered here.

## `Cell`: reading what a run produced

After `summary = pipe.run()`, the unit of read is a **`Cell`** — one
(run, step, lane) outcome with status and optional payload:

```python
from rubedo import Home

# Bound on the summary returned by run():
for cell in summary.cells("count_lines", resolve_output=True):
    print(cell.coordinate, cell.status, cell.output)

# Same payloads as a coord → value map (unchanged):
summary.output_for("count_lines")

# Latest live outputs across pipelines (same as /api/current-outputs):
home.current(pipeline="count-lines", resolve_output=True)

# Selection language — including output fields like path:
home.select("step:scan path:a.txt", resolve_output=True)
```

`resolve_output=False` (the default) lists cells cheaply without
deserializing spilled payloads. Pipeline/step identity always comes from
the run ledger, not from shared Arrow row metadata.

`home.runs(...)` lists historical runs (newest first; filters for
pipeline / kind / effective status). `home.diff(step=..., before=...,
after=...)` compares one step across two runs — cohort-aware when
`after` is a scoped partial at that step. See
[trials: sample, diff, roll out](trials.md).

## `p.plan()`: the dry-run

```python
print(pipe.plan(params={"min_lines": 0}))
```

`p.plan()` runs the planning phase alone and **writes nothing** — no ledger
rows, no object-store bytes, nothing. It walks every lane of every step and
reports what `p.run()` *would* do and why:

```text
Plan for 'count-lines' over : 1 execute, 3 pending
  execute  input_files          @root
  pending  read_lines           @root
  pending  count_lines          @root
  pending  total_lines          @all
```

Each line is one decision: `<action>  <step>  <coordinate>[ @ <address prefix>]`.
The possible actions:

- **`reuse`** — a live materialization already exists at this address;
  `p.run()` would skip execution and reuse it.
- **`execute`** — no live materialization exists (or `force=True` was
  passed); `p.run()` would call the step function.
- **`blocked`** — a required parent lane failed or was blocked (and the
  step's `on_failed="block"`, or every parent is unavailable); the step
  cannot run for this lane.
- **`pending`** — the decision depends on an *upstream execution* whose
  output — and therefore this lane's own address — isn't knowable without
  actually running it. This is normal downstream of anything that hasn't
  resolved yet: in the example above, `input_files` is a root `expand`
  with `check_cache=False`, so it always plans as `execute` (it re-scans
  external state every run and has no cached enumeration for the dry-run
  to preview against). Nothing downstream can be addressed until it runs
  and its children's content hashes are known. A root *without*
  `check_cache=False` is anchor-cached against `@root` and would instead
  plan its children as `reuse` when the anchor is live — see
  [`../concepts/sources.md`](../concepts/sources.md).
- **`filtered`** — a parent lane was declined with `Filtered(...)`; this
  step skips it without running.

Two extra flags ride alongside `execute`/`reuse` and show up as warnings:

- **`stale`** — an `execute` caused by an expired `stale_after`, not a cache
  miss. The existing generation is still live; the step reruns to
  re-verify it, and identical bytes will just refresh its clock rather than
  create a new generation.
- **code-drift** — a `reuse` whose step's *source code* has changed since
  the cached output was produced (same `version` string, different
  function body, under the default `code="warn"`). `p.plan()` surfaces this
  as a top-level warning, e.g.:

  ```text
  Plan for 'enrich-leads' over csv:leads.csv: 40 reuse
    ! Step 'enrich' source code changed but version is still '1.0.0':
      reusing 12 cached output(s) computed by the old code. Bump the
      version (or use code='auto') to recompute.
  ```

  It's legal — `code="warn"` is opt-in to *not* recompute on every edit —
  but it's exactly the situation that costs you a debugging session if you
  don't notice it, so `p.plan()` puts it front and center rather than burying
  it per-lane. See [`../concepts/versioning.md`](../concepts/versioning.md)
  for the `version` vs `code` distinction.

`pipe.plan(force=True)` reports what a `force=True` run would do (treats
every address as a cache miss, ignoring existing materializations). To
point at a different `.rubedo/` root, pass a `Home` when constructing the
pipeline (`pipeline(name=..., home=Home("/other/path"))`) — it applies to
both `.plan()` and `.run()` for that pipeline. Different `Home` instances
are independent storage roots; concurrent runs against them in one
process are safe — see [Tutorial](../tutorial.md) for the working-directory
rule.

## `trace()`: lineage from any point

Covered in depth in
[`../guides/search-and-invalidation.md`](search-and-invalidation.md) — the
short version: `trace(selection)` seeds on whatever a `Selection` matches
and walks recorded lineage edges both ways. `rubedo trace "<query>"` is the
CLI form. By default only **live** materializations seed the trace;
`include_superseded=True` (CLI `--all`) also seeds superseded, invalidated,
and pruned generations, so you can ask "what did this used-to-be-live
output feed, historically?" as well as "what does the current answer
depend on?"

## `rubedo du` / `storage_report()`: why is `.rubedo` this big?

```bash
rubedo du
```

```text
Object store: 277 objects, 5.7 MiB (349 materializations, 349 live)
                           Storage by pipeline / step
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━┓
┃ Pipeline      ┃ Step           ┃       Size ┃ Objects ┃ Materializat… ┃ Live ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━┩
│ count-lines   │ (all steps)    │    1.2 KiB │      20 │            30 │   30 │
│               │ count_lines    │      130 B │       5 │             7 │    7 │
│               │ read_lines     │      620 B │       7 │            14 │   14 │
│ graphify      │ (all steps)    │  328.1 KiB │     101 │           157 │  157 │
│               │ ...
Objects are shared (content-addressed), so per-scope sizes can sum to more than
the total.
Reclaimable (dry-run — nothing is deleted): 0 objects / 0 B have zero live
references
```

`storage_report()` (importable from `rubedo.du`) is the library form; both
walk the **ledger**, never the object-store directory itself — the ledger
is always the source of truth for what the store contains, so the report
never has to guess at a file it can't explain. Key numbers:

- **`total_objects` / `total_bytes`** — distinct physical objects
  (deduplicated by content hash) and their combined size on disk.
- **Per-pipeline / per-step breakdown** — also deduplicated *within its own
  scope*. Because the store shares identical bytes across steps and even
  across pipelines, per-scope totals can legitimately **sum to more than
  the grand total** — that's the honest reading of shared storage, not a
  bug.
- **`missing_objects`** — a materialization's bytes are absent from disk
  and *not* explained by a logged reclamation: genuine corruption, reported
  separately so it's never confused with a deliberate GC deletion.
- **Reclaimable (dry-run only)** — objects that currently have **zero live
  references**: every materialization pointing at that content hash is
  non-live. This is exactly the ref-counting rule retention GC uses (see
  [Keep the store small](#keep-the-store-small) — `rubedo du` computes the same
  audit but never deletes anything. `--json` gives you the same numbers as
  structured output for scripts.

## Keep the store small

The object store keeps every generation **forever by default** — old bytes
are cheap insurance against re-running an LLM call. Retention is the
opt-in for when they stop being worth their storage. The full design
(rejected policies, demote/sweep, every trap) is in
[`../development/retention.md`](../development/retention.md).

With no `retention=` set, nothing is ever deleted. Once the store crosses
roughly 1 GiB, a run prints a one-line warning pointing here.

```python
pipeline(name="scrape", ..., retention=5)   # keep only the last 5 runs' outputs
```

Auto-prune runs at the end of every **successful** run: it keeps everything
the last `N` terminal runs referenced (`N >= 1`), demotes the rest, and
deletes bytes only when *no* live output anywhere still points at them.
It skips silently if another run's heartbeat is live.

```bash
rubedo gc                        # dry-run: exactly what --delete would prune
rubedo gc --max-bytes 2GiB --delete
```

`rubedo gc` with no flags deletes nothing. `--delete` refuses while any run
is live. Recovery is lazy: if a pruned input reappears, the next run
recomputes it onto the same content hash. Expand-cache anchors are always
kept — pruning one would re-pay the scrape the anchor exists to avoid.

Set `retention=` on pipelines whose old generations you'd never resurrect.
Leave it unset where recomputation would be expensive if a pruned input
came back.

## The run event log

Every run records an append-only stream of events — `run_started`,
`step_attempt_failed` (one per retried attempt, with `{"attempt", "max_attempts"}`),
`run_completed`/`run_failed`, plus retention events (`retention_pruned`,
`retention_skipped`, `storage_threshold_exceeded`) when applicable. Alongside
it, every (lane, step) cell gets a terminal `RunCoordinateStatus` row
(`created`/`reused`/`failed`/`blocked`/`filtered`, with the error type and
message for failures) — that per-coordinate table, not the event stream
itself, is what `rubedo show <run_id> --failed` reads to show you exactly
which coordinate failed at which step and why:

```bash
rubedo show run_39786b4eeef4 --failed
```

See [`../reference/cli.md`](../reference/cli.md#rubedo-show) for the full
`show` reference, and
[`../reference/api/index.md`](../reference/api/index.md) for `RunSummary.failures()`,
the library equivalent.

## The web dashboard (read-only UI)

```bash
rubedo serve                    # API + UI on http://127.0.0.1:8000
```

The dashboard is a browser over the same ledger everything else here reads
— runs, materializations, lineage, current outputs — with search to drill
into a specific value or error. The **UI never mutates state**: every write
path a user typically reaches for (`run`, `gc --delete`) is library code or
the CLI. The API beneath the dashboard is read-only except for one endpoint,
`POST /api/selection/invalidate`, which is unauthenticated and meant for
local use — treat `rubedo serve` as a local tool, not something to expose
publicly. The built UI is served from the package; to hack on the web UI
itself, `cd web && npm run dev` (Vite proxies `/api` to `:8000`).
`server.py` never imports your pipeline code — it only ever reads the ledger
and the `definition()` snapshot each run recorded.

## Run View

Open a run in the dashboard and the default tab is **Run View**: a
definition-driven layout of that run's cells, not a flat dump of every
coordinate. The same payload is available as
`GET /api/runs/{id}/view`.

Sections follow the pipeline shape:

- **Branch** — one table per expand root and its downstream map/expand
  chain (params band above the first branch when the run recorded any).
- **Join** — equijoin results as their own tables.
- **Child** — nested expand fan-out under a parent row (expand column
  always present so you can open the expand cell itself).
- **Summary** — aggregate chips (click to expand the aggregate payload).
- **Fold** — post-aggregate map steps as a normal table after the
  aggregate they depend on (`after <aggregate>`).

Cell tinting reflects created / reused / failed for the run; clicking a
cell lazy-loads the object preview. Run View is observability only — it
does not invalidate or re-run.
