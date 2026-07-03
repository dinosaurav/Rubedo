# TODO

Living roadmap. Ordering within sections is rough priority; items marked
**[needs split]** depend on the plan/execute refactor of `run_pipeline`.

## Quick removals / cleanups

- [x] Remove RunDiff (page, route, `/api/runs/{l}/diff/{r}` endpoint, `diffRuns` in api.ts)
- [x] Remove `recompute()` from invalidation.py — trivial wrapper; users compose `invalidate()` + `run()`
- [ ] Keep hunting removal targets (candidates: `PipelineOut.step_name/code_version/workers` only describe the *first* step — misleading; fix or drop when the DAG view lands)
- [ ] Dashboard/API error state in the UI — a failing API currently looks identical to an empty database

## Engine core (ordered — each unlocks the ones below)

- [x] **Plan/execute split of `run_pipeline`** — `_plan_step` (read-only StepDecision
      per coordinate) / `_record_planned` / `_execute_step` / `_commit_execution_result`,
      orchestrated by a slim `run_pipeline`.
- [x] **Code-change detection** — orthogonal axes: `version` (semantic label) and
      `code="auto"|"warn"` (source hash joins identity vs. drift warnings via
      UserWarning + `code_drift_detected` event + `RunPlan.warnings`). Caveat:
      hashes the step function's own source only — helper edits are what the
      version bump is for. Possible middle ground later: hash the step's whole
      defining module (coarse but catches same-file helpers); full closure
      hashing deliberately rejected (dynamic dispatch + dependency upgrades
      make it unsound at real complexity cost).
- [ ] Semantic version ordering — parse `version` with `packaging` (PEP 440)
      wherever ordering matters: version-range selection ("invalidate everything
      computed by < 2.0") and UI sorting. Policy: no validation at registration;
      parseable versions order properly, unparseable ones ("read-v1") stay opaque
      labels — equality only, range operations skip them. Frontend gets a small
      semver-aware comparator for table sorting. Build alongside the selection
      language, which is its main consumer.
- [x] Explain / dry-run — `batchbrain.plan(pipeline, params=...)` returns a RunPlan
      (reuse / execute / pending / removed per coordinate-step, with addresses);
      still to do: surface it in the UI
- [ ] Cross-process concurrency safety — two simultaneous runs can race the
      liveness check-then-insert; commit should be one guarded transaction **[needs split]**
- [ ] Enforce the pairing rule mechanically (every `is_live` flip must ship a
      lifecycle row in the same flush) — session-level guard upgrade

## Step policies (`@step(...)`) **[needs split]**

- [x] Retries — `retries=`, `retry_on=`, `retry_delay=`/`retry_backoff=`; attempts
      recorded as `step_attempt_failed` events, attempt count on the coordinate
      status metadata
- [x] Rate limits — `rate_limit="10/min"`: even pacing shared across a step's
      workers, retries included
- [x] Staleness / TTL — `stale_after="24h"`: planning treats expired outputs as
      cache misses; recompute with different bytes supersedes, identical bytes
      append a `refreshed` lifecycle row and reset the `refreshed_at` projection.
- [x] `skip_cache` — inline utils fused into consumers' cache identity; lazy
      execution memoized per run, lineage skips through to nearest materialized
      ancestors, blocked/failed propagate with the util's failure surfacing on
      its consumer. Intended for quick idempotent helpers only (docs say so).
- [x] ~~Arbitrary step rules~~ — resolved: no plugin surface. Rule of thumb:
      execution-only concerns are user-wrappable (a step is just a function);
      anything touching identity/plan/ledger earns a deliberate built-in.
      Revisit only if a concrete third-party need appears.

## Data shape: filters, joins, fan-in **[needs split]**

- [x] Filters — a step declines a coordinate by returning `Filtered(reason)`;
      the verdict is a cached materialization (filtered flag), downstream gets
      status `filtered`, and content changes re-decide. Note: filtered
      coordinates drop out of /api/current-outputs (status filter) — revisit
      whether the UI should show them explicitly.
- [ ] Fan-in / reduce — one output from all coordinates of an upstream step
      ("combine the sheets"); input_hash = manifest-level hash of upstream outputs
- [ ] **Joins** — the big one. Materialize rows from two sources, join into pair
      coordinates, fan out again for per-pair steps. Implies: multi-source pipelines
      (today a pipeline has exactly one source), coordinate-*creating* steps, pair
      coordinate naming (`left|right` — composite-key convention already exists in
      CsvSource). Do filters and fan-in first; a join is a product + fan-out composed.

## Sources

- [ ] TableSource — rows of a SQL table (coordinate = primary key); the "crucially
      rows in a table" case. `updated_at`-based incremental scan as a later optimization.
- [ ] CPU-bound parallelism — per-step ProcessPoolExecutor option
      (`@step(executor="process")`). Threads (current) only parallelize I/O-bound
      work because of the GIL; multiprocessing gives true parallelism for
      compute-heavy steps. Constraint to design around: process pools pickle the
      step function and its arguments, so process-executor steps must be
      module-level functions (no closures) with picklable payloads/params.

## UI / API

- [ ] Pipeline detail view — full DAG (steps, edges, versions, params schema),
      not just the first step; API must expose all steps
- [ ] View a run as a DAG — per-step status counts on the graph
- [ ] Selection language — string DSL (`step:count_lines coord:*.txt live:false`)
      parsing to Selection; usable in UI search box and Python

## Product / positioning

- [ ] Pick a real name — brainstorm + PyPI availability check (parked; not the
      current priority)
- [ ] Interesting examples: LLM enrichment over a CSV, polite scraper with
      retries + staleness, sheet-combining (once fan-in exists)
- [ ] LLM seed prompt for generating examples — a concise API-teaching doc
      (llms.txt-style: concepts, step contract, cache identity rules, a worked
      example) so a model can generate correct pipelines on request
- [ ] README: sharpen the pitch — "dbt-style state for Python tasks; built for
      non-idempotent steps (LLMs, scraping)"; the generations/lifecycle model is
      the differentiator

## Done recently (context for the above)

- Source protocol (FolderSource, CsvSource), coordinates from anywhere
- Content-addressed store + generations; append-only lifecycle ledger with
  enforced immutability
- Params in cache identity; single `batchbrain.run()` entry point; processor
  vocabulary retired
