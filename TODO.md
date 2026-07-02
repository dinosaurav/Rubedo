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

- [ ] Retries — `retries=`, `retry_on=(exception types)`, backoff; attempts recorded
      as RunEvents, final outcome on RunCoordinateStatus. Default to retrying nothing
      (retrying a deterministic bug on an LLM step just multiplies cost).
- [ ] Rate limits — shared limiter across a step's workers (`rate_limit="10/min"`);
      per-step barrier execution makes this simple
- [ ] Staleness / TTL — outputs expire (`stale_after="24h"`); fits the lifecycle model:
      planning treats expired generations as non-live, recompute supersedes them.
      Natural for scraping/LLM outputs.
- [ ] `skip_cache` / ephemeral steps — cheap steps recomputed inline instead of
      materialized. Design questions: lineage edges across the skipped hop; what
      blocked-propagation means for a step with no materialization.
- [ ] Arbitrary step rules — once 2–3 concrete policies exist, extract the plugin
      surface from them (don't design the abstraction first)

## Data shape: filters, joins, fan-in **[needs split]**

- [ ] Filters — a step can decline a coordinate (return sentinel / `filter=` predicate);
      downstream steps skip it; new ledger status (`filtered`). The 1→0/1 warm-up
      for coordinate transformation.
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
