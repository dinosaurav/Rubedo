# TODO

Living roadmap. Ordering within sections is rough priority; items marked
**[needs split]** depend on the plan/execute refactor of `run_pipeline`.

## Quick removals / cleanups

- [ ] Remove RunDiff (page, route, `/api/runs/{l}/diff/{r}` endpoint, `diffRuns` in api.ts)
- [ ] Remove `recompute()` from invalidation.py — trivial wrapper; users compose `invalidate()` + `run()`
- [ ] Keep hunting removal targets (candidates: `PipelineOut.step_name/code_version/workers` only describe the *first* step — misleading; fix or drop when the DAG view lands)
- [ ] Dashboard/API error state in the UI — a failing API currently looks identical to an empty database

## Engine core (ordered — each unlocks the ones below)

- [ ] **Plan/execute split of `run_pipeline`** — pure planning phase (scan → manifest →
      per (coordinate, step) decision with addresses) separated from execution.
      Prerequisite for step policies, filters, joins, explain, and multi-process safety.
- [ ] **Code-change detection** — hash step function source alongside the manual
      `version` string (or at least warn when source changed but version didn't).
      The silent-stale-cache trap bites hardest for the iterate-on-LLM-steps persona.
- [ ] Explain / dry-run — "what would this run do and why" (falls out of the plan phase;
      surface in UI and/or as `run(..., dry_run=True)`)
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
- [ ] Multithreaded/multiprocess execution — **needs clarification**: steps already run
      in a per-step thread pool (fine for I/O-bound LLM/HTTP work). If this means
      CPU-bound steps → ProcessPoolExecutor option per step; if it means cross-step
      pipelining → that's a scheduler change **[needs split]**

## UI / API

- [ ] Pipeline detail view — full DAG (steps, edges, versions, params schema),
      not just the first step; API must expose all steps
- [ ] View a run as a DAG — per-step status counts on the graph
- [ ] Selection language — string DSL (`step:count_lines coord:*.txt live:false`)
      parsing to Selection; usable in UI search box and Python

## Product / positioning

- [ ] Pick a real name (BatchIt/BatchBrain both placeholder-y; check PyPI availability)
- [ ] Interesting examples: LLM enrichment over a CSV, polite scraper with
      retries + staleness, sheet-combining (once fan-in exists)
- [ ] Seed/nucleus prompt to help LLMs generate example pipelines against the API
- [ ] README: sharpen the pitch — "dbt-style state for Python tasks; built for
      non-idempotent steps (LLMs, scraping)"; the generations/lifecycle model is
      the differentiator

## Done recently (context for the above)

- Source protocol (FolderSource, CsvSource), coordinates from anywhere
- Content-addressed store + generations; append-only lifecycle ledger with
  enforced immutability
- Params in cache identity; single `batchbrain.run()` entry point; processor
  vocabulary retired
