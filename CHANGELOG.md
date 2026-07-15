# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.3] - 2026-07-15

### Added
- `rubedo serve` — one command starts the read-only FastAPI server with
  the built web UI served at `/` (SPA fallback for client-side routes).
  The web assets are bundled as package-data, so `pip install
  "rubedo[server]"` ships the dashboard. Vite builds to
  `src/rubedo/web_static/` and proxies `/api` to `:8000` in dev.
- `Pipeline.declare()` — writes a `kind="declaration"` Run with the full
  definition snapshot (including step source code) to the ledger without
  executing. The pipeline appears in the dashboard and `rubedo ls` before
  any run.
- Live run progress UI: per-step completion states (waiting/active/done)
  with progress bars and `finished/total` labels, animated topology on
  the Runs page. Live run cards expand/collapse and stay visible after
  completion (dismissible). The Runs page always polls (2s live, 5s idle)
  so new runs appear without a manual refresh.
- Clickable step detail panel in DagView: click any step node to see all
  specs (name, version, shape, depends_on, workers, retries, rate_limit,
  stale_after, executor, group_key, join_on, etc.) plus syntax-highlighted
  source code (open by default). A "View materializations →" link appears
  when a pipelineId is available.
- Click a pipeline name in the runs table to expand its DAG inline.
- `definition()` snapshot now includes a `source` field per step with the
  raw `inspect.getsource()` text.
- Playwright e2e specs (4 tests) spawning a backend with a temp
  `RUBEDO_HOME`, verifying the SPA renders real ledger data. Added to CI.
- `private/demo_live.py` — 7-step DAG with parallel branches and
  `stale_after="3s"` for observing live progress (`--force` and
  `--declare` flags).

### Changed
- SSE stream interval 1.0s → 0.3s for smoother live progress animation.
- `/api/pipelines` now includes `kind="declaration"` runs, not just
  `kind="process"`.
- `web/src/api.ts` uses relative `/api` URL (same-origin in prod, proxied
  by Vite in dev) instead of hardcoded `http://localhost:8000/api`.

### Fixed
- Playwright e2e: use `uv run python` in CI (bare `python` lacked venv
  dependencies like pydantic).

## [0.2.2] - 2026-07-14

### Changed
- `depends_on=` is now inferred for `reduce` and `join` steps too: a
  reduce step's parameter names its parent (like any map step), and a
  join's `join_on` keys ARE the parents. The parent-count validation for
  reduce moved from decoration time to build time so signature inference
  runs first. Explicit `depends_on=` still works and disables inference.
- `@p.step` (bare, no parens) now registers correctly — previously it
  silently did nothing (the decorator was returned uncalled).
- Swept all examples, docs, tests, and marketing to the terse step style:
  bare `@step`/`@p.step` with inference instead of explicit `name=`/
  `version=`/`shape=`/`depends_on=` that restate what the code already
  says.

### Added
- `test_depends_on_dict_alias_on_join` and
  `test_depends_on_dict_alias_on_reduce` — coverage for the `depends_on`
  dict alias form (`{"param": "step"}`) on join and reduce steps.

### Removed
- `docs/llms.txt` — stale duplicate of the canonical `notes/llms.txt`.

## [0.2.1] - 2026-07-13

### Changed
- The Pipeline rotation (TODO 15): one `Pipeline` object with verbs as
  methods (`.run()`/`.plan()`/`.describe()`/`.definition()`); `name` is
  the pipeline's sole identity (no `id=`); `pipeline()` is the sole
  constructor. `@p.step` registers steps on it; `pipeline(steps=[...])`
  takes an explicit list. `.build()` is gone — the spec is built lazily
  on first verb access.
- Step ergonomics (TODO 16): `@step` auto-names from the function name,
  defaults `version` to `"0"`, and works bare (`@step`) or called
  (`@step()`, `@step(version="2")`).
- Ingestion is a step, not a class (TODO 14): no `Source` protocol or
  `sources=` kwarg — a parentless generator `@step` infers `shape="expand"`
  and yields the initial lanes. A source-less `map` root mints a single
  `@root` lane from `params`.
- `describe(format="ascii")` — hand-rolled terminal DAG rendering; TTY
  autodetect picks ascii in a real terminal, text otherwise (TODO 20/24).
- Rewrote `notes/invariants.md` values-first (TODO 17); swept
  invariant-number references from docs/notes.
- Comment cleanup: process-notes out of source, constraints stay
  (TODO 19).
- Marketing landing page: spacing, syntax highlighting, hover tooltips,
  diamond-join rewrite.

### Added
- `pipeline(secrets=/env=)` declarations + `rubedo check` env lint
  (TODO 20/21).
- GitHub Pages workflow for the marketing site + docs.
- `StepSpec` is callable — `s(params)` runs a step in isolation for
  unit tests (TODO 24).

### Fixed
- `pipeline(retention=)` validated eagerly, not lazily.
- Marketing preview 404.

## [0.2.0] - 2026-07-12

### Added
- Retention GC (TODO 10b): `pipeline(retention=N)` auto-prunes a
  pipeline's last N terminal runs; `rubedo gc [--max-bytes] [--delete]`
  is a dry-run-by-default sweeper that demotes (paired `pruned`
  lifecycle rows) then deletes bytes only when no live materialization
  references them. `object_reclamations` table records every swept
  object.
- `schedule="broad"|"deep"` (TODO 9): broad completes each step across
  all lanes before the next; deep lets each lane race ahead through
  consecutive 1:1 map steps. Reduce/join/expand/multi-parent maps
  synchronize either way.
- Lane-level (downstream) invalidation — invalidating a lane
  propagates to its descendants.
- Source-less `map` root: a pipeline can begin with a plain step that
  mints a single `@root` lane from `params` instead of scanning for one.
- `examples/pdf_digest` — source-less map root feeding a vision→text DAG.

### Fixed
- `dist/*.gitignore` no longer leaks into GitHub release assets.

## [0.1.1] - 2026-07-09

### Added
- `trace()` / `rubedo trace` — lane-following lineage queries: seed on any
  selection and walk the recorded derivation edges upstream (what an output
  was derived from) and downstream (everything it contaminated), read-only;
  superseded generations are marked, never hidden.
- `storage_report()` / `rubedo du` — read-only storage observability:
  object-store size and live/reclaimable breakdown per pipeline and step,
  computed from the ledger, with a `--json` output for scripting.

## [0.1.0] - 2026-07-08

Initial public release.

### Added
- DAG pipelines over keyed collections — files in a folder, CSV rows, SQL
  table rows — with content-addressed caching: re-runs recompute only what
  changed (`hash(step, version, input_hash[, params][, code])`).
- Step shapes: `map` (default), `reduce` with optional `group_key`,
  `expand` (1:N lane minting), and N-way `join`; multi-source pipelines
  (`sources={name: Source}`).
- Step policies for flaky, expensive work: `retries`/`retry_on`,
  `rate_limit`, `stale_after` TTLs, data-quality `assertions`, cached
  `Filtered` verdicts, and `skip_cache` inline utils.
- Append-only run ledger with immutability guards, output generations
  (supersede/restore/refresh), lineage edges, and surgical invalidation via
  the `Selection` query language (`step:`, `version:<2.0`, indexed fields).
- Heartbeat-derived run liveness: stored status is terminal-only; readers
  derive `running`/`interrupted` from heartbeat freshness — a killed or
  slept run can never wedge as "running".
- Code-drift handling (`code="warn"|"auto"`), pipeline-level `params_model`
  validation, thread and process (`loky`) executors, terminal progress.
- Read-only ops CLI (`rubedo ls` / `show` / `invalidate`) and a read-only
  web dashboard (FastAPI + React) with live run streaming, lineage, and
  output search.
- MkDocs documentation, marketing site structure, community health files
  (issue/PR templates, CODEOWNERS), and the PyPI publishing workflow.
