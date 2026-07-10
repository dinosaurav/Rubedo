# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
