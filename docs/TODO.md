# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine
contradictions), with file pointers, gotchas, and acceptance criteria.
Read `CLAUDE.md` first for conventions, and `docs/invariants.md` for
vocabulary. One item = one (or a few) commits.

──────────────────────────────────────────────────────────────────────

## 0. CPU-bound parallelism — `@step(executor="process")`

**Decisions (Currently Implemented):**
- `StepSpec.executor: str = "thread"` (`thread` | `process`).
- Registration-time validation for `process`: the fn must be picklable —
  reject when `"<locals>" in fn.__qualname__` (closures/test-local defs)
  with a clear message ("process-executor steps must be module-level").
- **Parent-side orchestration, child runs the bare fn**: in
  `execution._execute_step`, when `executor == "process"`, build
  args/kwargs in the parent (so `_resolve_parent_value` / ephemeral
  resolution and `_build_step_params` stay parent-side; the resulting
  values must pickle — document this), then submit `step.fn` itself to a
  `ProcessPoolExecutor`. Retries and the rate limiter stay in the parent:
  acquire the limiter before each submit; on failure, resubmit per the
  retry policy. The existing `process()` closure logic can be reshaped so
  the attempt loop wraps "submit + future.result()" for the process case
  and the direct call for the thread case.
- One pool per step execution (same lifecycle as the current
  ThreadPoolExecutor), `max_workers=workers or step.workers`.
- `Filtered`/`ProcessResult` returns work unchanged (they pickle).

**The Overcomplication:**
The `executor="process"` option adds a layer of complexity because it forces the orchestrator (`execution._execute_step`) to manage two different concurrency primitives (`ThreadPoolExecutor` and `ProcessPoolExecutor`). This means we have to maintain a delicate balance where retries and rate limits run in the parent thread while the actual execution happens in the child process. It also imposes strict pickling constraints on step arguments and functions (no closures), which can confuse users when their data or inner functions fail to serialize.

**Possible Fix:**
Remove the `executor="process"` option from the engine entirely and default strictly to thread-based concurrency. If users have heavy CPU-bound tasks, they can implement multiprocessing *inside* their own step functions, thereby delegating the CPU-bound complexity to the user's code rather than baking it into the engine's orchestration layer.

**Status:** implemented and tested (`tests/test_process_executor.py`), but
the overcomplication note above is a live design question, not yet
settled — bring a decision (keep vs. remove) before touching this again.

──────────────────────────────────────────────────────────────────────

## 1. Codebase Housekeeping & Typing Improvements

**Goal:** Improve developer experience and code maintainability by expanding explicit type hints across the codebase.

**Decisions:**
- **Type Hints**: Expand type hinting for `RunMemo._values`, `Producer` Callables, and parameters in `execution.py`. (Not yet done — `RunMemo._values` is still `Dict[Any, Any]`.)
- **API Typing**: In `server.py`, some endpoints like `get_object_metadata` and `download_object` return untyped dictionaries or direct `FileResponse` objects. Add explicit Pydantic schemas (e.g. `ObjectMetadataOut`) to these to improve the API documentation.
- **`typing.Any` reduction**: Where possible, replace `Any` with specific generics or unions, particularly around serialized output data in `store.py`.

──────────────────────────────────────────────────────────────────────

## 2. Examples + LLM seed prompt (positioning)

- `examples/llm_enrich.py`: CsvSource over a small checked-in CSV,
  `screen` (Filtered) → `enrich` (fake-LLM function — deterministic
  stand-in with a comment showing where a real client call goes;
  `retries=3, retry_on=..., rate_limit="30/min"`, `index=["company"]`) →
  reduce summary (item 1's reduce steps have shipped — wire this in).
  Heavy comments; this is the flagship example.
- `examples/scraper.py`: FolderSource of URL-list files or CsvSource of
  URLs; fetch step with `stale_after="24h"`, retries, rate_limit;
  fake fetcher (no network in examples).
- `docs/llms.txt`: **done** — compact API-teaching doc for LLMs already
  exists; keep it in sync with README as the API evolves.
- README pitch paragraph: **done** — leads with "dbt-style state for
  Python tasks, built for non-idempotent steps (LLMs, scraping)".

──────────────────────────────────────────────────────────────────────

## 3. Joins  **[DO NOT BUILD without a design session with the owner]**

Direction (not yet settled enough to build): a join creates pair lanes
from two parents (`left|right`), which requires coordinate-*creating*
steps (shape="expand") and multi-root pipelines. The conceptual
foundation (lane keys vs identity vs search) is settled and reduce
builds half the machinery. Open questions needing the owner: pair
explosion control (predicates before materialization?), expand-step
manifest caching, multi-source pipeline API. Bring a proposal first.

## 4. Naming  **[parked by owner]**

Brainstorm + PyPI availability check when asked. Not the current priority.

──────────────────────────────────────────────────────────────────────

## 5. Future Product Directions (Recommended Next Steps)

These are strategic feature recommendations to expand the engine's capabilities for real-world, large-scale workflows:

- **Cloud Object Storage Sources (`S3Source` / `GCSSource`)**: Local folders and SQL are great starts, but modern data engineering lives in cloud buckets. Adding native sources for scanning and pulling from S3 or GCS is critical for adoption.
- **Incremental Source Scanning (High Watermarks)**: Currently, sources scan their entire domain on every run (relying on cache identity to skip work). For massive tables or buckets, a source should support an `updated_at > last_run` watermark to skip scanning untouched coordinates entirely, drastically speeding up the planning phase.
- **Dynamic Lane Expansion (`flat_map` shape)**: Currently we have `map` (1:1) and `reduce` (N:1). Adding an `expand` or `flat_map` shape (1:N) would allow a step to `yield` multiple outputs from a single lane (e.g., fetching an RSS feed and yielding an output lane for each article).
- **Robust CLI & Terminal UI**: The Web UI is excellent, but local-first developers love the terminal. A `rubedo` CLI with rich terminal output (using a library like `rich`) to show live DAG execution, progress bars for lanes, and interactive plan confirmations would greatly enhance the core DX.
- **Data Quality Assertions**: Similar to dbt tests, allowing users to define lightweight assertions or schemas on step outputs to automatically fail/block lanes if the data is malformed (e.g., an LLM returns invalid JSON that parses but misses required fields).

──────────────────────────────────────────────────────────────────────

## Done (compressed changelog — context for the above)

Source protocol (Folder/Csv, lane-key semantics, duplicate handling) ·
content-addressed store + generations (supersede/restore/refresh) ·
append-only lifecycle ledger with ORM immutability guards · params/code in
cache identity (`code="auto"|"warn"` drift warnings) · single
`run()`/`plan()` entry points, no registry, definition snapshots on runs ·
plan/execute/ledger module split · step policies: retries, rate_limit,
stale_after, skip_cache (fusion) · filters (`Filtered` verdicts, cached) ·
`@step(index=[...])` + `Selection(index=...)` + selection language
(`Selection.parse`, `{"query": ...}` API, UI query box) · DAG rendering
(describe/Mermaid + DagView on Pipelines/RunDetail with per-step counts) ·
trim pass removing v1 residue (`config=`/`config_hash`,
`Selection.coordinates`/`output_content_hash`, `Manifest.manifest_hash`,
manifest size/mtime columns, `previous_output_address`/
`previous_materialization_id`, `SelectionPreviewItem.coordinate`/
`coordinate_count`) and the metadata-filter query path (storage/display
kept, only filtering removed) · Dashboard page and redundant examples
removed · fan-in/reduce steps (`shape="reduce"`, full N→1 fan-in,
`tests/test_reduce.py`) · `TableSource` (SQL rows as lanes,
credential-free `source_id`, `tests/test_table_source.py`) ·
cross-process concurrency safety (SQLite WAL + busy_timeout,
IntegrityError retry-once on commit collision,
`tests/test_concurrency_safety.py`) · pairing-rule guard mechanically
enforcing invariant 8 (`before_commit` session listener,
`tests/test_pairing_guard.py`) · semantic version ordering + range
selection (`version:<2.0` etc. via `packaging.SpecifierSet`, version-aware
sort in DataTable.tsx) · UI polish cluster (API error states via
`fetchJson`, filtered lanes shown in Current Outputs, reduce badge in
DagView) ·
resolved-won't-do: arbitrary-rules plugin surface (wrapper-or-built-in
rule); plan()-in-UI (server never imports user code — use plan() in
Python).
