# TODO

Each open item below is written as a self-contained spec: the design
decisions are already made (do not re-litigate; flag genuine
contradictions), with file pointers, gotchas, and acceptance criteria.
Read `CLAUDE.md` first for conventions, and `docs/invariants.md` for
vocabulary. One item = one (or a few) commits.

──────────────────────────────────────────────────────────────────────



## 1. Joins  **[DO NOT BUILD without a design session with the owner]**

Direction (not yet settled enough to build): a join creates pair lanes
from two parents (`left|right`), which requires coordinate-*creating*
steps (shape="expand") and multi-root pipelines. The conceptual
foundation (lane keys vs identity vs search) is settled and reduce
builds half the machinery. Open questions needing the owner: pair
explosion control (predicates before materialization?), expand-step
manifest caching, multi-source pipeline API. Bring a proposal first.

──────────────────────────────────────────────────────────────────────

## 2. Future Product Directions (Recommended Next Steps)

These are strategic feature recommendations to expand the engine's capabilities for real-world, large-scale workflows:

- **Cloud Object Storage Sources (`S3Source` / `GCSSource`)**: Local folders and SQL are great starts, but modern data engineering lives in cloud buckets. Adding native sources for scanning and pulling from S3 or GCS is critical for adoption.
- **Configurable cloud ledger + object store (Postgres / S3-GCS)**: distinct from the Source item above, which is about *input* data — this is about the *internal* materialization store (`store.py`) and ledger DB (`db.py`) that back every run. `db.py` is already SQLAlchemy-based, so pointing it at a Postgres URL is comparatively mechanical (the WAL/busy_timeout pragma hook is SQLite-specific and would need to become conditional); `store.py`'s content-addressed layout (`hash[:2]/hash[2:4]/hash`) maps directly onto an S3 key prefix, but every `os.path`/`open()`/`os.replace()` call in it assumes a local filesystem and would need an abstraction swapped in behind the same interface. This — not the execution backend — is the real prerequisite for genuine multi-machine/cloud execution (see the executor="process" and Dask discussion in item 0's history); the configurable `RUBEDO_HOME` root (now shipped — see Done below) is a natural stepping stone since it already isolates where these paths get resolved.
- **Pluggable distributed execution backend (Dask / Ray / cloud)**: today `execution.py` only offers `executor="thread"|"process"`, both single-machine. `execution._execute_step`'s `call()` already treats "the pool" as anything satisfying `.submit(fn, *args, **kwargs) -> Future-with-.result()` (see `pool.submit(step.fn, *args, **kwargs).result()`), which is the same shape `dask.distributed.Client` and `ray` (via a thin wrapper) expose — so a third `executor="dask"`/`executor="ray"` value is a comparatively small change to *this specific call site*. The real cost is architectural, not mechanical: it requires a running scheduler/cluster, which cuts directly against this project's "zero-daemon" positioning (`docs/framework_analysis.md`), and it depends on the cloud ledger/object-store item above (a distributed worker can't write to a purely local SQLite file + local objects dir). Needs an owner design session before building: whether to add this as a third `executor=` value alongside `"process"`, or have it *replace* `"process"` outright (a Dask/Ray `LocalCluster` subsumes the same local-multi-process case — see item 0's loky/cloudpickle note for a lower-cost alternative that solves the picklability pain without any of this).
- **Incremental Source Scanning (High Watermarks)**: Currently, sources scan their entire domain on every run (relying on cache identity to skip work). For massive tables or buckets, a source should support an `updated_at > last_run` watermark to skip scanning untouched coordinates entirely, drastically speeding up the planning phase.
- **Dynamic Lane Expansion (`flat_map` shape)**: Currently we have `map` (1:1) and `reduce` (N:1). Adding an `expand` or `flat_map` shape (1:N) would allow a step to `yield` multiple outputs from a single lane (e.g., fetching an RSS feed and yielding an output lane for each article).
- **Robust CLI & Terminal UI**: The Web UI is excellent, but local-first developers love the terminal. A `rubedo` CLI with rich terminal output (using a library like `rich`) to show live DAG execution, progress bars for lanes, and interactive plan confirmations would greatly enhance the core DX.
- **Data Quality Assertions**: Similar to dbt tests, allowing users to define lightweight assertions or schemas on step outputs to automatically fail/block lanes if the data is malformed (e.g., an LLM returns invalid JSON that parses but misses required fields).
- **Storage Sprawl Management**: Include useful features to prevent storage sprawl, such as disk usage warnings, storage limits, automated policies to reduce/expire old data, and mark-and-sweep garbage collection to clean up unlinked or orphaned data.

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
DagView) · examples + positioning (`hn_digest` — real HN + LLM
filter→classify→reduce, the flagship non-idempotent-LLM demo;
`github_health`/`weather_advisory` — chained retried/rate-limited APIs
with `stale_after`; `gutenberg_stats` — `skip_cache` util +
`executor="process"`; `orders_rollup` — `TableSource` streaming
`batch_size`; `docs/llms.txt` LLM-authoring guide; README pitch
paragraph) · project rename (Batchit/batchbrain -> Rubedo) · configurable
`RUBEDO_HOME` root (env var, resolved by both `db.py` and `store.py`;
explicit `home=` param on `run()`/`plan()` takes precedence over env vars,
same precedence `db.py`'s `db_path` param already had; `RUBEDO_DB_PATH`
still wins over `RUBEDO_HOME` for the DB specifically when no explicit
param is given; `server.py` needed no code changes — it already picks up
the same env var transitively) · codebase typing pass (`_RunMemo._values`
typed as `Dict[Tuple[str, str], Tuple[Literal["ok", "err"], Any]]`;
`store.py`'s `read_materialization_output` param was fully unannotated,
now a `HasOutputContentHash` Protocol satisfied structurally by both
`Materialization` and `MatRef`; `ObjectMetadataOut`/
`MaterializationIndexEntryOut` schemas replace `get_object_metadata`'s
untyped dict return, `download_object` got an explicit `-> FileResponse`
return type; `_serialize`/`stage_and_commit`'s `result: Any` stayed `Any`
deliberately — step return values are genuinely heterogeneous, no
narrower type is honest there) · CPU-bound parallelism migrated to `loky` +
`cloudpickle` (`executor="process"`), allowing closures in process-executed
steps (`tests/test_process_executor.py` updated to verify local functions) ·
resolved-won't-do: arbitrary-rules plugin surface (wrapper-or-built-in
rule); plan()-in-UI (server never imports user code — use plan() in
Python).
