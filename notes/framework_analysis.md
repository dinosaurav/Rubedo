# Framework Analysis: Rubedo

**Date**: 2026-07-05
**Author**: Claude Opus 4.8
**Revision note**: Updated from the original 2026-07-04 analysis (Gemini 3.1 Pro) to reflect the shipped **producer model** — content-addressed lanes, `expand` (fan-out), `group_key` reduce, multi-source pipelines, and N-way `join`.

## What This Project Does
This project is a **local-first batch processing engine** designed specifically for **Python tasks** that are non-idempotent, expensive, or flaky—such as LLM calls, web scraping, or heavy API integrations.

It brings a "dbt-style state" to Python, meaning it treats data pipelines as DAGs over collections (files, CSV rows) and leverages **content-addressed caching** and an **append-only ledger**. Instead of re-running the entire pipeline, it surgically computes only what has changed based on cryptographic hashes of the inputs, code, and parameters.

Pipelines are not just 1:1 chains: steps can fan in (`reduce`, with per-group `group_key`), fan out (`expand` — one feed into a lane per article, cached so a scrape runs once), and **join** across multiple sources on an indexed key — a relational vocabulary applied to arbitrary Python objects, all with the same per-lane content-addressed caching.

### Core Principles & Unique Edges
1. **Granular, Content-Addressed Caching**: Caches at the coordinate level (e.g., a specific row in a CSV or a specific file). If you edit one row, only the downstream tasks for *that specific row* are recomputed.
2. **Immutable Ledger & Surgical Invalidation**: Outputs are immutable bytes in an object store. A SQLite ledger tracks the lifecycle (created, reused, superseded, invalidated). You can invalidate outputs based on the *content* they generated (e.g., `invalidate(company:acme)`), not just by timestamp.
3. **Resilience for the Real World**: Built-in policies for `retry_on`, `rate_limit`, and `stale_after` (TTL). This makes it perfect for wrangling rate-limited or flaky LLM APIs.
4. **Zero-Magic, Zero-Daemon Architecture**: There is no central server to deploy or daemon to run. The pipeline is just a Python object, executed locally, with state stored in a local `.rubedo` directory.
5. **Relational Shapes over Arbitrary Python**: Beyond 1:1 `map` steps, the engine offers `reduce`/`group_key` (fan-in), `expand` (1:N fan-out — e.g. a feed into a lane per article, cached so a scrape runs once), and N-way `join` (equijoin across sources on an indexed field). This is a relational vocabulary usually reserved for SQL engines, here applied to arbitrary Python objects and unstructured data — with per-lane content-addressed caching underneath every shape.

## Competitor Landscape

### vs. Dagster
- **Dagster's Edge**: Enterprise-grade observability, software-defined assets, and rich integrations.
- **Rubedo's Edge**: Lightweight and local-first. Dagster requires spinning up a web server/daemon even for local development. Rubedo is purely library-driven with an optional UI. Rubedo also provides much finer granular row-level caching out of the box compared to Dagster's partition-level assets.

### vs. Prefect
- **Prefect's Edge**: Hybrid execution (local workers + cloud orchestration) and fantastic error handling observability.
- **Rubedo's Edge**: True content-addressed caching. Prefect's caching is typically time-based or explicitly key-based, whereas Rubedo automatically hashes inputs, parameters, and even code (with `code="auto"`) to guarantee exact state reproducibility.

### vs. Metaflow (Netflix)
- **Metaflow's Edge**: Seamless scaling to the cloud (AWS Batch/Kubernetes) and snapshotting the entire Python environment/state to S3.
- **Rubedo's Edge**: Row-level surgical execution. Metaflow typically runs or resumes an entire step for a batch. Rubedo processes item by item, seamlessly reusing successful API calls from a previous interrupted run.

### vs. dbt
- **dbt's Edge**: The industry standard for SQL transformations in the data warehouse.
- **Rubedo's Edge**: Rubedo is essentially **"dbt for arbitrary Python"**. Where dbt manages incremental state for SQL tables, Rubedo manages incremental state for arbitrary Python objects and unstructured data (files/APIs). The producer model tightens the analogy: Rubedo now has the relational shapes dbt users expect — `join` across sources and `group_key` fan-in — but over Python objects instead of warehouse tables, and with row-level caching that dbt's table-grained (or partition-grained) incrementality can't match.

### vs. Hamilton
- **Hamilton's Edge**: Pure functional DAGs for data engineering, heavily focused on pandas/dataframe transformations.
- **Rubedo's Edge**: Focused on side-effects and flaky tasks (retries, rate limiting, TTLs) rather than pure dataframe math, backed by a persistent ledger.

## Strengths & Weaknesses

**Strengths:**
- **Unmatched for iterative local development** with LLMs or scrapers (never pay for the same LLM API call twice).
- **Relational + fan-out over unstructured data**: N-way `join`, per-group `reduce`, and feed/scrape fan-out (`expand`) with per-lane caching — a combination SQL engines and Python DAG tools rarely offer together.
- **Incredible auditability** via the append-only ledger and materialization lineage.
- **Zero-friction setup** (just run the Python script).
- **Surgical invalidation** (re-run a specific bad generation without tossing the baby out with the bathwater).

**Weaknesses:**
- **Local-Bound Execution**: Not designed (yet) for distributed compute across thousands of nodes (a cloud ledger/store + a Dask/Ray backend are on the roadmap — `notes/TODO.md` items 9–10).
- **Storage Heavy**: Content-addressed object stores keep everything, and `expand` currently double-stores its items pending the child-views optimization (TODO 13). Without garbage collection — which is deliberately unbuilt and flagged dangerous (TODO 12) — large outputs balloon the `.rubedo` directory.
- **Not for Real-Time Streaming**: Strictly a batch engine, and execution is *staged* (whole-step barriers); lane-pipelined execution that lets a lane race ahead of its siblings is a roadmap item (TODO 11), not reality yet.
- **Tight Python Coupling**: Not polyglot like some orchestration tools.
