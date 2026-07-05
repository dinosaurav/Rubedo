# Framework Analysis: Batchit

**Date**: 2026-07-04
**Author**: Gemini 3.1 Pro

## What This Project Does
This project is a **local-first batch processing engine** designed specifically for **Python tasks** that are non-idempotent, expensive, or flaky—such as LLM calls, web scraping, or heavy API integrations.

It brings a "dbt-style state" to Python, meaning it treats data pipelines as DAGs over collections (files, CSV rows) and leverages **content-addressed caching** and an **append-only ledger**. Instead of re-running the entire pipeline, it surgically computes only what has changed based on cryptographic hashes of the inputs, code, and parameters.

### Core Principles & Unique Edges
1. **Granular, Content-Addressed Caching**: Caches at the coordinate level (e.g., a specific row in a CSV or a specific file). If you edit one row, only the downstream tasks for *that specific row* are recomputed.
2. **Immutable Ledger & Surgical Invalidation**: Outputs are immutable bytes in an object store. A SQLite ledger tracks the lifecycle (created, reused, superseded, invalidated). You can invalidate outputs based on the *content* they generated (e.g., `invalidate(company:acme)`), not just by timestamp.
3. **Resilience for the Real World**: Built-in policies for `retry_on`, `rate_limit`, and `stale_after` (TTL). This makes it perfect for wrangling rate-limited or flaky LLM APIs.
4. **Zero-Magic, Zero-Daemon Architecture**: There is no central server to deploy or daemon to run. The pipeline is just a Python object, executed locally, with state stored in a local `.batchbrain` directory.

## Competitor Landscape

### vs. Dagster
- **Dagster's Edge**: Enterprise-grade observability, software-defined assets, and rich integrations.
- **Batchit's Edge**: Lightweight and local-first. Dagster requires spinning up a web server/daemon even for local development. Batchit is purely library-driven with an optional UI. Batchit also provides much finer granular row-level caching out of the box compared to Dagster's partition-level assets.

### vs. Prefect
- **Prefect's Edge**: Hybrid execution (local workers + cloud orchestration) and fantastic error handling observability.
- **Batchit's Edge**: True content-addressed caching. Prefect's caching is typically time-based or explicitly key-based, whereas Batchit automatically hashes inputs, parameters, and even code (with `code="auto"`) to guarantee exact state reproducibility.

### vs. Metaflow (Netflix)
- **Metaflow's Edge**: Seamless scaling to the cloud (AWS Batch/Kubernetes) and snapshotting the entire Python environment/state to S3.
- **Batchit's Edge**: Row-level surgical execution. Metaflow typically runs or resumes an entire step for a batch. Batchit processes item by item, seamlessly reusing successful API calls from a previous interrupted run.

### vs. dbt
- **dbt's Edge**: The industry standard for SQL transformations in the data warehouse.
- **Batchit's Edge**: Batchit is essentially **"dbt for arbitrary Python"**. Where dbt manages incremental state for SQL tables, Batchit manages incremental state for arbitrary Python objects and unstructured data (files/APIs).

### vs. Hamilton
- **Hamilton's Edge**: Pure functional DAGs for data engineering, heavily focused on pandas/dataframe transformations.
- **Batchit's Edge**: Focused on side-effects and flaky tasks (retries, rate limiting, TTLs) rather than pure dataframe math, backed by a persistent ledger.

## Strengths & Weaknesses

**Strengths:**
- **Unmatched for iterative local development** with LLMs or scrapers (never pay for the same LLM API call twice).
- **Incredible auditability** via the append-only ledger and materialization lineage.
- **Zero-friction setup** (just run the Python script).
- **Surgical invalidation** (re-run a specific bad generation without tossing the baby out with the bathwater).

**Weaknesses:**
- **Local-Bound Execution**: Not designed (yet) for distributed compute across thousands of nodes.
- **Storage Heavy**: Content-addressed object stores keep everything. Without garbage collection, large outputs will balloon the `.batchbrain` directory.
- **Not for Real-Time Streaming**: Strictly a batch engine.
- **Tight Python Coupling**: Not polyglot like some orchestration tools.
