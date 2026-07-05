# Rubedo / Rubedo Project Documentation

This document provides a detailed overview of the Rubedo (also internally referred to as `rubedo`) project. It outlines the current capabilities of the system, its architecture, and how each folder contributes to its overall functionality.

## Project Overview & Capabilities

Rubedo is a batch processing framework designed to efficiently run pipelines over collections of coordinates — files in a folder, rows in a CSV. It is composed of a Python-based backend processing engine and a React-based frontend web application.

### Key Capabilities:
1. **DAG Pipeline Framework:** Developers can define multi-step Directed Acyclic Graph (DAG) pipelines using `@step` and `pipeline()` decorators. Steps can declare explicit dependencies on upstream steps (`map` shape, 1:1 per lane) or fan in with `shape="reduce"` (N:1 over all surviving lanes of its parents), allowing outputs to flow downstream seamlessly.
2. **Pluggable Coordinate Sources:** A `Source` abstraction enumerates coordinates ("lanes") and loads their payloads. `FolderSource` (files, coordinate = relative path), `CsvSource` (rows, coordinate = key column), and `TableSource` (SQL rows, coordinate = key column, optional `batch_size` streaming mode) ship built in; `folder=` on `pipeline()` is sugar for `FolderSource`.
3. **Durable Materialization & Invalidation:** The engine treats processing as a durable, content-addressed materialization pipeline. It uses point-in-time `Manifest` snapshots to robustly track the state of input sources and skips recomputation if the input hash, `params`, and code identity (`version`, and source hash when `code="auto"`) haven't changed.
4. **Durable Run Ledger & Lineage:** The system records exhaustive telemetry for each run. It creates a robust event ledger (`RunEvent`) and computes per-coordinate status summaries (`RunCoordinateStatus`) categorizing work as `created`, `reused`, `failed`, `blocked`, `removed`, or `filtered` (a step declined the lane via `Filtered(...)` — a cached, first-class verdict). `MaterializationEdge` models persist parent-child lineage.
5. **Step Policies for Flaky/Non-Idempotent Work:** `retries`/`retry_on`/`retry_delay`/`retry_backoff`, `rate_limit` (paced across all workers), `stale_after` (TTL-based recompute), and `skip_cache` (an in-memory, never-materialized util whose identity fuses into its consumers' cache keys) — built for LLM calls and scraping, not just deterministic transforms.
6. **Concurrency & Execution Engine:** A multi-worker execution runner topologically sorts steps and processes lanes in parallel, correctly blocking on failed upstream dependencies. Steps can opt into `executor="process"` for CPU-bound work (a `ProcessPoolExecutor` per step; the function must be module-level/picklable).
7. **Search:** two independent channels — lane-key globs (`coordinate_glob`, source-shaped) and `@step(index=[...])` fields extracted from output values at commit time (content-shaped), both usable via the `Selection` query language (`Selection.parse("step:extract company:acme live:true")`).
8. **Database Storage:** Results, metadata, runs, run ledgers, topological lineage, and caching statuses are tracked in a SQL database (via SQLAlchemy), with content-addressed bytes in a local object store (`.rubedo/objects`).
9. **API Server:** A read-only FastAPI server exposing pipelines, runs, materializations, lineage, current outputs, and selection previews to the frontend, plus selection-based invalidation as its single write action. It never imports user pipeline code.
10. **Web UI:** A React + Vite dashboard for browsing runs, outputs, and lineage, and surgically invalidating outputs via the selection language. Runs themselves are triggered from library code (`rubedo.run`), not the UI.

## Folder Structure

Below is the breakdown of the top-level directories and critical files, and how they contribute to the project.

### `/rubedo/`
This is the core Python backend package containing the execution engine, database logic, and API. There is no CLI (see `docs/TODO.md` item 4 for that as a future direction) — pipelines run from library code (`rubedo.run`/`rubedo.plan`).

- **Definitions (`spec.py`)**: `@step` and `pipeline()` build plain `StepSpec`/`PipelineSpec` objects (no registry, no magic module loading); `describe()` renders a DAG as text or Mermaid before it ever runs.
- **Run Phases (`planning.py`, `execution.py`, `ledger.py`, `runner.py`)**: read-only planning (decisions, addresses, staleness/drift), DB-free execution (thread/process pool, retries, rate limits, ephemeral utils), all ledger writes (manifests, statuses, events, generations), and the `run()`/`plan()` orchestrators. Each run snapshots its pipeline definition into the ledger.
- **Sources & Invalidation (`sources.py`, `hashing.py`, `invalidation.py`)**: The `Source` protocol with `FolderSource`/`CsvSource`/`TableSource`, content hashing, and logic for determining if a coordinate's cached result is still valid.
- **Data Models & Storage (`db.py`, `models.py`, `schemas.py`, `store.py`, `selection.py`)**: SQLAlchemy database setup, ORM models (including tracking `MaterializationEdge`s and the append-only `materialization_lifecycle` table), Pydantic schemas, the `Selection` query language, and local object store logic.
- **Server (`server.py`)**: Read-only FastAPI application for the frontend, plus the invalidation endpoint. Never imports user pipeline code; the pipelines it lists are derived from the run ledger.

### `/web/`
This directory contains the Frontend User Interface. It is a modern single-page application built with React, TypeScript, and Vite.

- **`src/` & `public/`**: Contains the React component code, assets, and frontend logic.
- **`package.json`, `vite.config.ts`, `tsconfig.*`**: Node.js dependencies, Vite build configuration, and TypeScript configuration.
- **`playwright.config.ts`**: E2E testing setup using Playwright.
- The web app talks to the `rubedo` FastAPI backend (`src/api.ts`) to provide a visual dashboard of runs, materializations, lineage, and current outputs, with selection-based invalidation as its one write path.

### `/examples/`
Self-contained runnable pipelines (script + its own data), each demonstrating a different facet — see `examples/README.md` for the full table. `count_lines` is the basics (local files, `params_model`, reduce); `hn_digest` is the flagship non-idempotent-LLM demo (real HN API + LLM, filter → classify → reduce); `github_health`/`weather_advisory` show chained retried/rate-limited/`stale_after` API calls; `gutenberg_stats` shows `skip_cache` + `executor="process"`; `orders_rollup` shows `TableSource` in streaming mode.

### `/docs/`
Contains architectural and planning documentation.
- **`invariants.md`**: Defines the core vocabulary and systemic invariants that ensure the engine operates as a durable materialization system.
- **`TODO.md`**: Open work items as self-contained specs, plus a compressed changelog of what's already shipped.
- **`llms.txt`**: A compact API-teaching doc for LLMs generating Rubedo pipelines.
- **`framework_analysis.md`**: A positioning analysis against Dagster/Prefect/Metaflow/dbt/Hamilton.

### `/tests/`
The test suite for the Python backend.

- Contains `pytest` files like `test_api.py`, `test_engine.py`, and `test_pipelines.py` to ensure the core logic, API endpoints, and processing engine remain stable and bug-free.

### Top-Level Files

- **`pyproject.toml` & `uv.lock`**: Python packaging and dependency management configuration (listing dependencies like `sqlalchemy`, `pydantic`, `fastapi`, `uvicorn`).
- **`.gitignore`**: Git ignore rules.
