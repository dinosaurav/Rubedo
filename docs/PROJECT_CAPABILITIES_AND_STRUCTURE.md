# Rubedo / Rubedo Project Documentation

This document provides a detailed overview of the Rubedo (also internally referred to as `rubedo`) project. It outlines the current capabilities of the system, its architecture, and how each folder contributes to its overall functionality.

## Project Overview & Capabilities

Rubedo is a batch processing framework designed to efficiently run pipelines over collections of coordinates — files in a folder, rows in a CSV. It is composed of a Python-based backend processing engine and a React-based frontend web application.

### Key Capabilities:
1. **DAG Pipeline Framework:** Developers can define multi-step Directed Acyclic Graph (DAG) pipelines using `@step` and `pipeline()` decorators. Steps can declare explicit dependencies on upstream steps, allowing outputs to flow downstream seamlessly.
2. **Pluggable Coordinate Sources:** A `Source` abstraction enumerates coordinates and loads their payloads. `FolderSource` (files, coordinate = relative path) and `CsvSource` (rows, coordinate = key column) ship built in; `folder=` on `pipeline()` is sugar for `FolderSource`.
3. **Durable Materialization & Invalidation:** The engine treats processing as a durable, content-addressed materialization pipeline. It uses point-in-time `Manifest` snapshots to robustly track the state of input sources and skips recomputation if the input hashes, configuration, and code versions haven't changed.
4. **Durable Run Ledger & Lineage:** The system records exhaustive telemetry for each run. It creates a robust event ledger (`RunEvent`) and computes per-coordinate status summaries (`RunCoordinateStatus`) categorizing work as `created`, `reused`, `failed`, `blocked`, or `removed`. `MaterializationEdge` models persist parent-child lineage.
5. **Concurrency & Execution Engine:** Provides a multi-worker execution runner to topologically sort steps and process file tasks in parallel while correctly blocking on failed upstream dependencies.
6. **Database Storage:** Results, metadata, runs, run ledgers, topological lineage, and caching statuses are tracked in a SQL database (via SQLAlchemy).
7. **API Server:** A read-only FastAPI server exposing pipelines, runs, materializations, lineage, and current outputs to the frontend, plus selection-based invalidation as its single write action.
8. **Web UI:** A React + Vite dashboard for browsing runs, outputs, and lineage, and surgically invalidating outputs. Runs themselves are triggered from library code (`rubedo.run`), not the UI.

## Folder Structure

Below is the breakdown of the top-level directories and critical files, and how they contribute to the project.

### `/rubedo/`
This is the core Python backend package containing the execution engine, database logic, API, and CLI.

- **Definitions (`spec.py`)**: `@step` and `pipeline()` build plain `StepSpec`/`PipelineSpec` objects (no registry, no magic module loading); `describe()` renders a DAG as text or Mermaid before it ever runs.
- **Run Phases (`planning.py`, `execution.py`, `ledger.py`, `runner.py`)**: read-only planning (decisions, addresses, staleness/drift), DB-free execution (thread pool, retries, rate limits, ephemeral utils), all ledger writes (manifests, statuses, events, generations), and the `run()`/`plan()` orchestrators. Each run snapshots its pipeline definition into the ledger.
- **Sources & Invalidation (`sources.py`, `hashing.py`, `invalidation.py`)**: The `Source` protocol with `FolderSource`/`CsvSource`, content hashing, and logic for determining if a coordinate's cached result is still valid.
- **Data Models & Storage (`db.py`, `models.py`, `schemas.py`, `store.py`, `selection.py`)**: SQLAlchemy database setup, ORM models (including tracking `MaterializationEdge`s), Pydantic schemas, and local object store logic.
- **Server (`server.py`)**: Read-only FastAPI application for the frontend, plus the invalidation endpoint. Never imports user pipeline code; the pipelines it lists are derived from the run ledger.

### `/web/`
This directory contains the Frontend User Interface. It is a modern single-page application built with React, TypeScript, and Vite.

- **`src/` & `public/`**: Contains the React component code, assets, and frontend logic.
- **`package.json`, `vite.config.ts`, `tsconfig.*`**: Node.js dependencies, Vite build configuration, and TypeScript configuration.
- **`playwright.config.ts`**: E2E testing setup using Playwright.
- The web app likely communicates with the `rubedo` FastAPI backend to provide users with a visual dashboard of their batch processes.

### `/examples/`
Contains sample scripts and inputs to demonstrate how to use the framework.

- **`input/`**: Sample text files or data used by example pipelines.
- **`count_lines.py`, `simple_process.py`, `test_invalidation.py`**: Scripts demonstrating pipeline definition, execution, and how the caching/invalidation system behaves.

### `/docs/`
Contains architectural documentation.
- **`invariants.md`**: Defines the core vocabulary and systemic invariants that ensure the engine operates as a durable materialization system.

### `/tests/`
The test suite for the Python backend.

- Contains `pytest` files like `test_api.py`, `test_engine.py`, and `test_pipelines.py` to ensure the core logic, API endpoints, and processing engine remain stable and bug-free.

### Top-Level Files

- **`pyproject.toml` & `uv.lock`**: Python packaging and dependency management configuration (listing dependencies like `sqlalchemy`, `pydantic`, `fastapi`, `uvicorn`).
- **`.gitignore`**: Git ignore rules.
