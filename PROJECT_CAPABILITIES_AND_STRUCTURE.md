# Batchit / BatchBrain Project Documentation

This document provides a detailed overview of the Batchit (also internally referred to as `batchbrain`) project. It outlines the current capabilities of the system, its architecture, and how each folder contributes to its overall functionality.

## Project Overview & Capabilities

Batchit is a batch processing framework designed to efficiently run custom "processors" over files and data. It is composed of a Python-based backend processing engine and a React-based frontend web application.

### Key Capabilities:
1. **Processor Framework:** Developers can define custom processors using a simple `@processor` decorator. Processors can define specific input schemas (using Pydantic validation), target specific folders, and control concurrency (worker counts).
2. **Durable Materialization & Invalidation:** The engine treats processing as a durable, content-addressed materialization pipeline. It uses point-in-time `Manifest` snapshots to robustly track the state of input folders and skips recomputation if the input hashes, configuration, and code versions haven't changed.
3. **Durable Run Ledger:** The system records exhaustive telemetry for each run. It creates a robust event ledger (`RunEvent`) and computes per-coordinate status summaries (`RunCoordinateStatus`) categorizing work as `created`, `reused`, `failed`, or `removed`.
4. **Concurrency & Execution Engine:** Provides a multi-worker execution runner to process files in parallel.
5. **Database Storage:** Results, metadata, runs, run ledgers, and caching statuses are tracked in a SQL database (via SQLAlchemy).
6. **CLI Interface:** A built-in command-line interface (`batchbrain list`, `batchbrain show`, `batchbrain run`, `batchbrain explain`, `batchbrain show-materialization`, `batchbrain show-run`, `batchbrain show-events`) to inspect outputs, explain addressing logic, trigger processors, and trace execution history easily.
6. **API Server:** A FastAPI-based server exposing the processor states, runs, and potentially providing endpoints for the frontend.
7. **Web UI:** A React + Vite frontend for managing, visualizing, or monitoring the batch processes and their results.

## Folder Structure

Below is the breakdown of the top-level directories and critical files, and how they contribute to the project.

### `/batchbrain/`
This is the core Python backend package containing the execution engine, database logic, API, and CLI.

- **Execution & Processing (`runner.py`, `processor_runner.py`, `processor_worker.py`)**: Handles the lifecycle of running processors, managing parallel workers, and applying them to files.
- **State & Invalidation (`hashing.py`, `invalidation.py`, `scanner.py`)**: Responsible for scanning target directories, calculating file hashes, and determining if a file's cached result is still valid or needs to be recomputed.
- **Data Models & Storage (`db.py`, `models.py`, `schemas.py`, `store.py`, `selection.py`)**: SQLAlchemy database setup, ORM models, Pydantic schemas, and store access functions to persist run summaries and results.
- **Server & API (`api.py`, `server.py`)**: Implements a FastAPI application to expose the batch engine's capabilities over HTTP.
- **CLI (`cli.py`)**: Provides terminal commands to interact with the engine.
- **Processor Management (`registry.py`)**: Handles registering and looking up available processors defined in the project.

### `/web/`
This directory contains the Frontend User Interface. It is a modern single-page application built with React, TypeScript, and Vite.

- **`src/` & `public/`**: Contains the React component code, assets, and frontend logic.
- **`package.json`, `vite.config.ts`, `tsconfig.*`**: Node.js dependencies, Vite build configuration, and TypeScript configuration.
- **`playwright.config.ts`**: E2E testing setup using Playwright.
- The web app likely communicates with the `batchbrain` FastAPI backend to provide users with a visual dashboard of their batch processes.

### `/examples/`
Contains sample scripts and inputs to demonstrate how to use the framework.

- **`input/`**: Sample text files or data used by example processors.
- **`simple_process.py`, `test_invalidation.py`**: Scripts demonstrating processor definition, execution, and how the caching/invalidation system behaves.

### `/docs/`
Contains architectural documentation.
- **`invariants.md`**: Defines the core vocabulary and systemic invariants that ensure the engine operates as a durable materialization system.

### `/tests/`
The test suite for the Python backend.

- Contains `pytest` files like `test_api.py`, `test_engine.py`, and `test_processor.py` to ensure the core logic, API endpoints, and processing engine remain stable and bug-free.

### Top-Level Files

- **`batchbrain_processors.py`**: An example entry point at the root of the project. It demonstrates how to define a concrete processor (e.g., `count-lines`) with input validation (`CountLinesInputs`) and run it using the framework.
- **`pyproject.toml` & `uv.lock`**: Python packaging and dependency management configuration (listing dependencies like `sqlalchemy`, `pydantic`, `fastapi`, `uvicorn`).
- **`.gitignore`**: Git ignore rules.
