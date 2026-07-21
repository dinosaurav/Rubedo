# Getting Started

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only FastAPI backend
that powers the web dashboard — skip it if you only need the library and CLI.

To hack on Rubedo itself, or to run the bundled [examples](examples.md),
clone the repo and `uv sync` instead.

## Quickstart

Pipelines are plain Python objects — define them wherever your code lives,
no project scaffolding required:

```python
from rubedo import pipeline

p = pipeline(name="count-lines")

@p.step(check_cache=False)   # rescan the folder every run
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step
def count_lines(scan: dict):
    return {"line_count": len(scan["text"].splitlines())}

print(p.describe())           # the DAG, before ever running (also: format="mermaid")
print(p.plan())                # dry-run: what would p.run() do to my data, and why
summary = p.run()              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")
```

There's no `folder=` kwarg — ingestion is just a step: `scan` is a
parentless generator that walks `./input` and `yield`s each file's own
content (not just its path — the yielded payload is what gets hashed into
the lane's identity), so its `shape="expand"` (`out_shape="many"`) is
inferred; `count_lines`'s parameter names the `scan` step, so its
dependency is inferred too (see [Shapes](concepts/shapes.md)). `p.describe()` renders the DAG before
anything runs; `p.plan()` is a read-only dry-run of what `p.run()` would do to
every lane and why (`reuse`, `execute`, `blocked`, `filtered`, `pending`);
`p.run()` actually executes it and returns a `RunSummary`.

`check_cache=False` matters here: by default a root generator's fan-out is
cached against its own identity like any `expand` (see
[Shapes](concepts/shapes.md#expand-1n-fan-out)), so it wouldn't notice a
folder edit on its own — `check_cache=False` re-runs `scan` every `p.run()`
so it always sees the folder's current contents (see
[Sources](concepts/sources.md)).

With four input files, that quickstart prints:

```text
Plan for 'count-lines' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  count_lines          @root
created=8 reused=0
```

`scan` plans as a single `execute` — it has no parent to cache its
enumeration against, so its actual lanes are unknowable until it runs.
`count_lines` shows `pending`, not `execute`: its output address depends on
lanes `scan` hasn't minted yet, so the address (and therefore
reuse-or-execute) is unknowable without actually running `scan` first. Once
`p.run()` actually executes it, `created=8` is `scan`'s four file-lanes plus
`count_lines`'s four downstream lanes.

### Run it twice

This is the point of the whole project. Run the exact same script again,
untouched:

```text
created=0 reused=8
```

Nothing recomputed — every lane's output is already sitting at its
content-addressed location, so `p.run()` reads it back instead of re-executing
your code. Now edit one input file and run a third time:

```text
created=2 reused=6
```

Only the edited file's two lanes (`scan` and `count_lines`) recompute; the
other three files' outputs are untouched and reused as-is. This is
**surgical invalidation**: Rubedo doesn't know or care that only one file
changed — it just discovers that six of the eight addresses are still valid
and two aren't. For a step that calls a paid LLM instead of counting lines,
this is the difference between a few cents and re-paying for a thousand
rows every time you touch the code.

See the [tutorial](tutorial.md) for a longer walkthrough — querying
outputs, version bumps, and invalidation.

## The `.rubedo/` state directory

The first `p.run()` (or `p.plan()`, or a CLI command) creates a `.rubedo/`
directory: a SQLite ledger (`rubedo.sqlite`) plus a content-addressed object
store (`objects/`). It's created automatically and gitignored automatically
— there's nothing to set up.

!!! warning "`.rubedo/` resolves relative to the current working directory"
    Every entry point — `p.run()`, `p.plan()`, the CLI, and the API server —
    resolves `.rubedo/` relative to **wherever the process is running from**,
    not relative to the script's location or the pipeline's definition.
    Running the same pipeline from two different directories silently
    creates two separate, empty-looking stores; `rubedo ls` run from the
    wrong directory just shows nothing.

    Run everything from your project root (typically the repo root), and
    keep it consistent. If you need to run from anywhere — a cron job, a
    packaged CLI, a different working directory per invocation — pin the
    location explicitly instead of relying on the CWD:

    ```bash
    export RUBEDO_HOME=/var/lib/myproject/.rubedo
    ```

    or the lower-level `RUBEDO_DB_PATH` to point at the SQLite ledger
    directly. Precedence for the ambient default is `RUBEDO_DB_PATH` >
    `RUBEDO_HOME`/`rubedo.sqlite` > `.rubedo/rubedo.sqlite` (the
    CWD-relative default). The Python API takes a `Home` instance
    instead of a path string:

    ```python
    from rubedo import Home, pipeline

    home = Home("/var/lib/myproject/.rubedo")
    pipe = pipeline(name="...", home=home, steps=[...])
    ```

    Each `Home` owns its own ledger, object store, and lane tables, so
    concurrent runs against different homes in one process are safe —
    construct one `Home` per root and inject it.

## Registering steps as a list

The `@p.step` decorators above accumulate steps on the `Pipeline` object;
`pipeline(steps=[...])` takes an explicit list of `@step`-decorated
functions instead, which suits steps defined away from the pipeline that
uses them — there's no separate builder class, just one object either way,
and both forms compose freely:

```python
from rubedo import step, pipeline

@step(check_cache=False)
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@step
def count_lines(scan: dict): ...

p = pipeline(name="count-lines", steps=[scan, count_lines])
```

There's no `.build()` step: the underlying `PipelineSpec` is constructed and
validated lazily the first time you call a verb (`.run()`/`.plan()`/
`.describe()`), and cached from then on. `scan` above is a parentless
generator, so its `shape="expand"` (`out_shape="many"`) is inferred
automatically (see [Shapes](concepts/shapes.md)) — the same recipe
[`examples/count_lines`](https://github.com/dinosaurav/Rubedo/tree/main/examples/count_lines)
uses itself.

## Where to go next

- [Tutorial](tutorial.md) — build a small pipeline up incrementally:
  querying outputs, editing an input, bumping a step's version, and
  invalidating a selection.
- [Concepts: the model](concepts/model.md) — lanes, coordinates, addresses,
  and the vocabulary the rest of the docs assume.
- [Concepts: shapes](concepts/shapes.md) — `map`, `aggregate`, `fold`, `expand`, `join`.
- [Concepts: sources](concepts/sources.md) — the folder, CSV, SQL table, and
  cloud storage ingestion recipes.
- [Concepts: versioning](concepts/versioning.md) — `version` vs. `code`, and
  what a source edit means.
- [Guide: execution policies](guides/execution-policies.md) — retries, rate
  limits, `stale_after`, assertions, `executor="process"`.
- [Guide: search and invalidation](guides/search-and-invalidation.md) —
  `Selection`, `invalidate()`, `trace()`.
- [Guide: inspecting runs](guides/inspecting-runs.md) — `p.plan()`, `trace()`,
  the CLI, the dashboard.
- [Guide: retention](guides/retention.md) — `retention=N` and `rubedo gc`.
- [Examples](examples.md) — a tour of the runnable example pipelines.
- [API reference](reference/api/index.md) — every public function and class.
- [CLI reference](reference/cli.md) — `rubedo ls`, `show`, `invalidate`,
  `trace`, `du`, `gc`.
