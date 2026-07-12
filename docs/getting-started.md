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
from rubedo import ProcessResult, step, pipeline, run, plan, describe

@step(name="read_lines", version="read-v1")
def read_lines(path: str):
    return {"lines": open(path).read().splitlines()}

@step(name="count_lines", version="count-v1", depends_on=["read_lines"])
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(value={"line_count": len(read_lines["lines"])})

p = pipeline(id="count-lines", name="Count Lines", folder="input",
             steps=[read_lines, count_lines])

print(describe(p))            # the DAG, before ever running (also: format="mermaid")
print(plan(p))                # dry-run: what would run() do to my data, and why
summary = run(p)              # execute
print(f"created={summary.created_count} reused={summary.reused_count}")
```

`folder="input"` means: scan `./input` for files, one lane per file, and hand
each root step the file's absolute path. `describe()` renders the DAG before
anything runs; `plan()` is a read-only dry-run of what `run()` would do to
every lane and why (`reuse`, `execute`, `blocked`, `filtered`, `pending`);
`run()` actually executes it and returns a `RunSummary`.

With four input files of different lengths, that quickstart prints:

```text
Plan for 'count-lines' over folder:input: 4 execute, 4 pending
  execute  read_lines           file2.txt @ 2db729839948
  execute  read_lines           file3.txt @ caf5efe6be27
  execute  read_lines           file1.txt @ 48898d92ae13
  execute  read_lines           file4.txt @ 09ae8a2c8171
  pending  count_lines          file1.txt
  pending  count_lines          file2.txt
  pending  count_lines          file3.txt
  pending  count_lines          file4.txt
created=8 reused=0
```

`count_lines`'s lanes show as `pending` in the plan, not `execute` — its
output address depends on `read_lines`'s output, which doesn't exist yet, so
the address (and therefore reuse-or-execute) is unknowable without actually
running `read_lines` first.

### Run it twice

This is the point of the whole project. Run the exact same script again,
untouched:

```text
created=0 reused=8
```

Nothing recomputed — every lane's output is already sitting at its
content-addressed location, so `run()` reads it back instead of re-executing
your code. Now edit one input file and run a third time:

```text
created=2 reused=6
```

Only the edited file's two lanes (`read_lines` and `count_lines`) recompute;
the other three files' outputs are untouched and reused as-is. This is
**surgical invalidation**: Rubedo doesn't know or care that only one file
changed — it just discovers that six of the eight addresses are still valid
and two aren't. For a step that calls a paid LLM instead of counting lines,
this is the difference between a few cents and re-paying for a thousand
rows every time you touch the code.

See the [tutorial](tutorial.md) for a longer walkthrough — indexing,
querying, version bumps, and invalidation.

## The `.rubedo/` state directory

The first `run()` (or `plan()`, or a CLI command) creates a `.rubedo/`
directory: a SQLite ledger (`rubedo.sqlite`) plus a content-addressed object
store (`objects/`). It's created automatically and gitignored automatically
— there's nothing to set up.

!!! warning "`.rubedo/` resolves relative to the current working directory"
    Every entry point — `run()`, `plan()`, the CLI, and the API server —
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

    or the lower-level `RUBEDO_DB_PATH` to point at the SQLite file
    directly. Precedence is `RUBEDO_DB_PATH` > `RUBEDO_HOME`/`rubedo.sqlite`
    > `.rubedo/rubedo.sqlite` (the CWD-relative default). Library calls also
    take a `home=` keyword (`run(p, home="/var/lib/myproject/.rubedo")`) that
    wins over both env vars for that one call.

## `PipelineBuilder`: the fluent alternative

`pipeline(steps=[...])` takes an explicit list; `PipelineBuilder` lets you
accumulate steps with a decorator instead, which reads better once a
pipeline has more than a couple of steps:

```python
from rubedo import PipelineBuilder

p = PipelineBuilder(id="count-lines", name="Count Lines", folder="input")

@p.step(name="read_lines", version="read-v1")
def read_lines(path: str): ...

count_lines = p.build()
```

`p.build()` returns the same `PipelineSpec` object `pipeline()` would —
there's no separate builder-specific runtime behavior. `PipelineBuilder`
also has a `@p.source(...)` decorator, sugar for a parentless
`shape="expand"` root step (see [Shapes](concepts/shapes.md)), used by
[`examples/count_lines`](https://github.com/dinosaurav/Rubedo/tree/main/examples/count_lines)
itself.

## Where to go next

- [Tutorial](tutorial.md) — build a small pipeline up incrementally: indexing
  and querying outputs, editing an input, bumping a step's version, and
  invalidating a selection.
- [Concepts: the model](concepts/model.md) — lanes, coordinates, addresses,
  and the vocabulary the rest of the docs assume.
- [Concepts: shapes](concepts/shapes.md) — `map`, `reduce`, `expand`, `join`.
- [Concepts: sources](concepts/sources.md) — `FolderSource`, `CsvSource`,
  `TableSource`, and writing your own.
- [Concepts: versioning](concepts/versioning.md) — `version` vs. `code`, and
  what a source edit means.
- [Guide: execution policies](guides/execution-policies.md) — retries, rate
  limits, `stale_after`, assertions, `executor="process"`.
- [Guide: search and invalidation](guides/search-and-invalidation.md) —
  `Selection`, `invalidate()`, `trace()`.
- [Guide: inspecting runs](guides/inspecting-runs.md) — `plan()`, `trace()`,
  the CLI, the dashboard.
- [Guide: retention](guides/retention.md) — `retention=N` and `rubedo gc`.
- [Examples](examples.md) — a tour of the runnable example pipelines.
- [API reference](reference/api.md) — every public function and class.
- [CLI reference](reference/cli.md) — `rubedo ls`, `show`, `invalidate`,
  `trace`, `du`, `gc`.
