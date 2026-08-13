# First run

Install, write two functions, run twice. No API key — a folder of files
in, a line count out. Boring on purpose, so you can see reuse without
paying for it.

## Install

```bash
pip install rubedo           # or: pip install "rubedo[server]"
```

Requires Python 3.11+. The `server` extra adds the read-only FastAPI
backend that powers the web dashboard. The `s3` extra
(`pip install "rubedo[s3]"`) adds the S3-compatible cloud store. To hack
on Rubedo itself, or to run the bundled [examples](examples.md), clone the
repo and `uv sync`.

## Two functions

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
def count_lines(scan: dict):  # argument name = parent step
    return {"line_count": len(scan["text"].splitlines())}

print(p.describe())           # the graph, before ever running (also: format="mermaid")
print(p.plan())                # dry-run: what would p.run() do, and why
summary = p.run()
print(f"created={summary.created_count} reused={summary.reused_count}")
```

**What this is doing**

1. **`scan`** lists a folder and yields one item per file.
   `check_cache=False` re-reads the folder every run, so new and edited
   files show up. (Sources are cached like any step by default.)
2. **`count_lines`** runs once per file. The argument name `scan` is the
   parent — Rubedo builds the graph from the function signature.
3. **`plan()`, then `run()`.** `plan()` is a dry-run. `run()` executes.

`scan` is a parentless generator, so its shape is inferred as `expand`
(`out_shape="many"`). Names default to the function names; `version`
defaults to `"0"`. Prefer steps defined away from the pipeline?
`pipeline(steps=[...])` takes an explicit list of `@step`-decorated
functions — one object either way. See [Shapes](concepts/shapes.md).

## What `plan()` prints

With four input files, the first run looks like this:

```text
Plan for 'count-lines' over scan: 1 execute, 1 pending
  execute  scan                 @root
  pending  count_lines          @root
created=8 reused=0
```

`scan` plans as a single `execute` — `check_cache=False` means the source
re-lists the folder every run, so the dry-run has no cached enumeration
to preview. Its actual lanes (one per file) are unknowable until it runs.
`count_lines` shows `pending`, not `execute`: its output address depends
on lanes `scan` hasn't minted yet. Once `p.run()` executes,
`created=8` is `scan`'s four file-lanes plus `count_lines`'s four
downstream lanes.

A root *without* `check_cache=False` is anchor-cached against `@root` and
can plan children as `reuse` — see [Sources](concepts/sources.md).

## Run it twice

Same script, untouched:

```text
created=0 reused=8
```

Nothing recomputed. Edit one input file and run a third time:

```text
created=2 reused=6
```

`created=2` is two steps for one file (`scan` and `count_lines`), not two
files. The other three files stay put. For a step that calls a paid LLM
instead of counting lines, that is the difference between a few cents and
re-paying for a thousand rows.

## Where state lives

The first `p.run()` (or `p.plan()`, or a CLI command) creates a `.rubedo/`
directory: a SQLite ledger (`rubedo.sqlite`), a content-addressed object
store (`objects/`), and Arrow lane tables (`tables/`). Created
automatically, gitignored automatically.

!!! warning "`.rubedo/` resolves relative to the current working directory"
    Every entry point — `p.run()`, `p.plan()`, the CLI, and the API server —
    resolves `.rubedo/` relative to **wherever the process is running from**,
    not relative to the script's location. Running the same pipeline from
    two directories silently creates two stores; `rubedo ls` from the wrong
    directory shows nothing.

    Run everything from your project root. To run from anywhere, pin it:

    ```bash
    export RUBEDO_HOME=/var/lib/myproject/.rubedo
    ```

    Precedence for the ambient default is `RUBEDO_DB_PATH` >
    `RUBEDO_HOME`/`rubedo.sqlite` > `.rubedo/rubedo.sqlite`. The Python API
    takes a `Home` instance, not a path string:

    ```python
    from rubedo import Home, pipeline

    home = Home("/var/lib/myproject/.rubedo")
    pipe = pipeline(name="...", home=home, steps=[...])
    ```

    Each `Home` owns its own ledger, object store, and lane tables —
    concurrent runs against different homes in one process are safe.

## Next

- **[Tutorial](tutorial.md)** — query by content, edit an input, bump a
  version, invalidate a selection.
- **[Examples](examples.md)** — the same ideas against real services.
