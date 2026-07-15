# CLI Reference

The `rubedo` command (entry point `rubedo.cli:main`, installed by
`pip install rubedo`) is a **read-only ops CLI** over the local ledger, plus
one write path (`invalidate`) and one destructive-but-gated path
(`gc --delete`). It never imports your pipeline code — everything it shows
comes from the ledger and the `definition()` snapshot each run recorded.

```bash
rubedo --help
```

```text
usage: rubedo [-h] {ls,show,invalidate,trace,du,gc,serve,check} ...

positional arguments:
  {ls,show,invalidate,trace,du,gc,serve,check}
    ls                  List recent runs
    show                Show details for a specific run
    invalidate          Invalidate materializations by selection query
    trace               Follow lineage up/downstream from a selection
    du                  Report object-store usage and a reclaimable dry-run
                        audit
    gc                  Retention GC: prune old runs' outputs and delete
                        unreferenced objects (dry-run unless --delete)
    serve               Start the read-only FastAPI server (API + web UI)
    check               Lint a pipeline file for undeclared env reads
```

## Where it looks: `.rubedo/` resolution

The CLI (like every library entry point) resolves its ledger and object
store **relative to the current working directory** by default. The ledger
(SQLite file) and the object store (content-addressed blobs) are resolved
independently, each with its own precedence:

- **Ledger:** `RUBEDO_DB_PATH` (an exact SQLite path or `sqlite:///...` URL)
  if set, else `$RUBEDO_HOME/rubedo.sqlite`, else `.rubedo/rubedo.sqlite`.
- **Object store:** always under `$RUBEDO_HOME/objects/` if `RUBEDO_HOME` is
  set, else `.rubedo/objects/` — `RUBEDO_DB_PATH` has **no effect** on
  where objects live.

```bash
export RUBEDO_HOME=/var/lib/myproject/rubedo   # pin both the ledger and the store
rubedo ls                                       # now works from anywhere
```

!!! note "`RUBEDO_DB_PATH` alone splits the ledger from the store"
    Setting only `RUBEDO_DB_PATH` (without `RUBEDO_HOME`) moves the SQLite
    ledger but **not** the object store, which stays under
    `./.rubedo/objects` relative to wherever you run the command. For a
    single portable override, set `RUBEDO_HOME` — it moves both.

!!! note "Run every tool from the same directory"
    Pipelines, the CLI, and the server must all resolve to the *same*
    `.rubedo/` to see the same state. Running `rubedo ls` from the wrong
    directory doesn't error — it silently opens (or creates) a fresh, empty
    store at whatever `.rubedo/` it finds there. If a command reports "no
    runs found" unexpectedly, check `pwd` before checking anything else.

---

## `rubedo ls`

List recent runs, most recent first.

```
rubedo ls [--limit LIMIT]
```

| Flag | Default | Meaning |
|---|---|---|
| `--limit LIMIT` | `50` | Number of runs to show |

**Example:**

```bash
rubedo ls --limit 5
```

```text
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━┓
┃ ID          ┃ Pipeline    ┃ Status      ┃ Created /   ┃ Failed ┃ Started At  ┃
┃             ┃             ┃             ┃ Reused      ┃        ┃             ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━┩
│ run_39786b… │ graphify    │ completed   │ 157 / 0     │ 0      │ 2026-07-12… │
│ run_2b1b5f… │ executor-s… │ completed_… │ 0 / 8       │ 8      │ 2026-07-12… │
│ run_d29136… │ executor-s… │ completed_… │ 8 / 0       │ 8      │ 2026-07-12… │
│ run_22c9e8… │ pdf-digest  │ completed   │ 12 / 0      │ 0      │ 2026-07-12… │
│ run_d3ae49… │ hn-digest   │ completed   │ 26 / 0      │ 0      │ 2026-07-12… │
└─────────────┴─────────────┴─────────────┴─────────────┴────────┴─────────────┘
```

`Status` is derived (`completed` / `completed_with_failures` / `failed`, or
`running`/`interrupted` from the run's heartbeat) — never a stored
"running" value; see [`../concepts/model.md`](../concepts/model.md).

---

## `rubedo show`

Show details for one run: status, timing, per-run totals, and a per-step
breakdown; or, with `--failed`, just the failures.

```
rubedo show <run_id> [--json] [--failed]
```

| Flag | Meaning |
|---|---|
| `run_id` (positional, required) | The run id, e.g. `run_39786b4eeef4` |
| `--json` | Output as JSON instead of formatted tables |
| `--failed` | Show only failure details (step, coordinate, error type, message) |

**Example:**

```bash
rubedo show run_39786b4eeef4
```

```text
Run ID: run_39786b4eeef4
Pipeline: graphify
Status: completed
Started At: 2026-07-12T04:16:05.357276Z
Finished At: 2026-07-12T04:16:30.287039Z
Summary: Created: 157, Reused: 0, Failed: 0, Blocked: 0, Filtered: 0
                               Step Outcomes
┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┓
┃ Step                   ┃ Created ┃ Reused ┃ Failed ┃ Blocked ┃ Filtered ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━┩
│ src_files              │ 51      │ 0      │ 0      │ 0       │ 0        │
│ extract_code_nodes     │ 51      │ 0      │ 0      │ 0       │ 0        │
│ ...
└────────────────────────┴─────────┴────────┴────────┴─────────┴──────────┘
```

`rubedo show <run_id> --failed --json` gives you a structured list of
failures for scripting — each entry has `step_name`, `coordinate`,
`error_type`, `error_message`.

---

## `rubedo invalidate`

Invalidate materializations matching a [`Selection`
query](../guides/search-and-invalidation.md) — a logical tombstone; no
bytes or ledger rows are deleted.

```
rubedo invalidate <selection> --reason REASON [--downstream]
```

| Flag | Meaning |
|---|---|
| `selection` (positional, required) | Selection query string, e.g. `"step:extract company:acme"` |
| `--reason REASON` | **Required.** Why — recorded on the lifecycle row |
| `--downstream` | Also invalidate the full downstream closure over recorded lineage (see the [warning on blast radius](../guides/search-and-invalidation.md#widening-the-blast-radius-downstreamtrue)) — preview it first with `rubedo trace` |

**Example:**

```bash
rubedo invalidate "step:enrich company:acme" --reason "bad prompt"
```

```text
Invalidated 12 materializations.
New Run ID recorded for invalidation: run_a1b2c3d4e5f6
```

With `--downstream`, the output additionally breaks down seeds vs. the
downstream closure:

```text
Invalidated 47 materializations (12 seeds + 35 downstream).
New Run ID recorded for invalidation: run_a1b2c3d4e5f6
```

!!! warning "Preview first"
    `--downstream` can invalidate far more than the seed count suggests,
    especially through a `reduce` or `join` (one bad lane contaminates the
    whole fan-in output and everything after it). Run
    `rubedo trace "<same selection>"` first and read its counts before
    adding `--downstream`.

---

## `rubedo trace`

Follow lineage upstream and downstream from a selection — read-only, no
mutation.

```
rubedo trace <selection> [--all] [--json]
```

| Flag | Meaning |
|---|---|
| `selection` (positional, required) | Selection query string, e.g. `"company:acme step:extract"` |
| `--all` | Seed superseded/invalidated/pruned generations too (default: only live materializations seed the trace) |
| `--json` | Output nodes and edges as JSON |

**Example:**

```bash
rubedo trace "step:top_story"
```

```text
Trace: 15 seed, 0 upstream, 21 downstream
  seed       top_story            row-4d3d039e1592             @ 2581baed330a  value={'id': 48877668}
  seed       top_story            row-2f62339fd0eb             @ da063d067fa4  value={'id': 48876505}
  ...
  downstream screen               row-4d3d039e1592             @ 9c1a4e2b7f10
  downstream digest               @all                         @ f30a221cc890
```

If nothing matches, `rubedo trace` prints a note suggesting `--all` (unless
you already passed it):

```text
No live materializations match that selection. (try --all to include superseded ones)
```

---

## `rubedo du`

Report object-store usage per the ledger, plus a dry-run reclaimable
estimate. Never deletes anything.

```
rubedo du [--json]
```

| Flag | Meaning |
|---|---|
| `--json` | Output the full `StorageReport` as JSON |

**Example:**

```bash
rubedo du
```

```text
Object store: 277 objects, 5.7 MiB (349 materializations, 349 live)
                           Storage by pipeline / step
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━┓
┃ Pipeline      ┃ Step           ┃       Size ┃ Objects ┃ Materializat… ┃ Live ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━┩
│ count-lines   │ (all steps)    │    1.2 KiB │      20 │            30 │   30 │
│               │ count_lines    │      130 B │       5 │             7 │    7 │
│               │ ...
└───────────────┴────────────────┴────────────┴─────────┴───────────────┴──────┘
Objects are shared (content-addressed), so per-scope sizes can sum to more than
the total.
Reclaimable (dry-run — nothing is deleted): 0 objects / 0 B have zero live
references
```

See [`../guides/inspecting-runs.md`](../guides/inspecting-runs.md#rubedo-du-storage_report-why-is-rubedo-this-big)
for what "reclaimable" and "missing" mean.

---

## `rubedo gc`

Retention GC: apply every pipeline's recorded `retention=` policy, plus an
optional global byte budget. **Dry-run by default** — nothing is deleted
unless you pass `--delete`.

```
rubedo gc [--max-bytes SIZE] [--delete]
```

| Flag | Meaning |
|---|---|
| `--max-bytes SIZE` | Global byte budget, e.g. `2GiB`, `500MB`, or a raw byte count. After applying per-pipeline retention, prunes the oldest-referenced outputs across all pipelines (never anything a pipeline's *latest* run uses) until the store fits |
| `--delete` | Actually demote and delete. Default is dry-run: print exactly what `--delete` would do and touch nothing |

Accepted size units for `--max-bytes`: `B`, `KB`/`KiB`, `MB`/`MiB`,
`GB`/`GiB`, `TB`/`TiB` — decimal (`KB`) and binary (`KiB`) spellings are
both treated as binary (1024-based).

**Example (dry-run, the default):**

```bash
rubedo gc --max-bytes 2GiB
```

```text
Would prune 3 materialization(s); would prune 5 object(s) / 1.8 MiB  (dry-run —
nothing deleted; pass --delete to apply)
budget: 5.7 MiB before -> ~3.9 MiB after (max 2.0 GiB)
```

**Example (applied):**

```bash
rubedo gc --max-bytes 2GiB --delete
```

```text
Pruned 3 materialization(s); pruned 5 object(s) / 1.8 MiB
budget: 5.7 MiB before -> ~3.9 MiB after (max 2.0 GiB)
```

!!! warning "`--delete` refuses while any run is live"
    `rubedo gc --delete` exits `1` and prints `GC refused: ...` if any
    run's heartbeat currently reads "running" — a concurrent run could be
    mid-commit on an output pointing at bytes GC is about to unlink. Retry
    once the other run finishes. Dry-run (no `--delete`) is always safe.

See [`../guides/retention.md`](../guides/retention.md) for the full
policy model — what a prune does, the two triggers (`retention=` auto-prune
and `rubedo gc`), and the bytes-never-facts guarantee.

---

## `rubedo serve`

```bash
rubedo serve [--host HOST] [--port PORT] [--reload]
```

Starts the read-only FastAPI server. The API is at `/api/*` and the web
dashboard is served at `/` (from the built assets bundled with the
package). Requires the `server` extra: `pip install "rubedo[server]"`.

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | Bind address |
| `--port` | `8000` | Port |
| `--reload` | *(off)* | Auto-reload on Python file changes (dev) |

```bash
rubedo serve                    # http://127.0.0.1:8000
rubedo serve --port 9000        # custom port
rubedo serve --host 0.0.0.0     # listen on all interfaces
```

If the web assets aren't built (or aren't bundled), `rubedo serve` falls
back to API-only mode and prints a note about running `cd web && npm run
build`. To hack on the web UI itself, run `npm run dev` in the `web/`
directory — Vite's dev server proxies `/api` to the backend.

---

## Environment variables

| Variable | Effect |
|---|---|
| `RUBEDO_DB_PATH` | Exact SQLite path or `sqlite:///...` URL for the **ledger only**. Highest precedence for the ledger; has no effect on the object store. |
| `RUBEDO_HOME` | Root directory for both the ledger (`$RUBEDO_HOME/rubedo.sqlite`, used when `RUBEDO_DB_PATH` is unset) and the object store (`$RUBEDO_HOME/objects/`, always). |
| *(neither set)* | Falls back to `.rubedo/` under the current working directory for both. |

!!! note "The CLI has no `--home` flag"
    None of the subcommands above take a store-location flag — the CLI
    only reads `RUBEDO_HOME`/`RUBEDO_DB_PATH` (or the `.rubedo/` default).
    The **Python API** offers an override instead: `pipeline(name=...,
    home=...)` points every `.run()`/`.plan()` of that pipeline at a custom
    root, and `trace(sel, home=...)`, `gc(home=...)`, and
    `storage_report(home=...)` each accept `home=` to point that one call at
    a custom root — all taking precedence over both environment variables.
