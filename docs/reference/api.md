# API Reference

Everything here is importable from the top-level `rubedo` package —
`from rubedo import step, pipeline, ...` — except `gc()` and
`storage_report()`, which live in their own submodules (`rubedo.gc`,
`rubedo.du`) and aren't part of `rubedo.__all__`. There is no free
`run()`/`plan()`/`describe()` — they're methods on the `Pipeline` object
`pipeline()` returns (see below); `trace()`/`invalidate()`/`gc()` stay free
functions since they're store-level, not pipeline-level.

This page documents signatures, parameters, and defaults as they exist in
`src/rubedo/`. If something here and the docstring in source ever disagree,
the source wins — this pre-1.0 API moves fast (see
[notes/invariants.md](../notes/invariants.md)).

## `@step`

```python
def step(
    fn: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    version: str = "0",
    depends_on: Optional[Union[List[str], Dict[str, str]]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    workers: int = 4,
    code: str = "warn",
    retries: int = 0,
    retry_on=Exception,
    retry_delay: float = 0.0,
    retry_backoff: float = 1.0,
    rate_limit: Optional[str] = None,
    stale_after: Optional[str] = None,
    skip_cache: bool = False,
    index: Optional[List[str]] = None,
    shape: Optional[str] = None,
    executor: str = "thread",
    group_key: Optional[str] = None,
    join_on: Optional[Dict[str, str]] = None,
    output_model: Optional[Type[BaseModel]] = None,
    assertions: Optional[List[Callable[[Any], None]]] = None,
    on_failed: Literal["use_passed", "block"] = "use_passed",
)
```

A decorator that turns a plain function into a `StepSpec`. The engine never
imports your code — `@step` just builds a data object; nothing runs until
`p.run()`. Works bare (`@step`) or called (`@step()`, `@step(version="2")`,
...) — both mint the same `StepSpec`.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` \| `None` | function's `__name__` | The step's identity within the pipeline; referenced by `depends_on`. Two steps that resolve to the same name (explicit or defaulted) fail loudly at pipeline-construction time, naming both functions. |
| `version` | `str` | `"0"` | Semantic identity — bump it for a deliberate behavior change. Folded into every lane's output address, so bumping recomputes the whole step regardless of `code=`. Cannot be the literal string `"auto"` (that's what `code="auto"` is for). |
| `depends_on` | `list[str]` \| `dict[str, str]` \| `None` | `None` | Parent step names. When omitted entirely, it's *inferred* from `fn`'s signature (see below). Passed as a `list`, it's explicit and disables inference. Passed as a `dict` (`{"param_name": "step_name"}`), it's an *alias*: binds a parent's output to a differently-named parameter — also explicit, also disables inference. Either way, no parents makes this step a root — `shape="expand"` yields the pipeline's initial lanes (ingestion is just this shape; see [Concepts: sources](../concepts/sources.md)), or a `shape="map"` root mints a single source-less `@root` lane from `params`. |
| `params_model` | `Type[BaseModel]` \| `None` | `None` | A Pydantic model overriding the pipeline-level `params_model` for this step's own validation (rare; usually set on `pipeline()` instead). |
| `workers` | `int` | `4` | Thread/process pool size for this step, overridable per-run via `p.run(workers=N)`. |
| `code` | `"warn"` \| `"auto"` | `"warn"` | What a source edit means. `"warn"`: edits never recompute, but reusing an output whose code has since changed logs a loud warning (in run output, event log, and `p.plan()`). `"auto"`: the function's source hash joins the cache identity, so any edit recomputes with no version bump — requires an inspectable function source. |
| `retries` | `int` | `0` | Extra attempts after a failure, for exceptions matching `retry_on` only. Every attempt is logged as a run event. |
| `retry_on` | exception type or tuple | `Exception` | Narrow this to transient error types — retrying a deterministic bug on a paid API just multiplies its cost. |
| `retry_delay` | `float` | `0.0` | Seconds between retry attempts. |
| `retry_backoff` | `float` | `1.0` | Multiplier applied to `retry_delay` after each attempt. |
| `rate_limit` | `str` \| `None` | `None` | `"10/min"`, `"2/s"`, `"500/hour"` — paces this step's executions across all its workers, retries included. Parsed by `parse_rate_limit`; raises `ValueError` on a bad format. |
| `stale_after` | `str` \| `None` | `None` | `"24h"`, `"30min"`, `"7d"` — a cached output older than this re-executes on the next run. Different bytes supersede the old generation (downstream recomputes); identical bytes just refresh the freshness clock. Parsed by `parse_duration`. |
| `skip_cache` | `bool` | `False` | Marks an inline util: never materialized or recorded; its identity fuses into consumers' cache keys and it runs lazily (memoized per run) only when a consumer actually executes. Incompatible with `shape="expand"`, `shape="reduce"`, `stale_after`, and `index`. A `skip_cache` step must have at least one consumer. |
| `index` | `list[str]` \| `None` | `None` | Dotted-path fields of the output *value* to extract into the search index at commit time (e.g. `["company", "meta.region"]`). List-valued fields index one entry per element. Never affects cache identity — only newly created materializations are indexed under a new declaration. Incompatible with `skip_cache`. |
| `shape` | `"map"` \| `"reduce"` \| `"expand"` \| `"join"` \| `None` | `None` (inferred) | When omitted, inferred from the code: a generator function → `"expand"`; `join_on=` → `"join"`; `group_key=` → `"reduce"`; otherwise `"map"`. An explicit value always wins; an explicit value that contradicts what the code implies (a non-`"expand"` shape on a generator, or a shape that doesn't match `join_on=`/`group_key=`) raises. See [Concepts: shapes](../concepts/shapes.md). |
| `executor` | `"thread"` \| `"process"` | `"thread"` | `"process"` runs this step in a `loky` process pool (serialized via `cloudpickle`, so closures are fine) — for CPU-bound work. |
| `group_key` | `str` \| `None` | `None` | `shape="reduce"` only: an indexed field of the parent output to partition lanes by — one reduction per distinct value instead of one `"@all"` reduction. |
| `join_on` | `dict[str, str]` \| `None` | `None` | `shape="join"` only: `{parent_step: indexed_field}` for each of (at least two) parents — the N-way equijoin key. Keys must exactly match `depends_on`. |
| `output_model` | `Type[BaseModel]` \| `None` | `None` | Optional Pydantic model validated (`model_validate`) against the step's output value before it commits — raising fails the step, same as a failing `assertions` entry — and recorded into `definition()`'s JSON schema snapshot. |
| `assertions` | `list[Callable[[Any], None]]` \| `None` | `None` | Callables run against the committed output *value* before it commits; raising fails the step so bad data never propagates downstream. |
| `on_failed` | `"use_passed"` \| `"block"` | `"use_passed"` | `reduce`/`join` only: `"use_passed"` drops failed/blocked parent lanes and proceeds with the survivors (firing a `partial_fan_in` warning); `"block"` halts the step entirely if any parent lane is unavailable. |

```python
from rubedo import ProcessResult, step

def check_price_positive(val: dict):
    if val["price"] < 0:
        raise ValueError("Negative price")

@step(
    name="enrich",
    version="1.0.0",
    retries=3,
    retry_on=(TimeoutError, ConnectionError),
    retry_delay=1,
    retry_backoff=2,
    rate_limit="30/min",
    stale_after="24h",
    assertions=[check_price_positive],
)
def enrich(row: dict) -> ProcessResult:
    ...
```

At the other extreme, a step that needs no explicit policy at all can drop
`name=`/`version=` entirely:

```python
@step()
def parse(scan: dict): ...   # name="parse", version="0", code="warn"
```

Parameter binding: a root step (source-less `map` or root `expand`) receives
no payload argument at all — only `params`, if declared; a dependent step
(including a root `expand`'s own downstream consumers) receives one
**keyword** argument per parent, named after
that parent's step name (a `reduce` step's parent kwarg is a
`{coordinate: value}` dict instead of a single value); any step may
additionally declare a `params` argument to receive the run's validated
params (see [Concepts: model](../concepts/model.md)). A step returns
either a plain JSON-serializable value, a `ProcessResult` (value +
metadata), or `Filtered(reason=...)` to decline the lane.

A `StepSpec` is itself callable — `extract(scan={"text": "hi"})` is a pure
passthrough to the decorated function, so a step is directly unit-testable
without the engine, a store, or a ledger:

```python
def test_extract_uppercases():
    assert extract(scan={"text": "hi"}) == "HI"
```

### Shape and `depends_on` inference

Both of those are also inferred when not spelled out, since a decorated
function's own shape already implies most of this:

- `shape` defaults to `"expand"` for a generator function, `"join"`/
  `"reduce"` when `join_on=`/`group_key=` is given, `"map"` otherwise. An
  explicit `shape=` always wins; an explicit value that contradicts what
  the code implies (say, a generator decorated `shape="map"`, or `join_on=`
  on a step whose explicit shape isn't `"join"`) raises at decoration time
  rather than misbehaving mid-run.
- `depends_on`, left unset, is resolved once every step in the pipeline is
  registered (`pipeline()`/`p.step()` — decoration time can't see sibling
  steps yet): every parameter of the function other than `params` must
  name a registered step, and becomes a dependency, in signature order —
  `def parse(scan: dict)` above infers `depends_on=["scan"]` the moment
  `parse` sits in a pipeline alongside a step named `scan`. An unmatched
  parameter raises `ValueError` naming the step, the parameter, and the
  available step names — a signature typo dies at build instead of a
  call-time `TypeError` deep in a run. A function using `*args`/`**kwargs`
  skips inference entirely (it's a root unless you pass `depends_on=`
  explicitly). A step with no non-`params` parameter is a root.

Passing `depends_on=` explicitly — as a `list` (unchanged) or as
`{"param_name": "step_name"}` to bind a parent's output to a
differently-named parameter — always disables inference for that step.
Both forms build the exact same `StepSpec` inference would have, so
`definition()` (and every cached address) is identical either way — reach
for the explicit form once a pipeline has enough steps that inference stops
reading as obvious, or when a parameter's name legitimately differs from
the step it depends on.

A parentless generator function is a source-shaped `expand` root, sugar-free
— its `shape="expand"` is inferred automatically:

```python
from rubedo import step

@step
def hn_top():
    for sid in fetch_top_ids():
        yield fetch_story(sid)
```

Drop it straight into `pipeline(steps=[...])` — nothing else needed, since
the decorated function *is* the source. See
[Concepts: sources](../concepts/sources.md) for the folder/CSV/table/cloud
recipes.

## `pipeline()` / `Pipeline`

```python
def pipeline(
    name: str,
    steps: Optional[List[StepSpec]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    retention: Optional[int] = None,
    schedule: str = "broad",
    home: Optional[str] = None,
) -> Pipeline
```

Constructs a `Pipeline` — the one object steps register on and every verb
(`.run()`/`.plan()`/`.describe()`/`.definition()`) lives on. There is no
separate builder class and no free `run()`/`plan()`/`describe()`: `name` is
the pipeline's sole identity (there is no `id=`), and settings that apply to
every run of the pipeline — `schedule=`, `home=`, `retention=`,
`params_model=` — are constructor arguments, not per-call ones.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `name` | `str` | required | The pipeline's sole identity — recorded verbatim as the ledger's `pipeline_id` on every run. Renaming a pipeline orphans its history. |
| `steps` | `list[StepSpec]` \| `None` | `None` | Steps built by `@step`, if you already have them as a list. Steps can also be registered afterward via `@p.step(...)` — both forms compose freely. |
| `params_model` | `Type[BaseModel]` \| `None` | `None` | A Pydantic model that `p.run(params={...})`/`p.plan(params={...})` validate against; steps that declare a `params` argument receive the validated, JSON-dumped dict. |
| `retention` | `int` \| `None` | `None` | Keep only this pipeline's last N terminal runs' outputs; older, no-longer-referenced generations are pruned at the end of each successful run. Must be `>= 1` if set — validated eagerly, at construction. See [Guide: retention](../guides/retention.md). |
| `schedule` | `"broad"` \| `"deep"` | `"broad"` | Execution *order* for every run of this pipeline — never results (cache identity is order-independent). `"broad"` completes each step across all lanes before the next starts (paid-step-safe inspection checkpoints). `"deep"` lets each lane race ahead through consecutive 1:1 `map` steps as soon as its own inputs land. `reduce`/`join`/`expand`/multi-parent maps always synchronize on all lanes either way. Validated eagerly. |
| `home` | `str` \| `None` | `None` | Points this pipeline's ledger/object store at a custom root instead of the default `.rubedo`/`RUBEDO_HOME`, for every `.run()`/`.plan()` call. |

There is no `.build()`: the underlying `PipelineSpec` (at least one root
step; `skip_cache`/`join`/`group_key` consistency) is constructed and
validated lazily the first time you call a verb or access `.spec`, and
cached from then on — so registering more steps after that first call
invalidates the cache and it rebuilds.

```python
import csv
from rubedo import step, pipeline

@step
def leads():
    with open("data/leads.csv", newline="") as f:
        yield from csv.DictReader(f)

@step
def enrich(leads: dict):
    return {"email": leads["email"], "summary": call_llm(leads["notes"])}

pipeline(name="enrich-leads", steps=[leads, enrich])
```

Or register steps with decorators on the same object — no separate builder
class:

```python
p = pipeline(name="count-lines")

@p.step()
def scan():
    import os
    for name in sorted(os.listdir("input")):
        path = os.path.join("input", name)
        if os.path.isfile(path):
            yield {"path": name, "text": open(path).read()}

@p.step()
def read_lines(scan: dict):
    return {"lines": scan["text"].splitlines()}

count_lines_pipeline = p
```

A pipeline needs no explicit source step at all: some root step must
originate lanes itself — either a `shape="expand"` root (yields N lanes
every run) or a source-less `shape="map"` root (mints a single `@root`
lane from its `params`).

### `Pipeline.run()`

```python
def run(
    self,
    *,
    params: Optional[dict] = None,
    force: bool = False,
    progress: bool = False,
    workers: Optional[int] = None,
    progress_cb: Optional[Callable[[str, str, str], None]] = None,
) -> RunSummary
```

The single entry point that actually executes a pipeline.

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `params` | `dict \| None` | `None` | Run-level parameters, validated against the pipeline's `params_model` if one is declared. |
| `force` | `bool` | `False` | Re-executes every lane regardless of cache state. |
| `progress` | `bool` | `False` | Prints a live terminal progress display (`TerminalProgress`) while running. |
| `workers` | `int \| None` | `None` | Overrides every step's `workers=` for this run. |
| `progress_cb` | `Callable[[str, str, str], None] \| None` | `None` | Called as `(step_name, coordinate, action)` for every resolved cell — a lower-level hook than `progress=True`. |

Returns a `RunSummary` (see below).

```python
summary = p.run(params={"min_lines": 5})
print(f"created={summary.created_count} reused={summary.reused_count}")
```

### `Pipeline.plan()`

```python
def plan(self, *, params: Optional[dict] = None, force: bool = False) -> RunPlan
```

A read-only dry-run: tells you what `p.run()` would do to every lane, and
why — `reuse`, `execute`, `blocked`, `filtered`, or `pending` (a dependent
lane whose address can't be known until an upstream execution actually
happens) — without writing anything to the ledger or object store.

```python
print(p.plan())
```

```text
Plan for 'count-lines' over folder:input: 4 execute, 4 pending
  execute  read_lines           file2.txt @ 2db729839948
  ...
```

### `RunPlan`

```python
@dataclass
class RunPlan:
    pipeline_id: str
    source_id: str
    items: List[PlannedCoordinate]   # coordinate, step_name, action, output_address
    counts: Dict[str, int]           # action -> count
    warnings: List[str]              # e.g. code-drift notices
```

`str(plan_result)` renders the human-readable report shown above;
`plan_result.counts` gives programmatic access to the action tally.

### `Pipeline.describe()`

```python
def describe(self, format: Optional[str] = None) -> str
```

Renders a pipeline's DAG before it's ever run — no ledger access at all.
`format=None` (the default) autodetects: `"ascii"` when stdout is a real
terminal, `"text"` otherwise (pipes, captures, redirects — so `pytest`
output and `p.describe()` piped to a file are unaffected). Pass `format=`
explicitly to always win over autodetection. `format="text"` prints each
step in dependency order with its policies; `format="mermaid"` emits a
Mermaid graph for markdown viewers; `format="ascii"` draws topo-layered
boxes joined by unicode box-drawing edges — legible up to ~20 steps, right
in a terminal. Not graphviz-quality (naive edge crossings are allowed); if
a layer is too wide to draw legibly it falls back to `format="text"` for
that graph.

```python
print(p.describe())
print(p.describe(format="mermaid"))
print(p.describe(format="ascii"))
```

### `Pipeline.definition()`

```python
def definition(self) -> Dict[str, Any]
```

The JSON-safe snapshot of this pipeline's structure and policies — exactly
what gets recorded on every `Run` row (`Run.definition_json`) and rendered
by `.describe()`.

## `trace()`

```python
def trace(
    selection: Selection,
    *,
    include_superseded: bool = False,
    resolve_roots: bool = True,
    home: Optional[str] = None,
) -> TraceResult
```

Follows recorded lineage (`MaterializationEdge`) up and down from whatever a
`Selection` matches: upstream to the source items everything came from
(lineage roots show their stored payload when `resolve_roots=True`),
downstream to everything derived from it. By default only *live*
materializations seed the trace; `include_superseded=True` also seeds
non-live generations (superseded or invalidated) — traversal always follows
the real edges either way, so a live output's recorded parent may show up
marked non-live rather than hidden.

```python
from rubedo import Selection, trace

print(trace(Selection.parse("company:acme")))
```

```text
Trace: 2 seed, 3 upstream, 1 downstream
  ...
```

### `TraceResult` / `TraceNode`

```python
@dataclass
class TraceNode:
    materialization_id: int
    step_name: str
    pipeline_id: str
    coordinate: Optional[str]
    output_address: str
    is_live: bool
    filtered: bool
    relation: str   # "seed" | "upstream" | "downstream"
    depth: int
    root_value: Any = None

@dataclass
class TraceResult:
    nodes: List[TraceNode]
    edges: List[Tuple[int, int]]   # (parent_materialization_id, child_materialization_id)

    @property
    def seeds(self) -> List[TraceNode]: ...
    def by_step(self) -> Dict[str, List[TraceNode]]: ...
```

## `invalidate()`

```python
def invalidate(selection: Selection, reason: str, downstream: bool = False) -> dict
```

Flips matching *live* materializations to `is_live=False`, appending a
`materialization_lifecycle` row (`action="invalidated"`) for each — a
logical tombstone, never a delete. `reason` is required and recorded.
`downstream=True` widens the tombstone to the full downstream closure over
`MaterializationEdge` (everything *derived* from the selection's live
matches) — preview the blast radius first with `trace()` on the same
selection, since a `reduce`/`join` inside that closure honestly carries
everything after it too.

```python
from rubedo import Selection, invalidate

invalidate(Selection(index={"company": "acme"}), reason="bad prompt")
```

Returns a dict: `run_id`, `invalidated_count`, `seed_count`,
`downstream_count`, `materialization_ids`. The next `p.run()` recomputes
exactly the invalidated lanes (plus, if it wasn't already invalidated
explicitly, anything genuinely downstream of them through the normal
planning process).

## `Selection` / `Selection.parse()`

```python
class Selection(BaseModel):
    source_id: Optional[str] = None
    coordinate_glob: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    version_range: Optional[str] = None
    output_address: Optional[str] = None
    invalidated: Optional[bool] = None
    pipeline_id: Optional[str] = None
    index: Optional[Dict[str, str]] = None

    @classmethod
    def parse(cls, query: str) -> "Selection": ...
```

Criteria for selecting materializations, constructed directly or parsed
from the query-string language shared by Python, the CLI, and the web UI.
`Selection.parse()` splits on whitespace (`shlex`-aware, so
`company:"acme corp"` quotes a value with spaces) into `key:value` terms:

| Prefix | Selection field | Example |
|---|---|---|
| `source:` | `source_id` | `source:folder:input` |
| `coord:` / `coordinate:` | `coordinate_glob` | `coord:*.txt` |
| `step:` | `step` | `step:classify` |
| `pipeline:` | `pipeline_id` | `pipeline:reviews` |
| `version:<exact>` | `code_version` | `version:v2` |
| `version:<range>` (starts with `<`, `>`, `=`, `!`) | `version_range` (PEP 440 `SpecifierSet`) | `version:<2.0` |
| `address:` | `output_address` | `address:88ca616d68...` |
| `live:true`\|`false` | `invalidated` (inverted) | `live:false` |
| anything else | `index[key]` | `company:acme` |

Any term that isn't a reserved prefix matches an indexed output field
(`@step(index=[...])`) — indexed data is the language's open vocabulary.

```python
from rubedo import Selection

Selection.parse("step:extract company:acme live:true")
Selection(index={"company": "acme"})
```

See [Guide: search and invalidation](../guides/search-and-invalidation.md)
for the full query language and CLI equivalents.

## `ProcessResult`

```python
class ProcessResult(BaseModel):
    value: Any
    metadata: Optional[Dict[str, Any]] = None
```

The successful output of a step, carrying the value that downstream steps
receive plus optional metadata that rides along in the ledger but isn't
passed to consumers. A step may also return a plain JSON-serializable value
directly instead of wrapping it — `ProcessResult` is for when you want to
attach metadata alongside the value.

```python
from rubedo import ProcessResult

@step
def count_lines(read_lines: dict) -> ProcessResult:
    return ProcessResult(
        value={"line_count": len(read_lines["lines"])},
        metadata={"source": "count_lines step"},
    )
```

## `Filtered`

```python
class Filtered:
    def __init__(self, reason: Optional[str] = None): ...
```

Return this from a step to decline a coordinate. The decision is cached
like any other output (a filtered materialization), and downstream steps
skip that coordinate with status `"filtered"` instead of executing — an
expensive LLM-based filter runs once per input, not once per run.

```python
from rubedo import Filtered

@step
def screen(top_story: dict):
    if top_story["score"] < 50:
        return Filtered(reason="score below threshold")
    return top_story
```

## `RunSummary`

```python
class RunSummary(BaseModel):
    run_id: str
    status: str
    created_count: int = 0
    reused_count: int = 0
    failed_count: int = 0
    blocked_count: int = 0
    filtered_count: int = 0

    def failures(self) -> list[Dict[str, Any]]: ...
    def output_for(self, step_name: str) -> dict[str, Any]: ...
```

Returned by `Pipeline.run()`. `failures()` re-queries the ledger for this run's
failed coordinates and errors. `output_for(step_name)` re-reads every
materialization this run created, reused, or produced a filtered verdict
for at that step, keyed by coordinate — the values a step actually saw or
produced, without needing your own query.

```python
summary = p.run()
print(json.dumps(summary.output_for("total_lines"), indent=2, default=str))
```

## Sources

There is no `Source` class or protocol to implement — ingestion is a
parentless `@step(shape="expand")` (its shape inferred automatically from
a bare generator function; see [Shape and `depends_on` inference](#shape-and-depends_on-inference)
above), and every lane it yields is
content-addressed (`row-<hash>`): identical payloads collapse to one lane,
and an edited item reads as removed + created, so incrementality survives
reordering, dedup, and appends for free. To find or track an item by a
human field (email, id, file name), `@step(index=[...])` it and query — the
coordinate is never a human key.

See [Concepts: sources](../concepts/sources.md) for the folder, CSV, SQL
table, and cloud object storage recipes.

## `gc()`

```python
# from rubedo.gc import gc
def gc(
    delete: bool = False,
    max_bytes: Optional[int] = None,
    home: Optional[str] = None,
) -> GcReport
```

Applies every pipeline's `retention=N` policy, then — if `max_bytes` is
given — prunes oldest-run-first across pipelines until the store fits under
budget. Dry-run by default (`delete=False`): nothing is written or deleted,
and the returned report lists exactly what `delete=True` would do. With
`delete=True`, GC refuses (returns a `GcReport` with `.refused` set, applies
nothing) while any run's heartbeat is live — a concurrent run could be
committing an output that points at bytes GC is about to remove. This is
also what `rubedo gc [--max-bytes] [--delete]` calls. See
[Guide: retention](../guides/retention.md).

```python
from rubedo.gc import gc

report = gc(max_bytes=2 * 1024**3)          # dry-run against a 2 GiB budget
print(report)
applied = gc(max_bytes=2 * 1024**3, delete=True)
```

`GcReport` carries `applied`, `demoted_mat_ids`, `reclaimed` (list of
`(content_hash, bytes)`), `refused`, `max_bytes`, and
`total_bytes_before`, plus `.reclaimed_bytes`/`.demoted_count` properties
and a human-readable `__str__`.

## `storage_report()`

```python
# from rubedo.du import storage_report
def storage_report(home: Optional[str] = None) -> StorageReport
```

A read-only accounting of what the object store holds, computed entirely
from the ledger (never by enumerating `objects/` on disk) — total object
count and bytes, a per-pipeline/per-step breakdown (objects deduped within
each scope, since the store is content-addressed and shared), a
reclaimable estimate (objects with zero live references anywhere), and
missing-vs-reclaimed accounting for absent files. Nothing is ever deleted —
this is what `rubedo du [--json]` calls.

```python
from rubedo.du import storage_report

report = storage_report()
print(report)                 # human-readable
print(report.to_dict())       # JSON-safe dict
```

## See also

- [Concepts: the model](../concepts/model.md) — the vocabulary (lane,
  coordinate, address, materialization) these signatures assume.
- [Concepts: shapes](../concepts/shapes.md) — `map`/`reduce`/`expand`/`join`
  in depth, with worked examples.
- [Concepts: sources](../concepts/sources.md) — writing a custom `Source`.
- [Concepts: versioning](../concepts/versioning.md) — `version` vs. `code`.
- [Guide: execution policies](../guides/execution-policies.md) — retries,
  rate limits, `stale_after`, assertions, executors, in depth.
- [Guide: search and invalidation](../guides/search-and-invalidation.md) —
  the full `Selection` language and CLI equivalents.
- [Guide: inspecting runs](../guides/inspecting-runs.md) — `p.plan()`,
  `trace()`, the CLI, the dashboard.
- [Guide: retention](../guides/retention.md) — `retention=N`, `rubedo gc`,
  the demote/sweep model.
- [CLI reference](cli.md) — every `rubedo` subcommand.
