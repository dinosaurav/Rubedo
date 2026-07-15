"""Pipeline: one object, verbs as methods.

Sits *above* the engine — imports runner.py (which imports scheduler.py,
planning.py, execution.py, ledger.py) — so spec.py stays a pure-data leaf
that never imports this module or runner.py (if you feel a lazy import
coming here, the code is in the wrong module). All ledger writes still
happen on the main thread; this module only orchestrates (see runner.py
and scheduler.py for where that rule actually lives).

`pipeline(name=...)` returns a `Pipeline`; steps register via `@p.step`
(decorator) or the `steps=[...]` kwarg — both stay. There is
no `.build()`: the underlying `PipelineSpec` is constructed and validated
lazily, on first access to a verb (`.run()`/`.plan()`/`.describe()`/
`.definition()`), and cached — registering more steps after that first
verb call invalidates the cache so it rebuilds on next access.
"""
import inspect
from dataclasses import replace
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Type

from pydantic import BaseModel

from .models import RunSummary
from .render import describe as _describe
from .runner import RunPlan
from .runner import plan as _plan_pipeline
from .runner import run as _run_pipeline
from .scheduler import SCHEDULES
from .spec import PipelineSpec, StepSpec
from .spec import definition as _definition
from .spec import step as _step_decorator

# Reserved for the engine's own env vars (RUBEDO_HOME, RUBEDO_DB_PATH, ...):
# see store.py/db.py. secrets=/env= may not declare a RUBEDO_*-prefixed name.
_RESERVED_ENV_PREFIX = "RUBEDO_"


def _validate_env_declarations(
    pipeline_name: str, secrets: Sequence[str], env: Sequence[str]
) -> None:
    """secrets=/env= are step-independent (they don't read the accumulated
    step list), so — like schedule=/retention= — they're validated eagerly
    at construction rather than waiting for the first verb call.

    Rules: every name is a non-empty string; names are unique across the
    *combined* list (this also covers "no overlap between the two lists" —
    a name in both is a duplicate of itself); no name may start with
    RUBEDO_ (reserved for the engine's own env vars).
    """
    combined = list(secrets) + list(env)
    for n in combined:
        if not n:
            raise ValueError(
                f"pipeline '{pipeline_name}': secrets=/env= names must be "
                "non-empty strings"
            )
        if n.startswith(_RESERVED_ENV_PREFIX):
            raise ValueError(
                f"pipeline '{pipeline_name}': {n!r} is reserved — RUBEDO_* "
                "env vars are the engine's own and can't be declared in "
                "secrets=/env="
            )
    dupes = sorted({n for n in combined if combined.count(n) > 1})
    if dupes:
        raise ValueError(
            f"pipeline '{pipeline_name}': secrets=/env= names must be unique "
            f"(and not appear in both lists), duplicated: {dupes}"
        )


def _infer_depends_on(s: StepSpec, step_names: Set[str]) -> List[str]:
    """Signature -> depends_on for a step declared with no explicit
    `depends_on=` — the parameter list *is* the dependency declaration
    (parents already bind as kwargs keyed by step name, execution.py), in
    signature order. `params` is reserved and never a dependency. A
    signature with `*args`/`**kwargs` is ambiguous — skip inference
    entirely (an empty result, i.e. a root; pass `depends_on=` explicitly
    if such a step actually has parents). A step with no non-`params`
    parameter is a root, cleanly, with no special-casing.
    """
    deps: List[str] = []
    for param in inspect.signature(s.fn).parameters.values():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            return []
        if param.name == "params":
            continue
        if param.name not in step_names:
            raise ValueError(
                f"Step '{s.name}': parameter '{param.name}' doesn't match any "
                f"registered step name (available: {sorted(step_names)}) — "
                "pass depends_on= explicitly if this parameter isn't meant "
                "as a dependency"
            )
        deps.append(param.name)
    return deps


def _step_origin(s: StepSpec) -> str:
    """Where a step's function was defined, for duplicate-name errors —
    `module.qualname`, falling back to the step's own name if `fn` somehow
    lacks one (e.g. a wrapped callable)."""
    fn = s.fn
    module = getattr(fn, "__module__", None) or "?"
    qualname = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", s.name)
    return f"{module}.{qualname}"


def _build_spec(
    name: str,
    steps: List[StepSpec],
    params_model: Optional[Type[BaseModel]],
    retention: Optional[int],
    secrets: Tuple[str, ...] = (),
    env: Tuple[str, ...] = (),
) -> PipelineSpec:
    """Validate the accumulated steps and construct the PipelineSpec.

    Ingestion is not a separate concept: the roots (steps with no
    `depends_on`) *are* the source. A `shape="expand"` root (a parentless
    generator function, shape inferred automatically) yields the initial
    lanes and re-runs every run; a `shape="map"` root
    mints a single lane whose input is its params (or a constant when it
    takes none) — same params reuse, changed params recompute. A pipeline
    may declare several roots; `join` doesn't care that its parents are
    roots.

    retention=N keeps only this pipeline's last N terminal runs' outputs: at
    the end of a successful run (or on `rubedo gc`), generations that only
    older runs referenced are pruned — their liveness flipped off and, once
    no live output anywhere references the bytes, the object deleted. None
    (default) keeps everything. Set-and-forget storage hygiene for
    long-lived pipelines.

    retention itself is validated eagerly in `Pipeline.__init__` (it doesn't
    depend on the accumulated step list, so it fails fast at construction
    rather than waiting for the first verb call). secrets=/env= are
    likewise validated eagerly in `Pipeline.__init__`
    (`_validate_env_declarations`) for the same reason — by the time
    `_build_spec` runs, they're already-validated tuples.

    Duplicate step names are checked here rather than only deep in
    `topological_sort` (planning.py keeps its own copy of this check too, as
    a backstop for anyone building a `PipelineSpec` directly): with `@step`'s
    name defaulting to the function name, two steps built from
    same-named functions in different modules is the realistic collision, so
    the error names both functions' `module.qualname` — not just the shared
    step name — so it's obvious *where* the collision came from.
    """
    seen: Dict[str, StepSpec] = {}
    for s in steps:
        prior = seen.get(s.name)
        if prior is not None:
            raise ValueError(
                f"Duplicate step name {s.name!r}: defined by both "
                f"{_step_origin(prior)} and {_step_origin(s)} — pass name= "
                "to one of them to disambiguate"
            )
        seen[s.name] = s

    # Param-name -> depends_on inference: only now, with every sibling
    # step's name known, can an omitted depends_on= be resolved. Builds a
    # fresh, resolved step list rather than mutating the StepSpec objects
    # the decorators handed back to the caller (which may be reused
    # elsewhere). Steps with an explicit depends_on= (list or dict alias
    # form) pass through untouched.
    step_names = set(seen)
    steps = [
        s if s.depends_on_explicit else replace(s, depends_on=_infer_depends_on(s, step_names))
        for s in steps
    ]
    seen = {s.name: s for s in steps}

    roots = [s for s in steps if not s.depends_on]
    if not roots:
        raise ValueError("pipeline has no root step to originate lanes")

    consumed = {dep for s in steps for dep in s.depends_on}
    name_to_step = seen  # already validated unique above
    for s in steps:
        if s.skip_cache and s.name not in consumed:
            raise ValueError(
                f"Step '{s.name}' has skip_cache but no consumer: its output "
                "would never be computed or stored"
            )
        if s.shape == "join":
            for dep in s.join_on or {}:
                parent = name_to_step.get(dep)
                if parent and parent.skip_cache:
                    raise ValueError(
                        f"Step '{s.name}': shape='join' cannot have a skip_cache parent ('{dep}')"
                    )
        if s.shape == "reduce" and s.group_key is not None:
            for dep in s.depends_on:
                parent = name_to_step.get(dep)
                if parent and parent.skip_cache:
                    raise ValueError(
                        f"Step '{s.name}': group_key requires materialized parents, but '{dep}' is skip_cache"
                    )

    return PipelineSpec(
        name=name,
        steps=steps,
        params_model=params_model,
        retention=retention,
        secrets=secrets,
        env=env,
    )


class Pipeline:
    """A pipeline: register steps, then call a verb.

    Construct with `pipeline(name=...)`. `name` is the pipeline's sole
    identity (there is no separate `id`) — the ledger's `pipeline_id`
    column stores it verbatim; renaming a pipeline orphans its history.

    Settings that apply to every run of this pipeline live here at
    construction (`schedule=`, `home=`, alongside `retention=` and
    `params_model=`); `run()`/`plan()` keep only per-invocation things.

    secrets=/env= declare the pipeline's environment surface — executable
    documentation of what it needs to run, and (for a future cloud worker)
    what to inject at deploy time. secrets= names vault-injected, log-masked
    values (API keys); env= names deploy-config-injected, visible values
    (log levels). Locally both still come from the shell/`.env` exactly as
    before — the declaration changes nothing about execution, and neither
    list enters any step's cache identity. `rubedo check <file.py>` lints a
    pipeline's step bodies for `os.environ`/`os.getenv` reads that aren't
    declared in either list (advisory only, never blocks).
    """

    def __init__(
        self,
        name: str,
        steps: Optional[List[StepSpec]] = None,
        params_model: Optional[Type[BaseModel]] = None,
        retention: Optional[int] = None,
        schedule: str = "broad",
        home: Optional[str] = None,
        secrets: Optional[List[str]] = None,
        env: Optional[List[str]] = None,
    ):
        if schedule not in SCHEDULES:
            raise ValueError(f"schedule must be one of {SCHEDULES}, got {schedule!r}")
        if retention is not None and (
            isinstance(retention, bool) or not isinstance(retention, int) or retention < 1
        ):
            raise ValueError(
                f"pipeline '{name}': retention must be an integer >= 1 (runs to keep), "
                f"got {retention!r}"
            )
        secrets_t: Tuple[str, ...] = tuple(secrets or [])
        env_t: Tuple[str, ...] = tuple(env or [])
        _validate_env_declarations(name, secrets_t, env_t)
        self.name = name
        self.params_model = params_model
        self.retention = retention
        self.schedule = schedule
        self.home = home
        self.secrets = secrets_t
        self.env = env_t
        self._steps: List[StepSpec] = list(steps or [])
        self._spec: Optional[PipelineSpec] = None

    def step(self, *args, **kwargs):
        """Decorate a function to register it as a step on this pipeline
        (see `rubedo.step` for the policy kwargs). Works bare (`@p.step`)
        or called (`@p.step(...)`), like `rubedo.step`."""

        def decorator(fn):
            s = _step_decorator(**kwargs)(fn)
            self._steps.append(s)
            self._spec = None
            return s

        if len(args) == 1 and callable(args[0]) and not kwargs:
            return decorator(args[0])
        if args:
            raise TypeError(
                "p.step takes keyword arguments only (or a bare function): "
                "@p.step / @p.step(version='2')"
            )
        return decorator

    @property
    def spec(self) -> PipelineSpec:
        """The validated `PipelineSpec`, built lazily on first access from
        the steps registered so far, and cached."""
        if self._spec is None:
            self._spec = _build_spec(
                self.name,
                self._steps,
                self.params_model,
                self.retention,
                self.secrets,
                self.env,
            )
        return self._spec

    def run(
        self,
        *,
        params: Optional[dict] = None,
        force: bool = False,
        progress: bool = False,
        workers: Optional[int] = None,
        progress_cb: Optional[Callable[[str, str, str], None]] = None,
    ) -> RunSummary:
        """Run this pipeline — the single entry point.

        Params are validated against `params_model` whenever one is
        declared. `force=True` re-executes every cell regardless of cache
        state. `progress=True` prints a live terminal progress view;
        `progress_cb` (step_name, coordinate, status) is a lower-level hook
        for the same events.
        """
        return _run_pipeline(
            self.spec,
            params=params,
            workers=workers,
            force=force,
            home=self.home,
            progress=progress,
            progress_cb=progress_cb,
            schedule=self.schedule,
        )

    def plan(self, *, params: Optional[dict] = None, force: bool = False) -> RunPlan:
        """Dry-run: what would `run()` do, and why — without writing anything."""
        return _plan_pipeline(self.spec, params=params, force=force, home=self.home)

    def describe(self, format: Optional[str] = None) -> str:
        """Render this pipeline's DAG before ever running it.

        format=None (the default) autodetects: "ascii" in a real terminal,
        "text" otherwise (pipes, captures, redirects) — pass format=
        explicitly to always win over autodetection. format="text" prints
        steps in dependency order with their policies; format="mermaid"
        emits a Mermaid graph; format="ascii" draws a terminal DAG.
        """
        return _describe(self.spec, format=format)

    def definition(self) -> Dict[str, Any]:
        """JSON-safe snapshot of this pipeline's structure and policies —
        exactly what gets recorded on every `Run` row."""
        return _definition(self.spec)


def pipeline(
    name: str,
    steps: Optional[List[StepSpec]] = None,
    params_model: Optional[Type[BaseModel]] = None,
    retention: Optional[int] = None,
    schedule: str = "broad",
    home: Optional[str] = None,
    secrets: Optional[List[str]] = None,
    env: Optional[List[str]] = None,
) -> Pipeline:
    """Construct a pipeline. `name` is the only required argument and is
    the pipeline's sole identity.

    Steps attach either via the `steps=[...]` kwarg or by decorating
    functions with the returned object's `@p.step` — both can
    be mixed freely, and validation (at least one root, skip_cache/join/
    group_key consistency) runs lazily on first `.run()`/`.plan()`/
    `.describe()`/`.definition()` call, not here.

    schedule picks the execution order for every run of this pipeline
    (never the results — cache identity is order-independent): "broad"
    (default) completes each step across all lanes before the next one
    starts; "deep" lets each lane race ahead through consecutive 1:1 steps
    as soon as its own inputs commit, while reduce/join (and, for now,
    expand and multi-parent maps) still synchronize on all lanes.

    home, if given, points this pipeline's ledger/object store at a custom
    root instead of the default `.rubedo`/RUBEDO_HOME for every run/plan.

    retention=N keeps only this pipeline's last N terminal runs' outputs
    (see `Pipeline`/`_build_spec` for the full retention semantics).

    secrets=/env= declare this pipeline's environment surface (see
    `Pipeline`'s docstring) — pure documentation locally, validated eagerly
    at construction, and never part of any step's cache identity.
    """
    return Pipeline(
        name=name,
        steps=steps,
        params_model=params_model,
        retention=retention,
        schedule=schedule,
        home=home,
        secrets=secrets,
        env=env,
    )
