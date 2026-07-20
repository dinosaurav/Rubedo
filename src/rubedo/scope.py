"""Partial-execution scope: a frozen lane cohort at one map-shaped boundary.

A ``RunScope`` names an anchor step and an exact set of lane coordinates.
It controls *which* cells are requested — never cache/input/output identity.
Deterministic sampling helpers construct scopes; ``origin`` metadata is
diagnostic only (persisted for reproducibility, never hashed).

MVP anchors are non-root ``in_shape="one"`` / ``out_shape="one"`` map steps.
``skip_cache`` anchors are rejected: those steps are never materialized or
recorded on ``RunCoordinateStatus``, so a cohort anchored there would be
invisible in the ledger and unsafe to treat as a durable experiment boundary.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

from .hashing import hash_text
from .spec import PipelineSpec, StepSpec

StepRef = Union[str, StepSpec, Any]
LaneRef = Union[str, Any]  # coordinate str or Cell-like with .coordinate


def resolve_step_name(step_ref: StepRef) -> str:
    """Accept a step name, ``StepSpec``, or decorated callable ergonomically."""
    if isinstance(step_ref, str):
        if not step_ref:
            raise ValueError("step name must be a non-empty string")
        return step_ref
    if isinstance(step_ref, StepSpec):
        return step_ref.name
    name = getattr(step_ref, "name", None)
    if isinstance(name, str) and name:
        return name
    raise TypeError(
        f"expected step name, StepSpec, or step callable, got {type(step_ref)!r}"
    )


def _coordinate_of(lane: LaneRef) -> str:
    if isinstance(lane, str):
        if not lane:
            raise ValueError("lane coordinate must be a non-empty string")
        return lane
    coord = getattr(lane, "coordinate", None)
    if isinstance(coord, str) and coord:
        return coord
    raise TypeError(
        f"expected lane coordinate str or Cell-like with .coordinate, got {type(lane)!r}"
    )


def _coords_from(lanes: Optional[Iterable[LaneRef]] = None, cells: Optional[Iterable[LaneRef]] = None) -> list[str]:
    if lanes is not None and cells is not None:
        raise ValueError("pass lanes= or cells=, not both")
    src = lanes if lanes is not None else cells
    if src is None:
        raise ValueError("lanes= or cells= is required")
    out = [_coordinate_of(x) for x in src]
    # Preserve first-seen order after de-dupe for stable sampling input.
    seen: set[str] = set()
    uniq: list[str] = []
    for c in out:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def _lane_rank(seed: str, coordinate: str) -> int:
    """Stable rank in [0, 2^256): hash(seed, coordinate). Never part of cache identity."""
    return int(hash_text(f"{seed}\0{coordinate}"), 16)


_HASH_SPACE = 1 << 256


def sample_n_coordinates(
    coordinates: Sequence[str], *, n: int, seed: str
) -> list[str]:
    """Exact-N sample by ascending stable ``hash(seed, coordinate)`` (tie-break by coord)."""
    if n < 0:
        raise ValueError(f"n must be >= 0, got {n}")
    if not isinstance(seed, str) or seed == "":
        raise ValueError("seed must be a non-empty string")
    ranked = sorted(coordinates, key=lambda c: (_lane_rank(seed, c), c))
    return ranked[:n]


def sample_fraction_coordinates(
    coordinates: Sequence[str], *, fraction: float, seed: str
) -> list[str]:
    """Hash-threshold fraction sample: include when ``rank / 2^256 < fraction``.

    Same seed yields nested cohorts (smaller fraction ⊆ larger fraction).
    """
    if not (0.0 <= fraction <= 1.0):
        raise ValueError(f"fraction must be in [0, 1], got {fraction}")
    if not isinstance(seed, str) or seed == "":
        raise ValueError("seed must be a non-empty string")
    threshold = fraction * _HASH_SPACE
    picked = [c for c in coordinates if _lane_rank(seed, c) < threshold]
    return sorted(picked)


@dataclass(frozen=True)
class RunScope:
    """Frozen exact lane cohort at one map-shaped anchor step.

    ``lanes`` is the authoritative membership; ``origin`` is diagnostic only.
    """

    anchor: str
    lanes: frozenset[str]
    origin: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        if not self.anchor:
            raise ValueError("RunScope.anchor must be a non-empty string")
        object.__setattr__(self, "anchor", resolve_step_name(self.anchor))
        cleaned = frozenset(_coordinate_of(c) for c in self.lanes)
        object.__setattr__(self, "lanes", cleaned)
        if self.origin is not None and not isinstance(self.origin, Mapping):
            raise TypeError("RunScope.origin must be a mapping or None")

    @property
    def lane_list(self) -> list[str]:
        """Sorted coordinates — stable for persistence and comparisons."""
        return sorted(self.lanes)

    @classmethod
    def explicit(
        cls,
        anchor: StepRef,
        lanes: Iterable[LaneRef],
        *,
        origin: Optional[Mapping[str, Any]] = None,
    ) -> "RunScope":
        """Build a scope from an exact coordinate set."""
        return cls(
            anchor=resolve_step_name(anchor),
            lanes=frozenset(_coords_from(lanes=lanes)),
            origin=dict(origin) if origin is not None else None,
        )

    @classmethod
    def from_cells(
        cls,
        anchor: StepRef,
        cells: Iterable[LaneRef],
        *,
        origin: Optional[Mapping[str, Any]] = None,
    ) -> "RunScope":
        """Build a scope from Cell-like objects (or coordinate strings)."""
        coords = _coords_from(cells=cells)
        meta = dict(origin) if origin is not None else {"strategy": "explicit"}
        return cls(
            anchor=resolve_step_name(anchor),
            lanes=frozenset(coords),
            origin=meta,
        )

    @classmethod
    def sample_n(
        cls,
        anchor: StepRef,
        *,
        n: int,
        seed: str,
        lanes: Optional[Iterable[LaneRef]] = None,
        cells: Optional[Iterable[LaneRef]] = None,
        origin: Optional[Mapping[str, Any]] = None,
    ) -> "RunScope":
        """Exact-N deterministic sample over a candidate coordinate universe."""
        universe = _coords_from(lanes=lanes, cells=cells)
        picked = sample_n_coordinates(universe, n=n, seed=seed)
        meta = {
            "strategy": "sample_n",
            "n": n,
            "seed": seed,
            "universe_size": len(universe),
        }
        if origin:
            meta.update(dict(origin))
        return cls(
            anchor=resolve_step_name(anchor),
            lanes=frozenset(picked),
            origin=meta,
        )

    @classmethod
    def sample_fraction(
        cls,
        anchor: StepRef,
        *,
        fraction: float,
        seed: str,
        lanes: Optional[Iterable[LaneRef]] = None,
        cells: Optional[Iterable[LaneRef]] = None,
        origin: Optional[Mapping[str, Any]] = None,
    ) -> "RunScope":
        """Hash-threshold fraction sample (nested cohorts under the same seed)."""
        universe = _coords_from(lanes=lanes, cells=cells)
        picked = sample_fraction_coordinates(
            universe, fraction=fraction, seed=seed
        )
        meta = {
            "strategy": "sample_fraction",
            "fraction": fraction,
            "seed": seed,
            "universe_size": len(universe),
        }
        if origin:
            meta.update(dict(origin))
        return cls(
            anchor=resolve_step_name(anchor),
            lanes=frozenset(picked),
            origin=meta,
        )

    def to_invocation_dict(
        self, targets: Optional[Sequence[str]] = None
    ) -> dict[str, Any]:
        """JSON-safe invocation snapshot for ``Run.selection_json``."""
        payload: dict[str, Any] = {
            "type": "run_scope",
            "anchor": self.anchor,
            "lanes": self.lane_list,
        }
        if targets is not None:
            payload["targets"] = list(targets)
        if self.origin is not None:
            payload["origin"] = dict(self.origin)
        return payload


def invocation_selection_json(
    scope: Optional[RunScope],
    targets: Optional[Sequence[str]],
) -> Optional[str]:
    """Serialize scope/targets for ``Run.selection_json`` (None when full run)."""
    if scope is None and not targets:
        return None
    if scope is not None:
        return json.dumps(scope.to_invocation_dict(targets), sort_keys=True)
    return json.dumps(
        {"type": "run_scope", "targets": list(targets or [])},
        sort_keys=True,
    )


def ancestors_of(pipeline: PipelineSpec, step_name: str) -> set[str]:
    """``step_name`` plus every transitive dependency (ancestor closure)."""
    by_name = {s.name: s for s in pipeline.steps}
    if step_name not in by_name:
        raise ValueError(f"unknown step {step_name!r}")
    out: set[str] = set()
    stack = [step_name]
    while stack:
        cur = stack.pop()
        if cur in out:
            continue
        out.add(cur)
        stack.extend(by_name[cur].depends_on)
    return out


def descendants_of(pipeline: PipelineSpec, step_name: str) -> set[str]:
    """``step_name`` plus every transitive consumer."""
    children: dict[str, list[str]] = {s.name: [] for s in pipeline.steps}
    for s in pipeline.steps:
        for dep in s.depends_on:
            children.setdefault(dep, []).append(s.name)
    out: set[str] = set()
    stack = [step_name]
    while stack:
        cur = stack.pop()
        if cur in out:
            continue
        out.add(cur)
        stack.extend(children.get(cur, []))
    return out


def coordinate_preserving_scope_steps(
    pipeline: PipelineSpec, anchor: str
) -> set[str]:
    """Map-shaped descendants that retain the anchor's coordinate namespace.

    Broad scheduling plans each step from every parent coordinate. Restricting
    only the anchor would therefore make an aligned multi-parent map see a
    sampled parent beside an unsampled sibling and report disjoint lanes.
    Carry the cohort through map-shaped descendants until a join/expand/
    aggregate/fold mints or collapses coordinates; after that boundary the
    produced coordinates themselves delimit downstream work.
    """
    children: dict[str, list[StepSpec]] = {s.name: [] for s in pipeline.steps}
    for step in pipeline.steps:
        for dep in step.depends_on:
            children.setdefault(dep, []).append(step)

    scoped = {anchor}
    stack = [anchor]
    while stack:
        parent = stack.pop()
        for child in children.get(parent, []):
            if child.in_shape != "one" or child.out_shape != "one":
                continue
            if child.name not in scoped:
                scoped.add(child.name)
                stack.append(child.name)
    return scoped


def target_ancestor_closure(
    pipeline: PipelineSpec, targets: Sequence[str]
) -> set[str]:
    """Union of ancestor closures for every target step."""
    closure: set[str] = set()
    for t in targets:
        closure |= ancestors_of(pipeline, t)
    return closure


@dataclass
class ResolvedInvocation:
    """Normalized scope/targets ready for plan/run (never enters cache identity)."""

    scope: Optional[RunScope] = None
    targets: Optional[list[str]] = None
    # Steps that will be planned/executed (None = full pipeline topo).
    active_steps: Optional[set[str]] = None
    is_partial: bool = False


def normalize_partial_invocation(
    pipeline: PipelineSpec,
    scope: Optional[RunScope] = None,
    targets: Optional[Sequence[StepRef]] = None,
) -> ResolvedInvocation:
    """Validate and normalize ``scope`` / ``targets`` against ``pipeline``.

    Raises ``ValueError`` for MVP-illegal anchors or inconsistent targets.
    """
    by_name = {s.name: s for s in pipeline.steps}
    resolved_targets: Optional[list[str]] = None
    if targets is not None:
        resolved_targets = [resolve_step_name(t) for t in targets]
        if not resolved_targets:
            raise ValueError("targets= must be a non-empty list when provided")
        unknown = [t for t in resolved_targets if t not in by_name]
        if unknown:
            raise ValueError(
                f"unknown target step(s): {unknown}; "
                f"available: {sorted(by_name)}"
            )
        # Stable unique order preserving first occurrence.
        seen: set[str] = set()
        uniq: list[str] = []
        for t in resolved_targets:
            if t not in seen:
                seen.add(t)
                uniq.append(t)
        resolved_targets = uniq

    resolved_scope = scope
    if resolved_scope is not None:
        if not isinstance(resolved_scope, RunScope):
            raise TypeError(
                f"scope= must be a RunScope, got {type(resolved_scope)!r}"
            )
        # Re-normalize anchor in case it was constructed before steps existed.
        anchor_name = resolve_step_name(resolved_scope.anchor)
        if anchor_name not in by_name:
            raise ValueError(
                f"unknown scope anchor {anchor_name!r}; "
                f"available: {sorted(by_name)}"
            )
        step = by_name[anchor_name]
        if not step.depends_on:
            raise ValueError(
                f"scope anchor '{anchor_name}' is a root — MVP anchors must be "
                "non-root map steps (in_shape='one', out_shape='one'); "
                "sample historical child lanes but anchor at the first "
                "downstream map"
            )
        if step.in_shape != "one" or step.out_shape != "one":
            raise ValueError(
                f"scope anchor '{anchor_name}' has "
                f"in_shape={step.in_shape!r}, out_shape={step.out_shape!r}; "
                "MVP only permits map anchors (in_shape='one', out_shape='one'). "
                "Reject aggregate/fold/join/expand anchors — they mint different "
                "coordinate namespaces; they may still appear *downstream* of an "
                "anchor"
            )
        if step.skip_cache:
            raise ValueError(
                f"scope anchor '{anchor_name}' is skip_cache — rejected. "
                "skip_cache steps are never materialized or recorded on "
                "RunCoordinateStatus, so a cohort anchored there would be "
                "invisible in the ledger and unsafe as a durable experiment "
                "boundary; anchor at a materialized map step instead"
            )
        if anchor_name != resolved_scope.anchor:
            resolved_scope = RunScope(
                anchor=anchor_name,
                lanes=resolved_scope.lanes,
                origin=resolved_scope.origin,
            )

    if resolved_scope is not None and resolved_targets is not None:
        anchor = resolved_scope.anchor
        desc = descendants_of(pipeline, anchor)
        bad = [t for t in resolved_targets if t not in desc]
        if bad:
            raise ValueError(
                f"targets {bad} are not the anchor '{anchor}' or its "
                f"descendants; targets must lie in the anchor's downstream "
                f"subgraph (allowed: {sorted(desc)})"
            )
        closure = target_ancestor_closure(pipeline, resolved_targets)
        if anchor not in closure:
            raise ValueError(
                f"scope anchor '{anchor}' is not in the ancestor subgraph of "
                f"targets {resolved_targets}"
            )

    active: Optional[set[str]] = None
    if resolved_targets is not None:
        active = target_ancestor_closure(pipeline, resolved_targets)

    is_partial = resolved_scope is not None or resolved_targets is not None
    return ResolvedInvocation(
        scope=resolved_scope,
        targets=resolved_targets,
        active_steps=active,
        is_partial=is_partial,
    )


@dataclass
class ScopeCounts:
    """Requested / reached / missing lane tallies for a partial run or plan."""

    requested: int = 0
    reached: int = 0
    missing: int = 0
    missing_lanes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "scope_requested": self.requested,
            "scope_reached": self.reached,
            "scope_missing": self.missing,
            "missing_lanes": list(self.missing_lanes),
        }
