"""Read-only run-to-run output comparison.

``Home.diff`` / ``RunSummary.diff`` load ``RunCoordinateStatus`` cells for one
step from two explicit runs and classify each coordinate. No Run / RCS /
event / IHU writes — ledger rows are only read.
"""
from __future__ import annotations

import difflib
import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from typing import Any, Optional, Union, TYPE_CHECKING

from .models import Run, RunSummary
from .queries import Cell, get_run_cells
from .schemas import RunListItem

if TYPE_CHECKING:
    from .home import Home

RunRef = Union[str, RunSummary, RunListItem]

_SUCCESS_STATUSES = frozenset({"created", "reused", "filtered"})
_FAILED_STATUSES = frozenset({"failed", "blocked"})
_OUTCOMES = ("unchanged", "changed", "added", "removed", "failed")


def resolve_run_id(ref: RunRef) -> str:
    """Normalize a run reference to a run-id string.

    Accepts a run-id string, ``RunSummary`` (``.run_id``), or list-result
    object (``RunListItem.id``).
    """
    if isinstance(ref, str):
        if not ref:
            raise ValueError("run id must be a non-empty string")
        return ref
    if isinstance(ref, RunSummary):
        run_id = str(ref.run_id)
        if not run_id:
            raise ValueError("RunSummary.run_id must be a non-empty string")
        return run_id
    if isinstance(ref, RunListItem):
        run_id = str(ref.id)
        if not run_id:
            raise ValueError("RunListItem.id must be a non-empty string")
        return run_id
    raise TypeError(
        "run ref must be a run-id str, RunSummary, or RunListItem; "
        f"got {type(ref)!r}"
    )


@dataclass(frozen=True)
class ValueChange:
    """One field-level (or top-level) value difference."""

    path: str
    outcome: str  # added | removed | changed
    old: Any = None
    new: Any = None
    text_diff: Optional[str] = None

    def __str__(self) -> str:
        label = self.path if self.path else "(value)"
        if self.outcome == "added":
            return f"+ {label}: {self.new!r}"
        if self.outcome == "removed":
            return f"- {label}: {self.old!r}"
        if self.text_diff is not None:
            return f"~ {label}:\n{self.text_diff}"
        return f"~ {label}: {self.old!r} → {self.new!r}"


@dataclass(frozen=True)
class CellDiff:
    """Per-coordinate comparison within one step namespace."""

    coordinate: str
    outcome: str  # unchanged | changed | added | removed | failed
    before_status: Optional[str] = None
    after_status: Optional[str] = None
    before_output: Any = None
    after_output: Any = None
    before_output_identity: Optional[str] = None
    after_output_identity: Optional[str] = None
    before_output_address: Optional[str] = None
    after_output_address: Optional[str] = None
    changes: tuple[ValueChange, ...] = ()

    def __str__(self) -> str:
        lines = [f"{self.outcome:<9} {self.coordinate}"]
        if self.outcome in ("changed", "failed") and (
            self.before_status or self.after_status
        ):
            lines.append(
                f"          status {self.before_status!r} → {self.after_status!r}"
            )
        for change in self.changes:
            for line in str(change).splitlines():
                lines.append(f"          {line}")
        return "\n".join(lines)


@dataclass(frozen=True)
class RunDiff:
    """Typed result of ``Home.diff`` / ``RunSummary.diff``."""

    before_run_id: str
    after_run_id: str
    pipeline_id: str
    step: str
    items: tuple[CellDiff, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Ensure every outcome key is present for stable consumers.
        counts = {k: int(self.counts.get(k, 0)) for k in _OUTCOMES}
        object.__setattr__(self, "counts", counts)

    def __str__(self) -> str:
        c = self.counts
        lines = [
            (
                f"Diff {self.pipeline_id}/{self.step}: "
                f"{self.before_run_id} → {self.after_run_id}"
            ),
            (
                f"  unchanged={c.get('unchanged', 0)} "
                f"changed={c.get('changed', 0)} "
                f"added={c.get('added', 0)} "
                f"removed={c.get('removed', 0)} "
                f"failed={c.get('failed', 0)}"
            ),
        ]
        interesting = [
            item
            for item in self.items
            if item.outcome != "unchanged"
        ]
        if not interesting:
            lines.append("  (no differences)")
        else:
            for item in interesting:
                for line in str(item).splitlines():
                    lines.append(f"  {line}")
        return "\n".join(lines)


def _load_run(session, run_id: str) -> Run:
    run = session.query(Run).filter_by(id=run_id).first()
    if run is None:
        raise ValueError(f"unknown run {run_id!r}")
    return run


def _step_names_from_definition(run: Run) -> set[str]:
    if not run.definition_json:
        return set()
    try:
        definition = json.loads(str(run.definition_json))
    except Exception:
        return set()
    steps = definition.get("steps") or []
    names: set[str] = set()
    for entry in steps:
        if isinstance(entry, Mapping) and entry.get("name"):
            names.add(str(entry["name"]))
    return names


def _run_mentions_step(run: Run, step: str, cells: list[Cell]) -> bool:
    if step in _step_names_from_definition(run):
        return True
    return any(cell.step_name == step for cell in cells)


def _selection_payload(run: Run) -> Optional[dict[str, Any]]:
    if not run.selection_json:
        return None
    try:
        payload = json.loads(str(run.selection_json))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _cohort_lanes(run: Run, step: str) -> Optional[list[str]]:
    """Exact persisted scope lanes when ``run`` is partial and ``step`` is anchor."""
    if str(run.kind) != "partial":
        return None
    payload = _selection_payload(run)
    if not payload:
        return None
    if payload.get("anchor") != step:
        return None
    lanes = payload.get("lanes")
    if lanes is None:
        return None
    if not isinstance(lanes, (list, tuple)):
        raise ValueError(
            f"run {run.id!r} selection_json.lanes must be a list; "
            f"got {type(lanes)!r}"
        )
    return [str(lane) for lane in lanes]


def _cells_by_coordinate(cells: list[Cell]) -> dict[str, Cell]:
    by_coord: dict[str, Cell] = {}
    for cell in cells:
        # Last write wins if a run somehow recorded duplicates; RCS is
        # append-only per (run, step, coord) in practice.
        by_coord[cell.coordinate] = cell
    return by_coord


def _unified_text_diff(old: str, new: str) -> str:
    return "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile="before",
            tofile="after",
            lineterm="",
        )
    )


def _diff_dicts(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    prefix: str,
) -> list[ValueChange]:
    changes: list[ValueChange] = []
    keys = sorted(set(before) | set(after), key=lambda k: str(k))
    for key in keys:
        path = f"{prefix}.{key}" if prefix else str(key)
        in_before = key in before
        in_after = key in after
        if in_before and not in_after:
            changes.append(ValueChange(path=path, outcome="removed", old=before[key]))
            continue
        if in_after and not in_before:
            changes.append(ValueChange(path=path, outcome="added", new=after[key]))
            continue
        left = before[key]
        right = after[key]
        if left == right:
            continue
        if isinstance(left, dict) and isinstance(right, dict):
            changes.extend(_diff_dicts(left, right, prefix=path))
            continue
        changes.append(
            ValueChange(path=path, outcome="changed", old=left, new=right)
        )
    return changes


def diff_values(before: Any, after: Any) -> tuple[ValueChange, ...]:
    """Field-level value diff used by cell comparison.

    Nested dicts recurse with dotted paths. Top-level strings get a unified
    text diff. Lists and other scalars keep old/new without invented semantics.
    """
    if before == after:
        return ()
    if isinstance(before, dict) and isinstance(after, dict):
        return tuple(_diff_dicts(before, after, prefix=""))
    if isinstance(before, str) and isinstance(after, str):
        return (
            ValueChange(
                path="",
                outcome="changed",
                old=before,
                new=after,
                text_diff=_unified_text_diff(before, after) or None,
            ),
        )
    return (ValueChange(path="", outcome="changed", old=before, new=after),)


def _classify_outcome(
    before: Optional[Cell],
    after: Optional[Cell],
) -> str:
    if after is None:
        return "removed"
    if after.status in _FAILED_STATUSES:
        return "failed"
    if before is None:
        return "added"

    before_ok = before.status in _SUCCESS_STATUSES
    after_ok = after.status in _SUCCESS_STATUSES
    if before_ok and after_ok:
        # created/reused are execution outcomes for the same value; filtered
        # is a semantic verdict and a transition to/from it is a real change.
        if (before.status == "filtered") != (after.status == "filtered"):
            return "changed"
        before_id = before.output_identity
        after_id = after.output_identity
        if (
            before_id is not None
            and after_id is not None
            and before_id == after_id
        ):
            return "unchanged"
        return "changed"

    if before.status != after.status:
        return "changed"
    if before.output_identity != after.output_identity:
        return "changed"
    if before.output != after.output:
        return "changed"
    return "unchanged"


def _cell_diff(coordinate: str, before: Optional[Cell], after: Optional[Cell]) -> CellDiff:
    outcome = _classify_outcome(before, after)
    changes: tuple[ValueChange, ...] = ()
    if (
        outcome == "changed"
        and before is not None
        and after is not None
        and before.status in _SUCCESS_STATUSES
        and after.status in _SUCCESS_STATUSES
    ):
        changes = diff_values(before.output, after.output)
    return CellDiff(
        coordinate=coordinate,
        outcome=outcome,
        before_status=before.status if before else None,
        after_status=after.status if after else None,
        before_output=before.output if before else None,
        after_output=after.output if after else None,
        before_output_identity=before.output_identity if before else None,
        after_output_identity=after.output_identity if after else None,
        before_output_address=before.output_address if before else None,
        after_output_address=after.output_address if after else None,
        changes=changes,
    )


def _normalize_lanes(lanes: Optional[Collection[str]]) -> Optional[list[str]]:
    if lanes is None:
        return None
    out = [str(lane) for lane in lanes]
    if any(not lane for lane in out):
        raise ValueError("lanes= entries must be non-empty strings")
    return out


def diff_runs(
    home: "Home",
    *,
    step: str,
    before: RunRef,
    after: RunRef,
    lanes: Optional[Collection[str]] = None,
) -> RunDiff:
    """Compare one step's cells across two runs. Read-only."""
    if not step or not isinstance(step, str):
        raise ValueError("step= must be a non-empty string")

    before_id = resolve_run_id(before)
    after_id = resolve_run_id(after)
    explicit_lanes = _normalize_lanes(lanes)

    with home.session() as session:
        before_run = _load_run(session, before_id)
        after_run = _load_run(session, after_id)

        before_pipeline = str(before_run.pipeline_id or "")
        after_pipeline = str(after_run.pipeline_id or "")
        if not before_pipeline or not after_pipeline:
            raise ValueError(
                "both runs must record a pipeline_id to be comparable"
            )
        if before_pipeline != after_pipeline:
            raise ValueError(
                f"runs belong to different pipelines: "
                f"{before_id!r} is {before_pipeline!r}, "
                f"{after_id!r} is {after_pipeline!r}"
            )

        before_cells = get_run_cells(
            session,
            home,
            before_id,
            step=step,
            resolve_output=True,
        )
        after_cells = get_run_cells(
            session,
            home,
            after_id,
            step=step,
            resolve_output=True,
        )

        if not _run_mentions_step(before_run, step, before_cells):
            raise ValueError(
                f"step {step!r} not found in run {before_id!r} "
                f"(definition or recorded cells)"
            )
        if not _run_mentions_step(after_run, step, after_cells):
            raise ValueError(
                f"step {step!r} not found in run {after_id!r} "
                f"(definition or recorded cells)"
            )

        before_by = _cells_by_coordinate(before_cells)
        after_by = _cells_by_coordinate(after_cells)

        if explicit_lanes is not None:
            universe = list(dict.fromkeys(explicit_lanes))
        else:
            cohort = _cohort_lanes(after_run, step)
            if cohort is not None:
                # Cohort-aware default: exact persisted scope lanes at the
                # anchor. Missing scoped lanes stay in the universe as removed.
                universe = list(dict.fromkeys(cohort))
            else:
                universe = sorted(set(before_by) | set(after_by))

        items = tuple(
            _cell_diff(coord, before_by.get(coord), after_by.get(coord))
            for coord in universe
        )

    counts = {name: 0 for name in _OUTCOMES}
    for item in items:
        counts[item.outcome] = counts.get(item.outcome, 0) + 1

    return RunDiff(
        before_run_id=before_id,
        after_run_id=after_id,
        pipeline_id=before_pipeline,
        step=step,
        items=items,
        counts=counts,
    )


__all__ = [
    "CellDiff",
    "RunDiff",
    "RunRef",
    "ValueChange",
    "diff_runs",
    "diff_values",
    "resolve_run_id",
]
