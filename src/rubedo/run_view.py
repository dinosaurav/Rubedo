"""Grouped Run View projection.

Turns a historical run's definition snapshot + coordinate statuses +
materialization edges into a cardinality-aware tree: parent band →
indented expand child blocks (recursive) → per-group / run-level
summary strips for aggregates.

Design choice (see HANDOFF): **definition-first, edges for expand
lineage**. ``Run.definition_json`` already records ``depends_on`` and
``in_shape``/``out_shape`` for every historical run — enough to derive
step scope (parent / child(expand) / summary(expand)). Expand children
are content-addressed ``row-<hash>`` lanes with no parent in the
coordinate string, so parent→child nesting uses ``MaterializationEdge``
(written at create time; reuse keeps the creating run's edges).

The pure entry point is :func:`project_run_view`. I/O lives in
:func:`build_run_view`.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy.orm import Session

from .models import MaterializationEdge, Run, RunCoordinateStatus

PREVIEW_CHARS = 160


# ---------------------------------------------------------------------------
# Scope / step metadata
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Scope:
    """Where a step's cells live in the grouped table.

    ``kind``:
      - ``parent``: one cell per root lane (fan-out depth 0)
      - ``child``: one cell per child lane of ``expand_step``
      - ``summary``: one cell per aggregate group, attached to the
        group owned by ``expand_step`` (``None`` = run-level strip)
    """

    kind: str  # parent | child | summary
    expand_step: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind, "expand_step": self.expand_step}


PARENT_SCOPE = Scope("parent", None)


def scope_eq(a: Scope, b: Scope) -> bool:
    return a.kind == b.kind and a.expand_step == b.expand_step


@dataclass(frozen=True)
class StepMeta:
    name: str
    shape: str  # map | expand | aggregate | join
    depends_on: Tuple[str, ...]
    scope: Scope
    source_scope: Scope
    group_key: Optional[str] = None
    version: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "shape": self.shape,
            "depends_on": list(self.depends_on),
            "scope": self.scope.to_dict(),
            "source_scope": self.source_scope.to_dict(),
            "group_key": self.group_key,
            "version": self.version,
        }


def step_shape(entry: Mapping[str, Any]) -> str:
    """Infer conceptual shape from a definition step entry."""
    in_s = entry.get("in_shape") or "one"
    out_s = entry.get("out_shape") or "one"
    if in_s in ("aggregate", "fold"):
        return "aggregate"
    if in_s == "join":
        return "join"
    if out_s == "many":
        return "expand"
    return "map"


def topological_steps(definition: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Kahn topo-sort of ``definition['steps']``; stable on registration order."""
    steps = list(definition.get("steps") or [])
    by_name = {s["name"]: s for s in steps}
    names = [s["name"] for s in steps]
    position = {n: i for i, n in enumerate(names)}
    indeg = {
        n: len({d for d in (by_name[n].get("depends_on") or []) if d in by_name})
        for n in names
    }
    dependents: Dict[str, List[str]] = {n: [] for n in names}
    for n in names:
        for d in by_name[n].get("depends_on") or []:
            if d in by_name:
                dependents[d].append(n)
    ready = sorted([n for n, d in indeg.items() if d == 0], key=position.__getitem__)
    out: List[str] = []
    while ready:
        n = ready.pop(0)
        out.append(n)
        for m in dependents[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                ready.append(m)
                ready.sort(key=position.__getitem__)
    # Cycles (shouldn't happen in a recorded definition) — append leftovers.
    for n in names:
        if n not in out:
            out.append(n)
    return [by_name[n] for n in out]


def derive_step_metas(definition: Mapping[str, Any]) -> List[StepMeta]:
    """Derive shape + scope for every step from the definition DAG.

    Root expands are special: their children *are* the parent-band rows, so
    the expand step itself is scoped ``parent`` (displayed on the parent
    band). A non-root expand is scoped ``child(self)`` and nests a child
    block under its ``source_scope``. Aggregates over a child scope become
    ``summary(that expand)``; aggregates over parent-scope maps become
    run-level ``summary(None)``.
    """
    ordered = topological_steps(definition)
    scopes: Dict[str, Scope] = {}
    metas: List[StepMeta] = []

    for entry in ordered:
        name = entry["name"]
        shape = step_shape(entry)
        deps = tuple(entry.get("depends_on") or [])
        dep_scopes = [scopes[d] for d in deps if d in scopes]
        child_ids = {s.expand_step for s in dep_scopes if s.kind == "child" and s.expand_step}
        source_expand = next(iter(sorted(child_ids))) if child_ids else None
        driven = Scope("child", source_expand) if source_expand else PARENT_SCOPE

        if shape == "expand":
            if not deps:
                # Root source: lanes form the parent band.
                scope = PARENT_SCOPE
                source_scope = PARENT_SCOPE
            else:
                scope = Scope("child", name)
                source_scope = driven
        elif shape == "aggregate":
            if source_expand is not None:
                scope = Scope("summary", source_expand)
            else:
                scope = Scope("summary", None)
            source_scope = driven
        elif shape == "join":
            # New pair lanes — treat like a non-root expand for nesting.
            scope = Scope("child", name)
            source_scope = driven
        else:
            scope = driven
            source_scope = driven

        scopes[name] = scope
        metas.append(
            StepMeta(
                name=name,
                shape=shape,
                depends_on=deps,
                scope=scope,
                source_scope=source_scope,
                group_key=entry.get("group_key"),
                version=entry.get("version"),
            )
        )
    return metas


# ---------------------------------------------------------------------------
# View tree
# ---------------------------------------------------------------------------


@dataclass
class CellView:
    coordinate: str
    step_name: str
    status: str
    output_address: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    preview: Optional[Any] = None
    created_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GroupNode:
    """One row at a nesting level, plus nested expand blocks and summaries."""

    coordinate: str
    cells: Dict[str, CellView] = field(default_factory=dict)
    children: List["ChildBlock"] = field(default_factory=list)
    summary: Dict[str, CellView] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coordinate": self.coordinate,
            "cells": {k: v.to_dict() for k, v in self.cells.items()},
            "children": [c.to_dict() for c in self.children],
            "summary": {k: v.to_dict() for k, v in self.summary.items()},
        }


@dataclass
class ChildBlock:
    expand_step: str
    rows: List[GroupNode] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expand_step": self.expand_step,
            "rows": [r.to_dict() for r in self.rows],
        }


@dataclass
class RunView:
    steps: List[StepMeta]
    params: Dict[str, Any]
    groups: List[GroupNode]
    run_summary: Dict[str, CellView]
    totals: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "steps": [s.to_dict() for s in self.steps],
            "params": self.params,
            "groups": [g.to_dict() for g in self.groups],
            "run_summary": {k: v.to_dict() for k, v in self.run_summary.items()},
            "totals": dict(self.totals),
        }


# ---------------------------------------------------------------------------
# Pure projection
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CoordRecord:
    """One RunCoordinateStatus row, optionally with a truncated preview."""

    coordinate: str
    step_name: str
    status: str
    output_address: Optional[str] = None
    error_message: Optional[str] = None
    error_type: Optional[str] = None
    created_at: Optional[str] = None
    preview: Optional[Any] = None


def _cell(rec: CoordRecord) -> CellView:
    return CellView(
        coordinate=rec.coordinate,
        step_name=rec.step_name,
        status=rec.status,
        output_address=rec.output_address,
        error_message=rec.error_message,
        error_type=rec.error_type,
        preview=rec.preview,
        created_at=rec.created_at,
    )


def _index_coords(
    coords: Sequence[CoordRecord],
) -> Dict[Tuple[str, str], CoordRecord]:
    """(step_name, coordinate) → latest record (last write wins)."""
    out: Dict[Tuple[str, str], CoordRecord] = {}
    for c in coords:
        out[(c.step_name, c.coordinate)] = c
    return out


def _by_step(coords: Sequence[CoordRecord]) -> Dict[str, List[CoordRecord]]:
    out: Dict[str, List[CoordRecord]] = {}
    for c in coords:
        out.setdefault(c.step_name, []).append(c)
    return out


def _addr_index(
    coords: Sequence[CoordRecord],
) -> Dict[str, CoordRecord]:
    out: Dict[str, CoordRecord] = {}
    for c in coords:
        if c.output_address:
            out[c.output_address] = c
    return out


def _edge_maps(
    edges: Sequence[Tuple[str, str]],
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    children: Dict[str, List[str]] = {}
    parents: Dict[str, List[str]] = {}
    for p, c in edges:
        children.setdefault(p, []).append(c)
        parents.setdefault(c, []).append(p)
    return children, parents


def _root_expand(metas: Sequence[StepMeta]) -> Optional[StepMeta]:
    for m in metas:
        if m.shape == "expand" and not m.depends_on and scope_eq(m.scope, PARENT_SCOPE):
            return m
    return None


def _parent_coordinates(
    metas: Sequence[StepMeta], by_step: Mapping[str, List[CoordRecord]]
) -> List[str]:
    """Lane keys that form the parent band."""
    root_exp = _root_expand(metas)
    if root_exp is not None:
        recs = by_step.get(root_exp.name, [])
        return sorted({r.coordinate for r in recs})

    # Map / constant root: prefer @root, else whatever the first root used.
    roots = [m for m in metas if not m.depends_on]
    if not roots:
        return []
    coords: set[str] = set()
    for r in roots:
        for rec in by_step.get(r.name, []):
            coords.add(rec.coordinate)
    if "@root" in coords:
        return ["@root"]
    return sorted(coords)


def _own_cell_steps(metas: Sequence[StepMeta], own_scope: Scope) -> List[StepMeta]:
    return [
        m
        for m in metas
        if m.shape not in ("aggregate",)
        and scope_eq(m.scope, own_scope)
    ]


def _own_expand_steps(metas: Sequence[StepMeta], own_scope: Scope) -> List[StepMeta]:
    return [
        m
        for m in metas
        if m.shape in ("expand", "join")
        and not scope_eq(m.scope, PARENT_SCOPE)  # root expand is parent-band
        and scope_eq(m.source_scope, own_scope)
    ]


def _own_summary_steps(metas: Sequence[StepMeta], expand_step: Optional[str]) -> List[StepMeta]:
    return [
        m
        for m in metas
        if m.shape == "aggregate" and m.scope.expand_step == expand_step
    ]


def _children_of_expand(
    expand: StepMeta,
    parent_coord: str,
    coord_index: Mapping[Tuple[str, str], CoordRecord],
    by_step: Mapping[str, List[CoordRecord]],
    children_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
) -> List[CoordRecord]:
    """Expand/join child lanes under ``parent_coord`` via materialization edges."""
    if not expand.depends_on:
        return []
    dep = expand.depends_on[0]
    parent_rec = coord_index.get((dep, parent_coord))
    if parent_rec is None or not parent_rec.output_address:
        return []
    child_addrs = children_of.get(parent_rec.output_address, [])
    out: List[CoordRecord] = []
    seen: set[str] = set()
    for addr in child_addrs:
        rec = addr_index.get(addr)
        if rec is None or rec.step_name != expand.name:
            continue
        if rec.coordinate in seen:
            continue
        seen.add(rec.coordinate)
        out.append(rec)
    # Stable order by coordinate.
    out.sort(key=lambda r: r.coordinate)
    # Fallback: if edges are missing (legacy / edge-less reuse path) and the
    # expand's coordinates are disjoint from the parent set, leave empty —
    # inventing a parent would be wrong.
    _ = by_step
    return out


def _depth0_parent_for_addr(
    addr: str,
    parents_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
    parent_coords: set[str],
    *,
    _seen: Optional[set[str]] = None,
) -> Optional[str]:
    """Walk edges upward until we hit a depth-0 parent coordinate."""
    seen = _seen if _seen is not None else set()
    if addr in seen:
        return None
    seen.add(addr)
    rec = addr_index.get(addr)
    if rec is not None and rec.coordinate in parent_coords:
        return rec.coordinate
    for p in parents_of.get(addr, []):
        hit = _depth0_parent_for_addr(
            p, parents_of, addr_index, parent_coords, _seen=seen
        )
        if hit is not None:
            return hit
    return None


def _attribute_summaries(
    summary_steps: Sequence[StepMeta],
    by_step: Mapping[str, List[CoordRecord]],
    parents_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
    parent_coords: Sequence[str],
) -> Tuple[Dict[str, Dict[str, CellView]], Dict[str, CellView]]:
    """Split aggregate cells into per-parent-group vs run-level.

    An aggregate lane attaches to a parent group when every edge-parent
    resolves to the same depth-0 parent coordinate; otherwise it is
    run-level (typical ``@all`` over many parents).
    """
    parent_set = set(parent_coords)
    per_group: Dict[str, Dict[str, CellView]] = {c: {} for c in parent_coords}
    run_level: Dict[str, CellView] = {}

    for step in summary_steps:
        for rec in by_step.get(step.name, []):
            cell = _cell(rec)
            if not rec.output_address:
                # Failed/blocked with no address — try coordinate match, else run-level.
                if rec.coordinate in parent_set:
                    per_group[rec.coordinate][step.name] = cell
                else:
                    run_level[step.name] = cell
                continue
            parents = parents_of.get(rec.output_address, [])
            attributed: set[str] = set()
            for p in parents:
                hit = _depth0_parent_for_addr(p, parents_of, addr_index, parent_set)
                if hit is not None:
                    attributed.add(hit)
            if len(attributed) == 1:
                per_group[next(iter(attributed))][step.name] = cell
            elif rec.coordinate in parent_set:
                per_group[rec.coordinate][step.name] = cell
            else:
                run_level[step.name] = cell
    return per_group, run_level


def _build_node(
    coordinate: str,
    own_scope: Scope,
    metas: Sequence[StepMeta],
    coord_index: Mapping[Tuple[str, str], CoordRecord],
    by_step: Mapping[str, List[CoordRecord]],
    children_of: Mapping[str, List[str]],
    parents_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
    group_summaries: Mapping[str, Mapping[str, CellView]],
) -> GroupNode:
    cells: Dict[str, CellView] = {}
    for step in _own_cell_steps(metas, own_scope):
        rec = coord_index.get((step.name, coordinate))
        if rec is not None:
            cells[step.name] = _cell(rec)

    child_blocks: List[ChildBlock] = []
    for expand in _own_expand_steps(metas, own_scope):
        child_recs = _children_of_expand(
            expand, coordinate, coord_index, by_step, children_of, addr_index
        )
        child_scope = Scope("child", expand.name)
        rows: List[GroupNode] = []
        for crec in child_recs:
            rows.append(
                _build_node(
                    crec.coordinate,
                    child_scope,
                    metas,
                    coord_index,
                    by_step,
                    children_of,
                    parents_of,
                    addr_index,
                    group_summaries,
                )
            )
            # Ensure the expand step's own cell appears on the child row.
            if expand.name not in rows[-1].cells:
                rows[-1].cells[expand.name] = _cell(crec)
        child_blocks.append(ChildBlock(expand_step=expand.name, rows=rows))

    # Summaries whose expand nests under this scope, attributed to this row.
    summary: Dict[str, CellView] = {}
    if scope_eq(own_scope, PARENT_SCOPE):
        summary = dict(group_summaries.get(coordinate, {}))
        # Also attach summary(None) cells that were attributed to this parent.
        for step in _own_summary_steps(metas, None):
            if step.name in summary:
                continue
            # left for run_summary
    else:
        # Nested: summaries for expands whose source_scope is this child scope
        # are attributed via edges to depth-0; also allow direct coord match.
        for expand in _own_expand_steps(metas, own_scope):
            for step in _own_summary_steps(metas, expand.name):
                for rec in by_step.get(step.name, []):
                    if rec.coordinate == coordinate:
                        summary[step.name] = _cell(rec)

    return GroupNode(
        coordinate=coordinate,
        cells=cells,
        children=child_blocks,
        summary=summary,
    )


def project_run_view(
    definition: Mapping[str, Any],
    coordinates: Sequence[CoordRecord],
    edges: Sequence[Tuple[str, str]],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> RunView:
    """Pure projection: definition + coords + edges → grouped RunView.

    No DB, no Home, no I/O — unit-testable with fixture lists.
    """
    metas = derive_step_metas(definition)
    coord_index = _index_coords(coordinates)
    by_step = _by_step(coordinates)
    addr_index = _addr_index(coordinates)
    children_of, parents_of = _edge_maps(edges)

    parent_coords = _parent_coordinates(metas, by_step)

    # Prefer edge attribution for *all* aggregates; nested ones that resolve
    # to a single parent land in that group's summary.
    all_agg = [m for m in metas if m.shape == "aggregate"]
    per_group, run_level = _attribute_summaries(
        all_agg, by_step, parents_of, addr_index, parent_coords
    )

    # Nested-expand summaries that edge-attribution put in run_level stay there;
    # ones in per_group attach to parent Detail via group.summary.
    # Additionally, for summary(E) where E is nested, try to place on the
    # child block's owning parent — already done if attributed.

    groups = [
        _build_node(
            coord,
            PARENT_SCOPE,
            metas,
            coord_index,
            by_step,
            children_of,
            parents_of,
            addr_index,
            per_group,
        )
        for coord in parent_coords
    ]

    # Strip nested-expand aggregate cells from run_level when they were also
    # placed on a group (avoid double-rendering).
    placed: set[str] = set()
    for g in groups:
        placed.update(g.summary.keys())
    run_summary = {k: v for k, v in run_level.items() if k not in placed}

    totals = {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
    for c in coordinates:
        if c.status in totals:
            totals[c.status] += 1

    return RunView(
        steps=metas,
        params=dict(params or {}),
        groups=groups,
        run_summary=run_summary,
        totals=totals,
    )


# ---------------------------------------------------------------------------
# I/O wrapper
# ---------------------------------------------------------------------------


def truncate_preview(value: Any, limit: int = PREVIEW_CHARS) -> Any:
    """Compact preview for in-cell display; never returns multi-MB payloads."""
    if value is None:
        return None
    if isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return value if len(value) <= limit else value[: limit - 1] + "…"
    try:
        text = json.dumps(value, sort_keys=True, default=str)
    except Exception:
        text = str(value)
    if len(text) <= limit:
        try:
            return json.loads(text)
        except Exception:
            return text
    return text[: limit - 1] + "…"


def _preview_for_address(home: Any, address: Optional[str]) -> Optional[Any]:
    if not address:
        return None
    row = home.lanes.address_row_index().get(address)
    if not row:
        return None
    output = row.get("output")
    content_type = row.get("content_type")
    # Spilled refs look like "objects:<hash>" strings — don't inline bytes.
    if isinstance(output, str) and output.startswith("objects:"):
        return f"<{content_type or 'bytes'}>"
    try:
        value = home.store.read_output(output, content_type)
    except Exception:
        value = output
    return truncate_preview(value)


def build_run_view(home: Any, run_id: str, *, session: Optional[Session] = None) -> Optional[Dict[str, Any]]:
    """Load run artifacts from ``home`` and return a JSON-ready RunView dict."""

    def _build(sess: Session) -> Optional[Dict[str, Any]]:
        run = sess.query(Run).filter_by(id=run_id).first()
        if run is None:
            return None
        definition: Dict[str, Any] = {}
        if run.definition_json:
            try:
                definition = json.loads(str(run.definition_json))
            except Exception:
                definition = {}
        params: Dict[str, Any] = {}
        if run.params_json:
            try:
                params = json.loads(str(run.params_json))
            except Exception:
                params = {}

        rcs_rows = (
            sess.query(RunCoordinateStatus)
            .filter_by(run_id=run_id)
            .order_by(RunCoordinateStatus.id)
            .all()
        )
        run_addrs = {
            str(r.output_address) for r in rcs_rows if r.output_address
        }
        edge_rows: Iterable[Any] = []
        if run_addrs:
            edge_rows = (
                sess.query(
                    MaterializationEdge.parent_address,
                    MaterializationEdge.child_address,
                )
                .filter(
                    (MaterializationEdge.parent_address.in_(run_addrs))
                    | (MaterializationEdge.child_address.in_(run_addrs))
                )
                .all()
            )
        edges = [
            (str(p), str(c))
            for p, c in edge_rows
            if str(p) in run_addrs and str(c) in run_addrs
        ]

        coords: List[CoordRecord] = []
        for r in rcs_rows:
            addr = str(r.output_address) if r.output_address else None
            coords.append(
                CoordRecord(
                    coordinate=str(r.coordinate),
                    step_name=str(r.step_name or ""),
                    status=str(r.status),
                    output_address=addr,
                    error_message=str(r.error_message) if r.error_message else None,
                    error_type=str(r.error_type) if r.error_type else None,
                    created_at=str(r.created_at) if r.created_at else None,
                    preview=_preview_for_address(home, addr),
                )
            )

        view = project_run_view(definition, coords, edges, params=params)
        return view.to_dict()

    if session is not None:
        return _build(session)
    with home.session() as sess:
        return _build(sess)
