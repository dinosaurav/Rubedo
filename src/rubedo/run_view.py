"""Grouped Run View projection.

Turns a historical run's definition snapshot + coordinate statuses +
materialization edges into a cardinality-aware forest:

- **Branch section** — one table per root lineage (parallel roots do not
  share a band).
- **Join section** — one table per join step (pair lanes), with nested
  expand child blocks underneath.
- **Summary strips** — aggregates render once per group lane (list of
  cells), attached to the section that owns the folded expand/join —
  never collapsed by step name.

Design choice (see HANDOFF): **definition-first, edges for expand
lineage**. ``Run.definition_json`` records ``depends_on`` /
``shape`` (and legacy ``in_shape``/``out_shape``); expand children are content-addressed
``row-<hash>`` lanes, so parent→child nesting uses ``MaterializationEdge``.

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
    """Where a step's cells live.

    ``kind``:
      - ``branch``: root lineage band; ``expand_step`` is the root step name
      - ``join``: join-pair band; ``expand_step`` is the join step name
      - ``child``: fan-out lanes of ``expand_step`` (a non-root expand)
      - ``summary``: aggregate cells for the named expand (or ``None`` =
        section/run-level fold over a branch)
      - ``fold``: post-aggregate continuation; ``expand_step`` is the
        aggregate step that opened the fold — rendered as a normal
        one-row-per-group table, not a summary chip
    """

    kind: str
    expand_step: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"kind": self.kind, "expand_step": self.expand_step}


def scope_eq(a: Scope, b: Scope) -> bool:
    return a.kind == b.kind and a.expand_step == b.expand_step


def branch_scope(root: str) -> Scope:
    return Scope("branch", root)


def join_scope(join_step: str) -> Scope:
    return Scope("join", join_step)


def child_scope(expand_step: str) -> Scope:
    return Scope("child", expand_step)


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
    """Infer conceptual shape from a definition step entry.

    New snapshots store ``shape``. Historical ledger rows still have
    ``in_shape``/``out_shape``; both are accepted. Fold and ``join_table``
    render as aggregate (one coordinate / summary strip), not pair-join.
    """
    shape = entry.get("shape")
    if shape in ("map", "expand", "aggregate", "join"):
        return shape
    if shape in ("fold", "join_table"):
        return "aggregate"
    in_s = entry.get("in_shape") or "one"
    out_s = entry.get("out_shape") or "one"
    if in_s in ("aggregate", "fold"):
        return "aggregate"
    if in_s in ("join", "join_table"):
        return "join" if in_s == "join" else "aggregate"
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
    for n in names:
        if n not in out:
            out.append(n)
    return [by_name[n] for n in out]


def derive_step_metas(definition: Mapping[str, Any]) -> List[StepMeta]:
    """Derive shape + scope for every step from the definition DAG.

    Each root owns a ``branch(root)`` scope so parallel sources never share
    a band. Joins open a top-level ``join(self)`` scope (their own table).
    Non-root expands nest under whatever scope drives them.
    """
    ordered = topological_steps(definition)
    scopes: Dict[str, Scope] = {}
    metas: List[StepMeta] = []

    for entry in ordered:
        name = entry["name"]
        shape = step_shape(entry)
        deps = tuple(entry.get("depends_on") or [])
        dep_scopes = [scopes[d] for d in deps if d in scopes]

        if shape == "expand" and not deps:
            scope = branch_scope(name)
            source_scope = scope
        elif shape == "join":
            scope = join_scope(name)
            source_scope = scope
        elif shape == "expand":
            parent_scope = dep_scopes[0] if dep_scopes else branch_scope(name)
            scope = child_scope(name)
            source_scope = parent_scope
        elif shape == "aggregate":
            child_ids = sorted(
                {
                    s.expand_step
                    for s in dep_scopes
                    if s.kind == "child" and s.expand_step
                }
            )
            if child_ids:
                scope = Scope("summary", child_ids[0])
            else:
                scope = Scope("summary", None)
            source_scope = dep_scopes[0] if dep_scopes else Scope("summary", None)
        elif not deps:
            # Map / constant root.
            scope = branch_scope(name)
            source_scope = scope
        else:
            # Maps after an aggregate (or after another post-aggregate map)
            # continue in a fold band keyed by that aggregate — a normal
            # table, not summary chips (graphify / pdf-digest).
            shapes_so_far = {m.name: m.shape for m in metas}
            fold_anchor: Optional[str] = None
            for d in deps:
                if d not in scopes:
                    continue
                if shapes_so_far.get(d) == "aggregate":
                    fold_anchor = d
                    break
                if scopes[d].kind == "fold" and scopes[d].expand_step:
                    fold_anchor = scopes[d].expand_step
                    break
            if fold_anchor is not None:
                scope = Scope("fold", fold_anchor)
                source_scope = scope
            else:
                join_scopes = [s for s in dep_scopes if s.kind == "join"]
                child_scopes = [s for s in dep_scopes if s.kind == "child"]
                if join_scopes:
                    scope = join_scopes[0]
                elif child_scopes:
                    scope = child_scopes[0]
                else:
                    scope = dep_scopes[0]
                source_scope = scope

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
    summary: List[CellView] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "coordinate": self.coordinate,
            "cells": {k: v.to_dict() for k, v in self.cells.items()},
            "children": [c.to_dict() for c in self.children],
            "summary": [c.to_dict() for c in self.summary],
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
class Section:
    """One top-level table: a root branch, a join, or a post-aggregate fold."""

    id: str
    kind: str  # branch | join | fold
    title: str
    column_steps: List[str]
    row_scope: Scope
    groups: List[GroupNode] = field(default_factory=list)
    summary: List[CellView] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "title": self.title,
            "column_steps": list(self.column_steps),
            "row_scope": self.row_scope.to_dict(),
            "groups": [g.to_dict() for g in self.groups],
            "summary": [c.to_dict() for c in self.summary],
        }


@dataclass
class RunView:
    steps: List[StepMeta]
    params: Dict[str, Any]
    sections: List[Section]
    run_summary: List[CellView]
    totals: Dict[str, int]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "steps": [s.to_dict() for s in self.steps],
            "params": self.params,
            "sections": [s.to_dict() for s in self.sections],
            # Convenience: first section's groups (single-root pipelines).
            "groups": (
                [g.to_dict() for g in self.sections[0].groups] if self.sections else []
            ),
            "run_summary": [c.to_dict() for c in self.run_summary],
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
    out: Dict[Tuple[str, str], CoordRecord] = {}
    for c in coords:
        out[(c.step_name, c.coordinate)] = c
    return out


def _by_step(coords: Sequence[CoordRecord]) -> Dict[str, List[CoordRecord]]:
    out: Dict[str, List[CoordRecord]] = {}
    for c in coords:
        out.setdefault(c.step_name, []).append(c)
    return out


def _addr_index(coords: Sequence[CoordRecord]) -> Dict[str, CoordRecord]:
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


def _column_steps_for_scope(metas: Sequence[StepMeta], row_scope: Scope) -> List[str]:
    """Steps whose cells sit on rows of ``row_scope`` (not aggregates/nested expands)."""
    names: List[str] = []
    for m in metas:
        if m.shape == "aggregate":
            continue
        if m.shape == "expand" and m.scope.kind == "child":
            continue  # nested expand — child-block columns
        if scope_eq(m.scope, row_scope):
            names.append(m.name)
    return names


def _nested_expands(metas: Sequence[StepMeta], own_scope: Scope) -> List[StepMeta]:
    """Non-root expands that nest directly under ``own_scope`` (never joins)."""
    return [
        m
        for m in metas
        if m.shape == "expand"
        and m.scope.kind == "child"
        and scope_eq(m.source_scope, own_scope)
    ]


def _section_row_coords(
    anchor: StepMeta, by_step: Mapping[str, List[CoordRecord]]
) -> List[str]:
    recs = by_step.get(anchor.name, [])
    coords = sorted({r.coordinate for r in recs})
    if not coords and not anchor.depends_on:
        # Map root that minted @root
        return ["@root"]
    return coords


def _children_of_expand(
    expand: StepMeta,
    parent_coord: str,
    coord_index: Mapping[Tuple[str, str], CoordRecord],
    children_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
) -> List[CoordRecord]:
    """Expand child lanes under ``parent_coord`` via materialization edges."""
    if not expand.depends_on:
        return []
    dep = expand.depends_on[0]
    parent_rec = coord_index.get((dep, parent_coord))
    if parent_rec is None or not parent_rec.output_address:
        return []
    out: List[CoordRecord] = []
    seen: set[str] = set()
    for addr in children_of.get(parent_rec.output_address, []):
        rec = addr_index.get(addr)
        if rec is None or rec.step_name != expand.name:
            continue
        if rec.coordinate in seen:
            continue
        seen.add(rec.coordinate)
        out.append(rec)
    out.sort(key=lambda r: r.coordinate)
    return out


def _build_node(
    coordinate: str,
    own_scope: Scope,
    column_steps: Sequence[str],
    metas: Sequence[StepMeta],
    meta_by_name: Mapping[str, StepMeta],
    coord_index: Mapping[Tuple[str, str], CoordRecord],
    children_of: Mapping[str, List[str]],
    addr_index: Mapping[str, CoordRecord],
) -> GroupNode:
    cells: Dict[str, CellView] = {}
    for name in column_steps:
        rec = coord_index.get((name, coordinate))
        if rec is not None:
            cells[name] = _cell(rec)

    child_blocks: List[ChildBlock] = []
    for expand in _nested_expands(metas, own_scope):
        child_recs = _children_of_expand(
            expand, coordinate, coord_index, children_of, addr_index
        )
        child_own = child_scope(expand.name)
        child_cols = _column_steps_for_scope(metas, child_own)
        # Ensure the expand step itself is a column on child rows.
        if expand.name not in child_cols:
            child_cols = [expand.name, *child_cols]
        rows: List[GroupNode] = []
        for crec in child_recs:
            node = _build_node(
                crec.coordinate,
                child_own,
                child_cols,
                metas,
                meta_by_name,
                coord_index,
                children_of,
                addr_index,
            )
            if expand.name not in node.cells:
                node.cells[expand.name] = _cell(crec)
            rows.append(node)
        child_blocks.append(ChildBlock(expand_step=expand.name, rows=rows))

    return GroupNode(
        coordinate=coordinate,
        cells=cells,
        children=child_blocks,
        summary=[],
    )


def _expands_owned_by_section(
    metas: Sequence[StepMeta], row_scope: Scope
) -> set[str]:
    """Expand step names that nest somewhere under this section's row scope."""
    owned: set[str] = set()
    frontier = [row_scope]
    seen_scopes: set[Tuple[str, Optional[str]]] = set()
    while frontier:
        scope = frontier.pop()
        key = (scope.kind, scope.expand_step)
        if key in seen_scopes:
            continue
        seen_scopes.add(key)
        for m in _nested_expands(metas, scope):
            owned.add(m.name)
            frontier.append(child_scope(m.name))
    return owned


def _place_aggregates(
    metas: Sequence[StepMeta],
    by_step: Mapping[str, List[CoordRecord]],
    sections: Sequence[Section],
    *,
    skip: Optional[set[str]] = None,
) -> List[CellView]:
    """Attach aggregate cells to the owning section; leftovers → run_summary.

    Aggregates listed in ``skip`` (those with a fold continuation table) are
    omitted — they render as columns on the fold section instead.

    Multi-group aggregates (``group_key``) keep every lane — keyed by cell
    identity, never collapsed by step name.
    """
    skip = skip or set()
    meta_by_name = {m.name: m for m in metas}
    section_expands = {
        s.id: _expands_owned_by_section(metas, s.row_scope) for s in sections
    }
    run_level: List[CellView] = []

    for m in metas:
        if m.shape != "aggregate" or m.name in skip:
            continue
        cells = [_cell(r) for r in by_step.get(m.name, [])]
        if not cells:
            continue

        target: Optional[Section] = None
        if m.scope.expand_step:
            for s in sections:
                if m.scope.expand_step in section_expands[s.id]:
                    target = s
                    break
        else:
            # Parent-scope fold: prefer the single branch, else the section
            # whose maps are being folded (first dep's branch/join).
            branch_or_join = [s for s in sections if s.kind in ("branch", "join")]
            if len(branch_or_join) == 1:
                target = branch_or_join[0]
            elif m.depends_on:
                dep = meta_by_name.get(m.depends_on[0])
                if dep is not None:
                    for s in branch_or_join:
                        if scope_eq(s.row_scope, dep.scope) or (
                            dep.scope.kind == "branch"
                            and s.kind == "branch"
                            and s.id == dep.scope.expand_step
                        ):
                            target = s
                            break

        if target is not None:
            target.summary.extend(cells)
        else:
            run_level.extend(cells)
    return run_level


def _fold_maps_for(metas: Sequence[StepMeta], aggregate_name: str) -> List[StepMeta]:
    return [
        m
        for m in metas
        if m.scope.kind == "fold" and m.scope.expand_step == aggregate_name
    ]


def _build_fold_sections(
    metas: Sequence[StepMeta],
    by_step: Mapping[str, List[CoordRecord]],
    coord_index: Mapping[Tuple[str, str], CoordRecord],
) -> List[Section]:
    """One normal table per aggregate that has downstream maps.

    Columns = the aggregate + every map in its fold chain; rows = the
    aggregate's group coordinates (``@all`` or ``group_key`` values).
    """
    out: List[Section] = []
    for agg in metas:
        if agg.shape != "aggregate":
            continue
        fold_maps = _fold_maps_for(metas, agg.name)
        if not fold_maps:
            continue
        cols = [agg.name, *[m.name for m in fold_maps]]
        coords = _section_row_coords(agg, by_step)
        groups: List[GroupNode] = []
        for coord in coords:
            cells: Dict[str, CellView] = {}
            for name in cols:
                rec = coord_index.get((name, coord))
                if rec is not None:
                    cells[name] = _cell(rec)
            groups.append(GroupNode(coordinate=coord, cells=cells))
        out.append(
            Section(
                id=f"fold:{agg.name}",
                kind="fold",
                title=f"after {agg.name}",
                column_steps=cols,
                row_scope=Scope("fold", agg.name),
                groups=groups,
            )
        )
    return out


def project_run_view(
    definition: Mapping[str, Any],
    coordinates: Sequence[CoordRecord],
    edges: Sequence[Tuple[str, str]],
    *,
    params: Optional[Mapping[str, Any]] = None,
) -> RunView:
    """Pure projection: definition + coords + edges → sectioned RunView."""
    metas = derive_step_metas(definition)
    meta_by_name = {m.name: m for m in metas}
    coord_index = _index_coords(coordinates)
    by_step = _by_step(coordinates)
    addr_index = _addr_index(coordinates)
    children_of, _parents_of = _edge_maps(edges)

    sections: List[Section] = []

    # Branch sections — one per root, registration/topo order.
    for m in metas:
        if m.scope.kind != "branch" or m.scope.expand_step != m.name:
            continue
        # Only anchors: the root step itself (expand or map root).
        if m.depends_on:
            continue
        row_scope = branch_scope(m.name)
        cols = _column_steps_for_scope(metas, row_scope)
        coords = _section_row_coords(m, by_step)
        groups = [
            _build_node(
                c,
                row_scope,
                cols,
                metas,
                meta_by_name,
                coord_index,
                children_of,
                addr_index,
            )
            for c in coords
        ]
        sections.append(
            Section(
                id=m.name,
                kind="branch",
                title=m.name,
                column_steps=cols,
                row_scope=row_scope,
                groups=groups,
            )
        )

    # Join sections — top-level tables, not nested under a branch.
    for m in metas:
        if m.shape != "join":
            continue
        row_scope = join_scope(m.name)
        cols = _column_steps_for_scope(metas, row_scope)
        coords = _section_row_coords(m, by_step)
        groups = [
            _build_node(
                c,
                row_scope,
                cols,
                metas,
                meta_by_name,
                coord_index,
                children_of,
                addr_index,
            )
            for c in coords
        ]
        sections.append(
            Section(
                id=m.name,
                kind="join",
                title=m.name,
                column_steps=cols,
                row_scope=row_scope,
                groups=groups,
            )
        )

    # Post-aggregate continuation tables (aggregate + downstream maps).
    fold_sections = _build_fold_sections(metas, by_step, coord_index)
    folded_aggs = {
        s.row_scope.expand_step for s in fold_sections if s.row_scope.expand_step
    }
    sections.extend(fold_sections)

    run_summary = _place_aggregates(
        metas, by_step, sections, skip=folded_aggs
    )

    totals = {"created": 0, "reused": 0, "failed": 0, "blocked": 0, "filtered": 0}
    for c in coordinates:
        if c.status in totals:
            totals[c.status] += 1

    return RunView(
        steps=metas,
        params=dict(params or {}),
        sections=sections,
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


def _preview_for_address(
    home: Any, address: Optional[str], address_index: Mapping[str, Dict[str, Any]]
) -> Optional[Any]:
    if not address:
        return None
    row = address_index.get(address)
    if not row:
        return None
    output = row.get("output")
    content_type = row.get("content_type")
    if isinstance(output, str) and output.startswith("objects:"):
        return f"<{content_type or 'bytes'}>"
    try:
        value = home.store.read_output(output, content_type)
    except Exception:
        value = output
    return truncate_preview(value)


def build_run_view(
    home: Any, run_id: str, *, session: Optional[Session] = None
) -> Optional[Dict[str, Any]]:
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
        run_addrs = {str(r.output_address) for r in rcs_rows if r.output_address}
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

        address_index = home.lanes.address_row_index()
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
                    preview=_preview_for_address(home, addr, address_index),
                )
            )

        return project_run_view(definition, coords, edges, params=params).to_dict()

    if session is not None:
        return _build(session)
    with home.session() as sess:
        return _build(sess)
