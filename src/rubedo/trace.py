"""Lane-following: traverse lineage from a selection of materializations.

trace(selection) seeds on the materializations matching the selection and
walks MaterializationEdge transitively — upstream to what they were derived
from, downstream to everything derived from them. Pure read-only queries
over existing ledger tables; no new bookkeeping.

Liveness semantics: by default only *live* materializations seed a trace
("the current state of the world"); pass include_superseded=True to seed
history too. Traversal itself always follows edges regardless of liveness —
a live output's recorded parent may be a superseded generation (the parent
was recomputed but this output hasn't been re-derived yet), and skipping it
would lie about the derivation. Non-live nodes are marked, never hidden.

Root resolution: a lineage *root* (no parent materializations — a root
step's output) is resolved for display by reading its stored payload, not
via any auto-indexing; `@step(index=[...])` stays the opt-in seeding handle.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .db import get_session
from .models import Materialization, MaterializationEdge, RunCoordinateStatus
from .selection import Selection, get_selection_materialization_ids


@dataclass
class TraceNode:
    """One materialization reached by a trace."""

    materialization_id: int
    step_name: str
    pipeline_id: str
    coordinate: Optional[str]
    output_address: str
    is_live: bool
    filtered: bool
    relation: str  # "seed" | "upstream" | "downstream"
    depth: int  # edge distance from the nearest seed (0 for seeds)
    root_value: Any = None  # stored payload, resolved only for lineage roots


@dataclass
class TraceResult:
    """Everything a trace reached: nodes plus the edges between them."""

    nodes: List[TraceNode]
    edges: List[Tuple[int, int]]  # (parent_materialization_id, child_materialization_id)

    @property
    def seeds(self) -> List[TraceNode]:
        return [n for n in self.nodes if n.relation == "seed"]

    def by_step(self) -> Dict[str, List[TraceNode]]:
        out: Dict[str, List[TraceNode]] = {}
        for n in self.nodes:
            out.setdefault(n.step_name, []).append(n)
        return out

    def __str__(self) -> str:
        counts: Dict[str, int] = {}
        for n in self.nodes:
            counts[n.relation] = counts.get(n.relation, 0) + 1
        stale = sum(1 for n in self.nodes if not n.is_live)
        header = (
            "Trace: "
            + ", ".join(f"{counts.get(r, 0)} {r}" for r in ("seed", "upstream", "downstream"))
            + (f" ({stale} superseded/invalidated)" if stale else "")
        )
        lines = [header]
        order = {"upstream": 0, "seed": 1, "downstream": 2}
        for n in sorted(
            self.nodes, key=lambda n: (order[n.relation], n.depth, n.step_name)
        ):
            flags = "" if n.is_live else " [not live]"
            if n.filtered:
                flags += " [filtered]"
            value = ""
            if n.root_value is not None:
                preview = repr(n.root_value)
                value = f"  value={preview[:60] + '…' if len(preview) > 60 else preview}"
            lines.append(
                f"  {n.relation:<10} {n.step_name:<20} "
                f"{(n.coordinate or '?'):<28} @ {n.output_address[:12]}{flags}{value}"
            )
        return "\n".join(lines)


def _bfs(
    session: Session, seed_ids: Set[int], *, downstream: bool
) -> Tuple[Dict[int, int], List[Tuple[int, int]]]:
    """Walk edges transitively from seeds; {mat_id: depth} plus edges seen."""
    here, there = (
        (MaterializationEdge.parent_id, MaterializationEdge.child_id)
        if downstream
        else (MaterializationEdge.child_id, MaterializationEdge.parent_id)
    )
    reached: Dict[int, int] = {}
    edges: List[Tuple[int, int]] = []
    frontier = set(seed_ids)
    depth = 0
    while frontier:
        depth += 1
        rows = (
            session.query(MaterializationEdge)
            .filter(here.in_(frontier))
            .all()
        )
        edges.extend((int(r.parent_id), int(r.child_id)) for r in rows)
        nxt = {int(getattr(r, there.key)) for r in rows}
        frontier = {
            m for m in nxt if m not in reached and m not in seed_ids
        }
        for m in frontier:
            reached[m] = depth
    return reached, edges


def trace(
    selection: Selection,
    *,
    include_superseded: bool = False,
    resolve_roots: bool = True,
    home: Optional[str] = None,
) -> TraceResult:
    """Follow lineage up and down from the materializations a selection matches.

    include_superseded seeds non-live generations too (traversal always
    follows real edges either way — see module docstring). resolve_roots
    reads the stored payload of lineage roots so a trace can show the human
    what source item everything came from.
    """
    if home is not None:
        from .runner import _init_home

        _init_home(home)

    with get_session() as session:
        seed_ids = set(get_selection_materialization_ids(session, selection))
        if seed_ids and not include_superseded:
            live_rows = (
                session.query(Materialization.id)
                .filter(Materialization.id.in_(seed_ids), Materialization.is_live)
                .all()
            )
            seed_ids = {int(r.id) for r in live_rows}

        up, up_edges = _bfs(session, seed_ids, downstream=False)
        down, down_edges = _bfs(session, seed_ids, downstream=True)

        # A node reachable both ways (diamonds) keeps its upstream reading.
        placement: Dict[int, Tuple[str, int]] = {}
        for m in seed_ids:
            placement[m] = ("seed", 0)
        for m, d in down.items():
            placement.setdefault(m, ("downstream", d))
        for m, d in up.items():
            placement[m] = ("upstream", d) if placement.get(m, ("", 0))[0] != "seed" else placement[m]

        all_ids = set(placement)
        if not all_ids:
            return TraceResult(nodes=[], edges=[])

        mats = {
            int(m.id): m
            for m in session.query(Materialization)
            .filter(Materialization.id.in_(all_ids))
            .all()
        }
        coords: Dict[int, str] = {}
        for mat_id, coordinate in (
            session.query(
                RunCoordinateStatus.materialization_id, RunCoordinateStatus.coordinate
            )
            .filter(RunCoordinateStatus.materialization_id.in_(all_ids))
            .all()
        ):
            coords[int(mat_id)] = str(coordinate)

        # Lineage roots: reached nodes with no parent edge in the ledger at all.
        parented = {
            int(r.child_id)
            for r in session.query(MaterializationEdge.child_id)
            .filter(MaterializationEdge.child_id.in_(all_ids))
            .all()
        }
        root_values: Dict[int, Any] = {}
        if resolve_roots:
            from .store import read_materialization_output

            for m in all_ids - parented:
                try:
                    root_values[m] = read_materialization_output(mats[m])  # type: ignore[arg-type]
                except Exception:
                    pass  # a missing object never breaks a read-only trace

        nodes = [
            TraceNode(
                materialization_id=m,
                step_name=str(mat.step_name),
                pipeline_id=str(mat.pipeline_id),
                coordinate=coords.get(m),
                output_address=str(mat.output_address),
                is_live=bool(mat.is_live),
                filtered=bool(mat.filtered),
                relation=placement[m][0],
                depth=placement[m][1],
                root_value=root_values.get(m),
            )
            for m, mat in mats.items()
        ]
        edge_set = {e for e in up_edges + down_edges}
        return TraceResult(nodes=nodes, edges=sorted(edge_set))
