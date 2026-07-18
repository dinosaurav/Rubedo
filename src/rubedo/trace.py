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
step's output) is resolved for display by reading its stored payload.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .db import get_session
from .models import (
    MaterializationEdge,
    RunCoordinateStatus,
    InputHashUsage,
)
from . import lane_store
from .selection import Selection, get_selection_addresses


@dataclass
class TraceNode:
    """One materialization reached by a trace."""

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
    edges: List[Tuple[str, str]]  # (parent_address, child_address)

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
    session: Session, seed_addrs: Set[str], *, downstream: bool
) -> Tuple[Dict[str, int], List[Tuple[str, str]]]:
    """Walk edges transitively from seeds; {address: depth} plus edges seen.

    Address-based: uses ``parent_address`` / ``child_address`` columns on
    ``MaterializationEdge`` — no integer FK lookups.
    """
    here, there = (
        (MaterializationEdge.parent_address, MaterializationEdge.child_address)
        if downstream
        else (MaterializationEdge.child_address, MaterializationEdge.parent_address)
    )
    reached: Dict[str, int] = {}
    edges: List[Tuple[str, str]] = []
    frontier = set(seed_addrs)
    depth = 0
    while frontier:
        depth += 1
        rows = (
            session.query(MaterializationEdge)
            .filter(here.in_(frontier))
            .all()
        )
        edges.extend((str(r.parent_address), str(r.child_address)) for r in rows)
        nxt = {str(getattr(r, there.key)) for r in rows}
        frontier = {
            m for m in nxt if m not in reached and m not in seed_addrs
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
    from .runner import _check_home_guard

    _check_home_guard(home)
    if home is not None:
        from .runner import _init_home

        _init_home(home)

    with get_session() as session:
        seed_addrs = set(get_selection_addresses(session, selection))
        if not seed_addrs:
            return TraceResult(nodes=[], edges=[])

        if not include_superseded:
            # Live = fulfilled=True in input_hash_usages.
            fulfilled_addrs = {
                str(u.address) for u in session.query(InputHashUsage)
                .filter(InputHashUsage.fulfilled.is_(True))
                .all()
            }
            seed_addrs = seed_addrs & fulfilled_addrs

        up, up_edges = _bfs(session, seed_addrs, downstream=False)
        down, down_edges = _bfs(session, seed_addrs, downstream=True)

        # A node reachable both ways (diamonds) keeps its upstream reading.
        placement: Dict[str, Tuple[str, int]] = {}
        for a in seed_addrs:
            placement[a] = ("seed", 0)
        for a, d in down.items():
            placement.setdefault(a, ("downstream", d))
        for a, d in up.items():
            placement[a] = ("upstream", d) if placement.get(a, ("", 0))[0] != "seed" else placement[a]

        all_addrs = set(placement)
        if not all_addrs:
            return TraceResult(nodes=[], edges=[])

        # Build nodes from Arrow rows + RCS coordinates
        arrow_idx = lane_store.address_row_index()
        # Filter out expand-anchor rows — they're cache entries (the child
        # hashes), not real lanes. They have no RCS, no edges, and their
        # output is {"_children": [...]} — not a user payload.
        all_addrs = {
            a for a in all_addrs
            if arrow_idx.get(a, {}).get("lane_key") != "@root"
        }
        if not all_addrs:
            return TraceResult(nodes=[], edges=[])
        # Coordinates from RCS (latest per address)
        addr_coords: Dict[str, str] = {}
        for addr, coord in (
            session.query(
                RunCoordinateStatus.output_address,
                RunCoordinateStatus.coordinate,
            )
            .filter(
                RunCoordinateStatus.output_address.isnot(None),
                RunCoordinateStatus.output_address.in_(all_addrs),
            )
            .order_by(RunCoordinateStatus.id.asc())
            .all()
        ):
            addr_coords[str(addr)] = str(coord)

        # Lineage roots: reached addresses with no parent edge.
        parented = {
            str(r.child_address)
            for r in session.query(MaterializationEdge.child_address)
            .filter(MaterializationEdge.child_address.in_(all_addrs))
            .all()
        }
        root_values: Dict[str, Any] = {}
        if resolve_roots:
            from .store import read_output

            for addr in all_addrs - parented:
                row = arrow_idx.get(addr)
                if row:
                    # Skip expand-anchor rows — they're cache entries (the
                    # child hashes), not real lanes with a user payload.
                    if row.get("lane_key") == "@root":
                        continue
                    try:
                        root_values[addr] = read_output(
                            row.get("output"), row.get("content_type")
                        )
                    except Exception:
                        pass  # a missing object never breaks a read-only trace

        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .all()
        }

        nodes = [
            TraceNode(
                step_name=str(arrow_idx.get(a, {}).get("step_name", "")),
                pipeline_id=str(arrow_idx.get(a, {}).get("pipeline_id", "")),
                coordinate=addr_coords.get(a),
                output_address=a,
                is_live=a in fulfilled_addrs,
                filtered=bool(arrow_idx.get(a, {}).get("filtered", False)),
                relation=placement[a][0],
                depth=placement[a][1],
                root_value=root_values.get(a),
            )
            for a in all_addrs
        ]
        edge_set = {e for e in up_edges + down_edges}
        return TraceResult(nodes=nodes, edges=sorted(edge_set))
