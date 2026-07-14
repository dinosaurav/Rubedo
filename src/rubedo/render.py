"""Pipeline rendering: describe() and its text/mermaid/ascii backends.

Sits above spec.py and planning.py (both imported at module level — no
lazy imports, no cycles): rendering needs topological order, which
belongs to planning, and it operates on the pure data spec.py defines.
"""
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .planning import topological_sort
from .spec import PipelineSpec, StepSpec


def describe(spec: PipelineSpec, format: str = "text") -> str:
    """Render a pipeline's DAG before ever running it.

    format="text" prints steps in dependency order with their policies;
    format="mermaid" emits a Mermaid graph for markdown viewers; format="ascii"
    draws topo-layered boxes connected by unicode box-drawing edges, for
    reading the DAG shape directly in a terminal.
    """
    topo = topological_sort(spec)

    if format == "mermaid":
        lines = ["graph TD"]
        for s in topo:
            label = f"{s.name}<br/>{s.version}" if s.version else s.name
            shape = f'{s.name}(["{label}"])' if s.skip_cache else f'{s.name}["{label}"]'
            lines.append(f"    {shape}")
        for s in topo:
            for dep in s.depends_on:
                lines.append(f"    {dep} --> {s.name}")
        return "\n".join(lines)

    if format == "ascii":
        return _describe_ascii(spec, topo)

    if format != "text":
        raise ValueError(
            f"Unknown format {format!r}: expected 'text', 'mermaid', or 'ascii'"
        )

    roots = sorted(s.name for s in spec.steps if not s.depends_on)
    lines = [f"Pipeline '{spec.name}' — roots: {', '.join(roots)}"]
    for s in topo:
        deps = f" <- {', '.join(s.depends_on)}" if s.depends_on else " (root)"
        policies = []
        if s.skip_cache:
            policies.append("skip_cache")
        if s.retries:
            policies.append(f"retries={s.retries}")
        if s.rate_limit:
            count, period = s.rate_limit
            policies.append(f"rate_limit={count}/{int(period)}s")
        if s.stale_after is not None:
            policies.append(f"stale_after={int(s.stale_after)}s")
        if s.code_mode == "auto":
            policies.append("code=auto")
        if s.params_model is not None:
            policies.append(f"params={s.params_model.__name__}")
        if s.shape in ("reduce", "join") and s.on_failed == "block":
            policies.append("on_failed=block")
        policy_str = f"  [{', '.join(policies)}]" if policies else ""
        lines.append(f"  {s.name} ({s.version}){deps}{policy_str}")
    return "\n".join(lines)


# --- format="ascii": hand-rolled topo-layered terminal DAG rendering -------
#
# No new dependencies (networkx in the dev group is for tests, not layout).
# Not-graphviz-quality: legible up to ~20 steps, naive edge crossings
# allowed. Determinism is load-bearing: every
# ordering decision below comes from `topo` (itself derived from spec
# order — see `topological_sort`) or from plain list iteration, never from
# dict/set iteration order, so the same spec always renders byte-identical.

_ASCII_MAX_WIDTH = 100  # canvas wider than this falls back to format="text"
_ASCII_GAP = 2  # blank columns between adjacent boxes in the same layer


@dataclass
class _AsciiNode:
    """One column-slot in the layered layout.

    `label is None` marks a virtual passthrough node: a placeholder minted
    so an edge spanning more than one layer (e.g. a join whose parents sit
    at different depths) still only ever connects adjacent layers — it
    draws as a bare vertical line threading through the layers it skips.
    """
    id: Tuple[str, Any]
    width: int
    label: Optional[str] = None
    x0: int = 0

    @property
    def xc(self) -> int:
        return self.x0 + self.width // 2


def _ascii_layers(
    topo: List[StepSpec],
) -> Tuple[List[List[_AsciiNode]], Dict[Tuple[str, Any], List[Tuple[str, Any]]]]:
    """Group steps into topo-depth layers and mint virtual passthrough nodes
    so every edge spans exactly one layer.

    Returns (layers, adjacency): `layers[d]` is the ordered list of nodes at
    depth d (spec order, via `topo`); `adjacency[node_id]` is that node's
    ordered list of child node ids in layer d+1.
    """
    depth: Dict[str, int] = {}
    for s in topo:
        depth[s.name] = 0 if not s.depends_on else 1 + max(depth[d] for d in s.depends_on)
    max_depth = max(depth.values(), default=0)

    layers: List[List[_AsciiNode]] = [[] for _ in range(max_depth + 1)]
    for s in topo:
        label = s.name if s.shape == "map" else f"{s.name} [{s.shape}]"
        layers[depth[s.name]].append(
            _AsciiNode(id=("s", s.name), width=len(label) + 4, label=label)
        )

    adjacency: Dict[Tuple[str, Any], List[Tuple[str, Any]]] = {}
    vcount = 0
    for s in topo:
        child_depth = depth[s.name]
        for parent_name in s.depends_on:
            prev: Tuple[str, Any] = ("s", parent_name)
            for mid in range(depth[parent_name] + 1, child_depth):
                vcount += 1
                vid: Tuple[str, Any] = ("v", vcount)
                layers[mid].append(_AsciiNode(id=vid, width=1, label=None))
                adjacency.setdefault(prev, []).append(vid)
                prev = vid
            adjacency.setdefault(prev, []).append(("s", s.name))

    return layers, adjacency


def _ascii_positions(layers: List[List[_AsciiNode]]) -> int:
    """Assign each node's x0, left-packed within its layer. Returns the
    widest layer's total width (the canvas width)."""
    canvas_width = 0
    for layer in layers:
        x = 0
        for node in layer:
            node.x0 = x
            x += node.width + _ASCII_GAP
        if layer:
            x -= _ASCII_GAP
        canvas_width = max(canvas_width, x)
    return canvas_width


def _describe_ascii(spec: PipelineSpec, topo: List[StepSpec]) -> str:
    """Render `topo` as layered boxes joined by box-drawing edges.

    Falls back to format="text" when a layer is too wide to draw legibly —
    never emits garbage, never crashes on a legal DAG.
    """
    if not topo:
        return describe(spec, format="text")

    layers, adjacency = _ascii_layers(topo)
    canvas_width = _ascii_positions(layers)
    if canvas_width > _ASCII_MAX_WIDTH:
        return describe(spec, format="text")

    node_by_id = {node.id: node for layer in layers for node in layer}

    def blank_row() -> List[str]:
        return [" "] * canvas_width

    canvas_rows: List[List[str]] = []
    for i, layer in enumerate(layers):
        box_rows = [blank_row() for _ in range(3)]
        for node in layer:
            if node.label is None:
                for r in range(3):
                    box_rows[r][node.x0] = "│"
                continue
            w = node.width
            for col, ch in enumerate("┌" + "─" * (w - 2) + "┐"):
                box_rows[0][node.x0 + col] = ch
            for col, ch in enumerate("│ " + node.label + " │"):
                box_rows[1][node.x0 + col] = ch
            for col, ch in enumerate("└" + "─" * (w - 2) + "┘"):
                box_rows[2][node.x0 + col] = ch
        canvas_rows.extend(box_rows)

        if i == len(layers) - 1:
            continue

        edges: List[Tuple[_AsciiNode, _AsciiNode]] = [
            (node, node_by_id[child_id])
            for node in layer
            for child_id in adjacency.get(node.id, [])
        ]

        out_seen: Dict[Tuple[str, Any], int] = {}
        in_seen: Dict[Tuple[str, Any], int] = {}
        out_rank: List[int] = []
        in_rank: List[int] = []
        for parent, child in edges:
            out_rank.append(out_seen.get(parent.id, 0))
            out_seen[parent.id] = out_rank[-1] + 1
            in_rank.append(in_seen.get(child.id, 0))
            in_seen[child.id] = in_rank[-1] + 1
        fan_out = out_seen  # final counts, keyed by parent id

        height = max([max(o, i) for o, i in zip(out_rank, in_rank)], default=-1) + 1
        height = max(height, 1)
        conn_rows = [blank_row() for _ in range(height)]

        for (parent, child), orank, irank in zip(edges, out_rank, in_rank):
            px, cx = parent.xc, child.xc
            bend = max(orank, irank)
            if px == cx:
                for r in range(height):
                    conn_rows[r][px] = "│"
                continue
            for r in range(bend):
                conn_rows[r][px] = "│"
            for r in range(bend + 1, height):
                conn_rows[r][cx] = "│"
            lo, hi = (px, cx) if px < cx else (cx, px)
            for col in range(lo + 1, hi):
                conn_rows[bend][col] = "─"
            # Parent side: a plain corner if this is the parent's last
            # outgoing branch (nothing more to split below), else a
            # T-junction (the trunk continues down to the remaining
            # branches).
            parent_is_last = orank == fan_out[parent.id] - 1
            if cx > px:
                conn_rows[bend][px] = "└" if parent_is_last else "├"
            else:
                conn_rows[bend][px] = "┘" if parent_is_last else "┤"
            # Child side: a plain corner for the first incoming branch
            # (nothing above it yet), else a T-junction (the trunk already
            # runs down from an earlier merge).
            child_is_first = irank == 0
            if px < cx:
                conn_rows[bend][cx] = "┐" if child_is_first else "┤"
            else:
                conn_rows[bend][cx] = "┌" if child_is_first else "├"

        canvas_rows.extend(conn_rows)

    body = "\n".join("".join(row).rstrip() for row in canvas_rows)
    return f"Pipeline '{spec.name}'\n{body}"
