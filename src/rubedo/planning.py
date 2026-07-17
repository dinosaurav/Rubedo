"""Plan phase: deciding what a run would do, without doing it.

Everything here is read-only with respect to the ledger — the single DB
access is the live-materialization lookup that answers "is this cached?".
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union, Literal

from sqlalchemy.orm import Session

from .hashing import compute_output_address, hash_json
from .spec import PipelineSpec, StepSpec
from .store import read_output
from .util import iso_age_seconds


def _identity_from_output(row: dict) -> str:
    """Read the content identity (for downstream input_hash) from an
    Arrow row's ``output_identity`` column.  This is computed once at
    commit time from the original output value and stored directly —
    no recompute from the Arrow-read-back value, so the union struct
    null-fill (``{"a": 1}`` → ``{"a": 1, "b": None}``) doesn't shift
    the identity."""
    return row.get("output_identity") or ""


def _field_values_from_ref(ref, field: str) -> List[str]:
    """Extract the stringified values of a field from a MatRef's output.

    When the output is a native Arrow value (a dict from a struct column),
    the field is a dict key — read it directly.  When the output is a
    string (the spill/mixed fallback) or None, the field is not available
    without deserializing from the object store.
    """
    output = getattr(ref, "output", None)
    if isinstance(output, dict):
        val = output.get(field)
        if val is None:
            return []
        if isinstance(val, (list, tuple)):
            return [str(v) for v in val]
        return [str(val)]
    return []


@dataclass
class RootItem:
    """A synthetic lane item for a source-less root map: its single `@root`
    lane, or (for a root skip_cache step) the per-coordinate placeholder
    used to key its ephemeral memo. Not part of the public API — a root
    step (no `depends_on`) mints its own lanes directly: an expand root
    yields N via its generator, a map root mints this one synthetic lane."""

    coordinate: str
    content_hash: str


class MatRef:
    """A lightweight reference to a committed materialization."""
    def __init__(
        self,
        id,
        output_address,
        output_content_hash,
        content_type=None,
        filtered=False,
        output=None,
    ):
        self.id = id
        self.output_address = output_address
        self.output_content_hash = output_content_hash
        self.content_type = content_type
        self.filtered = filtered
        self.output = output  # the Arrow "output" column value (inline JSON or ref string)


class _ArrowRowRef:
    """Adapter that makes an Arrow lane_store row dict satisfy the
    HasOutputContentHash protocol so ``read_output`` can read its value
    from the inline JSON or the object store (for spilled refs).  Used by
    the expand-anchor path which needs to deserialize the anchor's stored
    children list.

    The ``output_content_hash`` (identity for downstream ``input_hash``
    computation) is read directly from the ``output_identity`` column —
    computed once at commit time from the original output value."""

    def __init__(self, row: dict):
        self._row = row
        self.output = row.get("output")
        self.output_content_hash = row.get("output_identity") or ""
        self.content_type = row.get("content_type")


@dataclass
class EphemeralRef:
    """A skip_cache step's stand-in for a materialization.

    Carries the step's identity (not its output — it hasn't run) so that
    consumers' cache keys can be computed statically, plus everything needed
    to lazily compute the actual value if a consumer executes.
    """

    step: StepSpec
    item: RootItem
    parent_refs: Dict[str, Any]
    identity_hash: str

    @property
    def output_content_hash(self) -> str:
        # Chains into consumers' input hashes exactly like a real
        # materialization's content hash would
        return self.identity_hash


@dataclass
class StepDecision:
    """The planned action for a coordinate in a step (execute, reuse, filter, block, or pending)."""
    coordinate: str
    action: str  # reuse | execute | blocked | pending | filtered
    item: Optional[RootItem] = None
    input_hash: Optional[str] = None
    output_address: Optional[str] = None
    existing: Optional[MatRef] = None
    parent_mats: Dict[str, Any] = field(default_factory=dict)
    failed_parents: List[str] = field(default_factory=list)
    blocked_parents: List[str] = field(default_factory=list)
    filtered_parents: List[str] = field(default_factory=list)
    # Reusing an output whose code has changed since it was computed
    # (same version string): legal, but worth a warning
    code_drift: bool = False
    # Execute decision caused by an expired output rather than a cache miss:
    # identical recompute bytes refresh the existing generation's clock
    stale: bool = False


# The single lane a source-less root map step mints. Its content is a fixed
# constant, so the output address reduces to hash(step, version, ROOT_LANE,
# params): same params reuse the cached output, changed params make a new
# generation (exactly like a stable-coordinate source lane whose bytes change).
ROOT_LANE = "@root"


def topological_sort(pipeline: PipelineSpec) -> List[StepSpec]:
    """Sort the pipeline steps in topological order based on dependencies."""
    # Validate and sort
    name_to_step = {s.name: s for s in pipeline.steps}

    if len(name_to_step) != len(pipeline.steps):
        raise ValueError("Duplicate step names in pipeline")

    for s in pipeline.steps:
        for dep in s.depends_on:
            if dep not in name_to_step:
                raise ValueError(f"Step '{s.name}' depends on unknown step '{dep}'")

    # Kahn's algorithm or DFS
    visited = set()
    temp_mark = set()
    order = []

    def visit(n: str):
        if n in temp_mark:
            raise ValueError(f"Cycle detected involving step '{n}'")
        if n not in visited:
            temp_mark.add(n)
            s = name_to_step[n]
            for dep in s.depends_on:
                visit(dep)
            temp_mark.remove(n)
            visited.add(n)
            order.append(s)

    for s in pipeline.steps:
        if s.name not in visited:
            visit(s.name)

    return order


def _compute_step_input_hash(
    step: StepSpec,
    coordinate: str,
    sf_content_hash: str,
    parent_mats: Dict[str, MatRef],
) -> str:
    """Compute the combined input hash for a step given its parent materializations."""
    if not step.depends_on:
        return sf_content_hash
    if len(step.depends_on) == 1:
        parent_name = step.depends_on[0]
        return parent_mats[parent_name].output_content_hash

    # Multi-parent — for declarative union, only present parents contribute
    parent_hashes = {
        dep: parent_mats[dep].output_content_hash
        for dep in sorted(step.depends_on)
        if dep in parent_mats
    }
    return hash_json(parent_hashes)


def expand_anchor_address(
    step: StepSpec, parent_hash: str, params_hash: str, accepts_params: bool
) -> str:
    """Address of an expand step's cache anchor (the child content hashes).

    Keyed on the *parent* content, so it is predictable from the parent alone
    — the entry point that lets a re-run skip the fn.
    """
    return compute_output_address(
        step.name,
        step.version,
        parent_hash,
        params_hash=params_hash if accepts_params else None,
        code_hash=step.code_hash if step.code_mode == "auto" else None,
    )


def expand_child_coord(child_hash: str) -> str:
    """Content-addressed coordinate of an expand child lane."""
    return f"row-{child_hash[:12]}"


def expand_child_identity(
    step: StepSpec, child_hash: str, params_hash: str, accepts_params: bool
) -> tuple[str, str]:
    """(input_hash, output_address) of one content-addressed expand child.

    The child's identity *is* its content (`child_hash = hash(value)`), so
    identical children collapse and a re-run lands on the same generation —
    exactly like a source lane. No parent/subkey in the identity.
    """
    return child_hash, compute_output_address(
        step.name,
        step.version,
        child_hash,
        params_hash=params_hash if accepts_params else None,
        code_hash=step.code_hash if step.code_mode == "auto" else None,
    )




def _step_accepts_params(step: StepSpec) -> bool:
    """Check if the step function signature accepts a 'params' keyword argument."""
    import inspect

    if step.fn is None:
        return False  # declarative step — no function, no params
    return "params" in inspect.signature(step.fn).parameters


def _build_step_params(step: StepSpec, params: Optional[dict]):
    """Construct and validate parameters for a step using its optional Pydantic model."""
    if step.params_model:
        return step.params_model(**(params or {}))
    return params or {}


def _code_drift_message(step: StepSpec, drifted: int) -> str:
    """Format a warning message about reusing outputs whose source code has drifted."""
    return (
        f"Step '{step.name}' source code changed but version is still "
        f"'{step.version}': reusing {drifted} cached output(s) computed by the "
        "old code. Bump the version (or use code='auto') to recompute."
    )


def _group_reduce_lanes(
    session: Session, step: StepSpec, parent_mats: Dict[str, Dict[str, Union[MatRef, EphemeralRef]]]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Partition a reduce's parent lanes into groups by a field.

    Reads each lane's ``group_key`` field from the parent's output (a
    dict from a native Arrow struct column) directly.  A lane with
    several values (a list-valued field) joins each group; a lane with
    none raises.
    """
    coord_ref_map = []
    for dep, coord_refs in parent_mats.items():
        for coord, ref in coord_refs.items():
            coord_ref_map.append((dep, coord, ref))

    groups: Dict[str, Dict[str, Dict[str, Union[MatRef, EphemeralRef]]]] = {}
    gk = step.group_key or ""
    for dep, coord, ref in coord_ref_map:
        values = _field_values_from_ref(ref, gk)
        if not values:
            raise ValueError(
                f"reduce step '{step.name}': group_key '{step.group_key}' "
                f"has no value for lane '{coord}' (parent '{dep}'). "
                f"The field must exist in the parent's output dict."
            )
        for v in values:
            groups.setdefault(v, {d: {} for d in step.depends_on})[dep][coord] = ref
    return groups


def _reduce_group_decision(
    session: Session,
    step: StepSpec,
    group_coord: str,
    group_mats: Dict[str, Dict[str, Union[MatRef, EphemeralRef]]],
    params_hash: str,
    force: bool,
    accepts_params: bool,
    failed_parents: Optional[List[str]] = None,
    blocked_parents: Optional[List[str]] = None,
    pipeline_id: str = "",
) -> StepDecision:
    """Reuse/execute decision for one reduce group (or the sole '@all')."""
    hash_data = {}
    for dep, coords_dict in sorted(group_mats.items()):
        hash_data[dep] = {
            c: coords_dict[c].output_content_hash for c in sorted(coords_dict.keys())
        }
    # A real group folds its key into identity, so two groups with coincidentally
    # identical members still get distinct addresses; '@all' keeps the plain
    # shape so existing (ungrouped) reduce caches stay valid.
    if step.group_key is None:
        input_hash = hash_json(hash_data)
    else:
        input_hash = hash_json({"group": group_coord, "members": hash_data})

    output_address = compute_output_address(
        step.name,
        step.version,
        input_hash,
        params_hash=params_hash if accepts_params else None,
        code_hash=step.code_hash if step.code_mode == "auto" else None,
    )

    existing_mat = None
    if pipeline_id:
        from .lane_store import batch_lookup_by_address
        result = batch_lookup_by_address(
            pipeline_id, step.name, {output_address}, session
        )
        existing_mat = result.get(output_address)
    expired = False
    if existing_mat and step.stale_after is not None:
        freshness = existing_mat.get("ts")
        if freshness and iso_age_seconds(freshness) > step.stale_after:
            expired = True

    if existing_mat and not force and not expired:
        return StepDecision(
            coordinate=group_coord,
            action="reuse",
            input_hash=input_hash,
            output_address=output_address,
            existing=MatRef(
                existing_mat.get("mat_id") or existing_mat.get("row_id", ""),
                output_address,
                existing_mat.get("output_identity", ""),
                existing_mat.get("content_type"),
                filtered=existing_mat.get("filtered", False),
                output=existing_mat.get("output"),
            ),
            code_drift=(
                step.code_mode == "warn"
                and step.code_hash is not None
                and step.code_hash is not None
                and step.code_hash != existing_mat.get("code_hash")
            ),
            failed_parents=failed_parents or [],
            blocked_parents=blocked_parents or [],
        )
    return StepDecision(
        coordinate=group_coord,
        action="execute",
        input_hash=input_hash,
        output_address=output_address,
        parent_mats=group_mats,
        stale=expired,
        failed_parents=failed_parents or [],
        blocked_parents=blocked_parents or [],
    )





def _plan_join(
    session: Session,
    step: StepSpec,
    coord_step_mats: Dict[Tuple[str, str], Union[MatRef, EphemeralRef, Literal["blocked", "failed", "pending", "filtered"]]],
    params_hash: str,
    force: bool,
    accepts_params: bool,
    pipeline_id: str = "",
) -> List[StepDecision]:
    """Plan an N-way equijoin.

    Bucket each side's lanes by its `join_on` field value (read from the
    index at plan time), then emit one decision per matched tuple — the
    cartesian product of the sides that share a value. Pair coordinate is the
    members' coordinates joined by '|'.
    """
    import itertools

    deps = list(step.join_on.keys())  # type: ignore
    buckets: Dict[str, Dict[str, List[tuple]]] = {dep: {} for dep in deps}
    failed_parents: List[str] = []
    blocked_parents: List[str] = []
    pending = False

    coord_ref_map: Dict[str, List[Tuple[str, Any]]] = {dep: [] for dep in deps}

    for (coord, d), ref in coord_step_mats.items():
        if d not in buckets:
            continue
        if ref == "blocked":
            blocked_parents.append(f"{d}:{coord}")
        elif ref == "failed":
            failed_parents.append(f"{d}:{coord}")
        elif ref == "pending":
            pending = True
        elif ref == "filtered" or getattr(ref, "filtered", False):
            pass
        elif ref is not None:
            coord_ref_map[d].append((coord, ref))

    if failed_parents or blocked_parents:
        if step.on_failed == "block":
            return [
                StepDecision(
                    coordinate="@join",
                    action="blocked",
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            ]
        # use_passed: fall through, computing permutations of surviving lanes
    if pending:
        return [StepDecision(coordinate="@join", action="pending")]

    for d in deps:
        field = step.join_on[d]  # type: ignore
        for coord, ref in coord_ref_map[d]:
            values = _field_values_from_ref(ref, field)
            if not values:
                raise ValueError(
                    f"join step '{step.name}': side '{d}' has no value "
                    f"for join field '{field}' at lane '{coord}'. "
                    f"The field must exist in the parent's output dict."
                )
            for v in values:
                buckets[d].setdefault(v, []).append((coord, ref))

    common = set(buckets[deps[0]])
    for dep in deps[1:]:
        common &= set(buckets[dep])

    combo_list = []
    for value in sorted(common):
        for combo in itertools.product(*[buckets[dep][value] for dep in deps]):
            pair_coord = "|".join(coord for coord, _ in combo)
            parent_mats = {dep: ref for dep, (coord, ref) in zip(deps, combo)}
            input_hash = _compute_step_input_hash(step, pair_coord, "", parent_mats)
            output_address = compute_output_address(
                step.name,
                step.version,
                input_hash,
                params_hash=params_hash if accepts_params else None,
                code_hash=step.code_hash if step.code_mode == "auto" else None,
            )
            combo_list.append((pair_coord, parent_mats, input_hash, output_address))

    if not combo_list:
        if failed_parents or blocked_parents:
            return [
                StepDecision(
                    coordinate="@join",
                    action="blocked",
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            ]
        return []

    addrs = [out_addr for _, _, _, out_addr in combo_list]
    mats_by_addr = {}
    if addrs and pipeline_id:
        from .lane_store import batch_lookup_by_address
        mats_by_addr = batch_lookup_by_address(
            pipeline_id, step.name, set(addrs), session
        )

    decisions: List[StepDecision] = []
    for pair_coord, parent_mats, input_hash, output_address in combo_list:
        existing_mat = mats_by_addr.get(output_address)
        expired = False
        if existing_mat and step.stale_after is not None:
            freshness = existing_mat.get("ts")
            if freshness and iso_age_seconds(freshness) > step.stale_after:
                expired = True

        if existing_mat and not force and not expired:
            decisions.append(
                StepDecision(
                    coordinate=pair_coord,
                    action="reuse",
                    input_hash=input_hash,
                    output_address=output_address,
                    existing=MatRef(
                        existing_mat.get("mat_id") or existing_mat.get("row_id", ""),
                        output_address,
                        existing_mat.get("output_identity", ""),
                        existing_mat.get("content_type"),
                        filtered=existing_mat.get("filtered", False),
                output=existing_mat.get("output"),
                    ),
                    code_drift=(
                        step.code_mode == "warn"
                        and step.code_hash is not None
                        and existing_mat.get("code_hash") is not None
                        and step.code_hash != existing_mat.get("code_hash")
                    ),
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            )
        else:
            decisions.append(
                StepDecision(
                    coordinate=pair_coord,
                    action="execute",
                    input_hash=input_hash,
                    output_address=output_address,
                    parent_mats=parent_mats,
                    stale=expired,
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            )

    return decisions


def _plan_step(
    session: Session,
    step: StepSpec,
    scanned_items: List[RootItem],
    coord_step_mats: Dict[Tuple[str, str], Union[MatRef, EphemeralRef, Literal["blocked", "failed", "pending", "filtered"]]],
    params_hash: str,
    force: bool,
    accepts_params: bool,
    lanes: Optional[List[str]] = None,
    pipeline_id: str = "",
) -> List[StepDecision]:
    """Decide the fate of every coordinate for one step. Read-only.

    `lanes`, when given, restricts planning to that subset of coordinates —
    the deep scheduler's per-lane advance uses it so a lane can be planned
    the moment its own parent commits, without re-deciding siblings. Only
    map-shaped steps support a subset (collective shapes consume whole lane
    sets); the decisions are byte-identical to what whole-step planning
    would produce for those coordinates at the same ledger state.

    `force` is the run-level override (``--force``); `step.check_cache=False`
    is the per-step equivalent. Both make plan skip reuse and emit "execute",
    but the commit path is unaffected — results still land in cache.
    """
    # check_cache=False on the step is a per-step force: skip reuse, still commit.
    force = force or not step.check_cache
    if lanes is not None and step.shape != "map":
        raise ValueError(
            f"lane-subset planning requires shape='map' (step '{step.name}' "
            f"is shape='{step.shape}')"
        )
    if step.shape == "reduce":
        parent_mats: Dict[str, Dict[str, Union[MatRef, EphemeralRef]]] = {dep: {} for dep in step.depends_on}
        failed_parents: List[str] = []
        blocked_parents: List[str] = []
        pending = False

        # Gather every surviving parent lane from coord_step_mats (not just
        # source coordinates), so a reduce also folds in minted/expanded lanes.
        for (coord, d), ref in coord_step_mats.items():
            if d not in parent_mats:
                continue
            if ref == "blocked":
                blocked_parents.append(f"{d}:{coord}")
            elif ref == "failed":
                failed_parents.append(f"{d}:{coord}")
            elif ref == "pending":
                pending = True
            elif ref == "filtered" or getattr(ref, "filtered", False):
                pass
            elif ref is not None:
                parent_mats[d][coord] = ref

        if failed_parents or blocked_parents:
            if step.on_failed == "block":
                return [
                    StepDecision(
                        coordinate="@all",
                        action="blocked",
                        failed_parents=failed_parents,
                        blocked_parents=blocked_parents,
                    )
                ]
            # use_passed: fall through, dropping failed/blocked lanes
            
        if pending:
            return [StepDecision(coordinate="@all", action="pending")]

        if all(not lanes for lanes in parent_mats.values()) and (failed_parents or blocked_parents):
            return [
                StepDecision(
                    coordinate="@all",
                    action="blocked",
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            ]

        # One group ("@all") unless group_key partitions the lanes by a
        # field of the parent output.
        if step.group_key is None:
            groups = {"@all": parent_mats}
        else:
            groups = _group_reduce_lanes(session, step, parent_mats)

        return [
            _reduce_group_decision(
                session, step, gcoord, gmats, params_hash, force, accepts_params,
                failed_parents=failed_parents, blocked_parents=blocked_parents,
                pipeline_id=pipeline_id,
            )
            for gcoord, gmats in sorted(groups.items())
        ]

    if step.shape == "join":
        return _plan_join(
            session, step, coord_step_mats, params_hash, force, accepts_params,
            pipeline_id=pipeline_id,
        )

    if step.shape == "expand" and not step.depends_on:
        # Root expand = source: no parent to cache against, so it always
        # executes (re-scan the world every run). Execution mints the lanes.
        return [StepDecision(coordinate="@root", action="execute", parent_mats={})]

    decisions = []
    
    if not step.depends_on:
        targets = [(it.coordinate, it, it.content_hash) for it in scanned_items]
        if lanes is not None:
            wanted = set(lanes)
            targets = [t for t in targets if t[0] in wanted]
    else:
        if lanes is not None:
            # Per-lane advance: the caller knows exactly which coordinates
            # just resolved, so don't rescan every cell.
            coords = {
                c
                for c in lanes
                if any((c, dep) in coord_step_mats for dep in step.depends_on)
            }
        else:
            coords = set()
            for dep in step.depends_on:
                for (c, d) in coord_step_mats.keys():
                    if d == dep:
                        coords.add(c)

        coord_to_item = {it.coordinate: it for it in scanned_items}
        targets = []
        for c in sorted(coords):
            it = coord_to_item.get(c)
            if not it:
                it = RootItem(coordinate=c, content_hash="")
            targets.append((c, it, it.content_hash))

    resolved_targets = []
    map_addrs = []
    anchor_addrs = []

    for coord, it, sf_content_hash in targets:
        parent_mats: Dict[str, MatRef] = {}  # type: ignore
        failed_parents: List[str] = []  # type: ignore
        blocked_parents: List[str] = []  # type: ignore
        filtered_parents: List[str] = []
        pending = False

        for dep in step.depends_on:
            if (coord, dep) not in coord_step_mats:
                if step.declarative:
                    # Declarative union: a lane only needs to exist in one
                    # parent, not all — skip missing parents
                    continue
                raise ValueError("parents produce disjoint lane sets — a multi-parent map step requires aligned coordinates; use shape='join'")
            parent_mat = coord_step_mats[(coord, dep)]
            if parent_mat == "blocked":
                blocked_parents.append(dep)
            elif parent_mat == "failed":
                failed_parents.append(dep)
            elif parent_mat == "pending":
                pending = True
            elif parent_mat == "filtered" or getattr(parent_mat, "filtered", False):
                filtered_parents.append(dep)
            else:
                parent_mats[dep] = parent_mat  # type: ignore

        if step.skip_cache:
            if failed_parents or blocked_parents:
                coord_step_mats[(coord, step.name)] = "blocked"
            elif filtered_parents:
                coord_step_mats[(coord, step.name)] = "filtered"
            elif pending:
                coord_step_mats[(coord, step.name)] = "pending"
            else:
                identity = {
                    "step": step.name,
                    "version": step.version,
                    "parents": {
                        dep: ref.output_content_hash  # type: ignore
                        for dep, ref in parent_mats.items()
                    }
                    if step.depends_on
                    else {"source": sf_content_hash},
                }
                if accepts_params:
                    identity["params"] = params_hash
                if step.code_mode == "auto":
                    identity["code"] = step.code_hash  # type: ignore
                coord_step_mats[(coord, step.name)] = EphemeralRef(
                    step=step,
                    item=it,
                    parent_refs=parent_mats,
                    identity_hash=hash_json(identity),
                )
            continue

        if failed_parents or blocked_parents:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="blocked",
                    item=it,
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            )
            continue

        if filtered_parents:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="filtered",
                    item=it,
                    filtered_parents=filtered_parents,
                )
            )
            continue

        if pending:
            decisions.append(StepDecision(coordinate=coord, action="pending", item=it))
            continue

        if step.shape == "expand":
            parent_hash = parent_mats[step.depends_on[0]].output_content_hash  # type: ignore
            anchor_address = expand_anchor_address(
                step, parent_hash, params_hash, accepts_params
            )
            anchor_addrs.append(anchor_address)
            resolved_targets.append(("expand", coord, it, parent_mats, anchor_address, parent_hash))
        else:
            input_hash = _compute_step_input_hash(step, coord, sf_content_hash, parent_mats)  # type: ignore
            output_address = compute_output_address(
                step.name,
                step.version,
                input_hash,
                params_hash=params_hash if accepts_params else None,
                code_hash=step.code_hash if step.code_mode == "auto" else None,
            )
            map_addrs.append(output_address)
            resolved_targets.append(("map", coord, it, parent_mats, input_hash, output_address))

    all_addrs = set(map_addrs + anchor_addrs)
    mats_by_addr = {}
    if all_addrs and pipeline_id:
        from .lane_store import batch_lookup_by_address
        mats_by_addr = batch_lookup_by_address(
            pipeline_id, step.name, all_addrs, session
        )

    child_identities_by_target = {}
    all_child_addrs = []

    for kind, *args in resolved_targets:
        if kind == "expand":
            coord, it, parent_mats, anchor_address, parent_hash = args
            if force:
                continue
            anchor = mats_by_addr.get(anchor_address)
            if not anchor:
                continue
            if step.stale_after is not None:
                freshness = anchor.get("ts")
                if freshness and iso_age_seconds(freshness) > step.stale_after:
                    continue
            children_hashes = read_output(anchor.get("output"), anchor.get("content_type"))
            if children_hashes:
                identities = []
                for child_hash in children_hashes:
                    input_hash, child_addr = expand_child_identity(
                        step, child_hash, params_hash, accepts_params
                    )
                    identities.append((child_hash, input_hash, child_addr))
                    all_child_addrs.append(child_addr)
                child_identities_by_target[coord] = identities
            else:
                child_identities_by_target[coord] = []

    child_mats_by_addr = {}
    if all_child_addrs and pipeline_id:
        from .lane_store import batch_lookup_by_address
        child_mats_by_addr = batch_lookup_by_address(
            pipeline_id, step.name, set(all_child_addrs), session
        )

    for kind, *args in resolved_targets:
        if kind == "expand":
            coord, it, parent_mats, anchor_address, parent_hash = args
            identities = child_identities_by_target.get(coord)  # type: ignore
            if identities is None:
                decisions.append(
                    StepDecision(
                        coordinate=coord,
                        action="execute",
                        item=it,
                        parent_mats=parent_mats,
                    )
                )
                continue
            
            incomplete = False
            out = []
            for child_hash, input_hash, child_addr in identities:
                child = child_mats_by_addr.get(child_addr)
                if child is None:
                    incomplete = True
                    break
                out.append(
                    StepDecision(
                        coordinate=expand_child_coord(child_hash),
                        action="reuse",
                        input_hash=input_hash,
                        output_address=child_addr,
                        existing=MatRef(
                            child.get("mat_id") or child.get("row_id", ""),
                            child_addr,
                            child.get("output_identity", ""),
                            child.get("content_type"),
                            filtered=child.get("filtered", False),
                            output=child.get("output"),
                        ),
                    )
                )
            
            if incomplete:
                decisions.append(
                    StepDecision(
                        coordinate=coord,
                        action="execute",
                        item=it,
                        parent_mats=parent_mats,
                    )
                )
            else:
                decisions.extend(out)

        else:
            coord, it, parent_mats, input_hash, output_address = args
            existing_mat = mats_by_addr.get(output_address)

            expired = False
            if existing_mat and step.stale_after is not None:
                freshness = existing_mat.get("ts")
                if freshness and iso_age_seconds(freshness) > step.stale_after:
                    expired = True

            if existing_mat and not force and not expired:
                decisions.append(
                    StepDecision(
                        coordinate=coord,
                        action="reuse",
                        item=it,
                        input_hash=input_hash,
                        output_address=output_address,
                        existing=MatRef(
                            existing_mat.get("mat_id") or existing_mat.get("row_id", ""),
                            output_address,
                            existing_mat.get("output_identity", ""),
                            existing_mat.get("content_type"),
                            filtered=existing_mat.get("filtered", False),
                            output=existing_mat.get("output"),
                        ),
                        code_drift=(
                            step.code_mode == "warn"
                            and step.code_hash is not None
                            and existing_mat.get("code_hash") is not None
                            and step.code_hash != existing_mat.get("code_hash")
                        ),
                    )
                )
            else:
                decisions.append(
                    StepDecision(
                        coordinate=coord,
                        action="execute",
                        item=it,
                        input_hash=input_hash,
                        output_address=output_address,
                        parent_mats=parent_mats,
                        stale=expired,
                    )
                )
    return decisions
