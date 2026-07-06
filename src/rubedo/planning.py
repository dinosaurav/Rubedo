"""Plan phase: deciding what a run would do, without doing it.

Everything here is read-only with respect to the ledger — the single DB
access is the live-materialization lookup that answers "is this cached?".
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .hashing import compute_output_address, hash_json
from .models import Materialization, MaterializationIndexEntry
from .spec import PipelineSpec, StepSpec
from .sources import SourceItem
from .store import read_materialization_output
from .util import iso_age_seconds


class MatRef:
    """A lightweight reference to a committed materialization."""
    def __init__(
        self,
        id,
        output_address,
        output_content_hash,
        content_type=None,
        filtered=False,
    ):
        self.id = id
        self.output_address = output_address
        self.output_content_hash = output_content_hash
        self.content_type = content_type
        self.filtered = filtered


@dataclass
class EphemeralRef:
    """A skip_cache step's stand-in for a materialization.

    Carries the step's identity (not its output — it hasn't run) so that
    consumers' cache keys can be computed statically, plus everything needed
    to lazily compute the actual value if a consumer executes.
    """

    step: StepSpec
    item: SourceItem
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
    item: Optional[SourceItem] = None
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

    # Multi-parent
    parent_hashes = {
        dep: parent_mats[dep].output_content_hash for dep in sorted(step.depends_on)
    }
    return hash_json(parent_hashes)


def expand_anchor_address(
    step: StepSpec, parent_hash: str, params_hash: str, accepts_params: bool
) -> str:
    """Address of an expand step's cache anchor (the full yielded list).

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


def _plan_expand_reuse(
    session: Session,
    step: StepSpec,
    parent_mats: Dict[str, Any],
    params_hash: str,
    force: bool,
    accepts_params: bool,
) -> Optional[List[StepDecision]]:
    """Cached expansion → child reuse decisions (skip the fn); else None.

    A hit needs the anchor live (and fresh) *and* every child it lists still
    present — a partial cache falls back to re-running the whole expansion.
    """
    if force:
        return None
    parent_hash = parent_mats[step.depends_on[0]].output_content_hash
    anchor = (
        session.query(Materialization)
        .filter_by(
            output_address=expand_anchor_address(
                step, parent_hash, params_hash, accepts_params
            ),
            is_live=True,
        )
        .first()
    )
    if anchor is None:
        return None
    if step.stale_after is not None:
        freshness = anchor.refreshed_at or anchor.created_at
        if iso_age_seconds(freshness) > step.stale_after:
            return None

    out: List[StepDecision] = []
    for child_hash in read_materialization_output(anchor):  # list of content hashes
        input_hash, child_addr = expand_child_identity(
            step, child_hash, params_hash, accepts_params
        )
        child = (
            session.query(Materialization)
            .filter_by(output_address=child_addr, is_live=True)
            .first()
        )
        if child is None:
            return None  # incomplete cache — re-run the expansion
        out.append(
            StepDecision(
                coordinate=expand_child_coord(child_hash),
                action="reuse",
                input_hash=input_hash,
                output_address=child_addr,
                existing=MatRef(
                    child.id,
                    child.output_address,
                    child.output_content_hash,
                    child.content_type,
                    filtered=child.filtered,
                ),
            )
        )
    return out


def _step_accepts_params(step: StepSpec) -> bool:
    """Check if the step function signature accepts a 'params' keyword argument."""
    import inspect

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
        "old code. Bump the version (or use version='auto') to recompute."
    )


def _group_reduce_lanes(
    session: Session, step: StepSpec, parent_mats: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Partition a reduce's parent lanes into groups by an indexed field.

    Reads each lane's `MaterializationIndexEntry` rows for `step.group_key`: a
    lane with several values (a list-valued index) joins each group; a lane
    with none raises, since you cannot group by a field it never indexed.
    """
    groups: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for dep, coord_refs in parent_mats.items():
        for coord, ref in coord_refs.items():
            values = [
                e.value
                for e in session.query(MaterializationIndexEntry)
                .filter_by(materialization_id=ref.id, field=step.group_key)
                .all()
            ]
            if not values:
                raise ValueError(
                    f"reduce step '{step.name}': group_key '{step.group_key}' "
                    f"has no indexed value for lane '{coord}' (parent '{dep}'). "
                    f"Add index=['{step.group_key}'] to that step."
                )
            for v in values:
                groups.setdefault(v, {d: {} for d in step.depends_on})[dep][coord] = ref
    return groups


def _reduce_group_decision(
    session: Session,
    step: StepSpec,
    group_coord: str,
    group_mats: Dict[str, Dict[str, Any]],
    params_hash: str,
    force: bool,
    accepts_params: bool,
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

    existing_mat = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    expired = False
    if existing_mat and step.stale_after is not None:
        freshness = existing_mat.refreshed_at or existing_mat.created_at
        expired = iso_age_seconds(freshness) > step.stale_after

    if existing_mat and not force and not expired:
        return StepDecision(
            coordinate=group_coord,
            action="reuse",
            input_hash=input_hash,
            output_address=output_address,
            existing=MatRef(
                existing_mat.id,
                existing_mat.output_address,
                existing_mat.output_content_hash,
                existing_mat.content_type,
                filtered=existing_mat.filtered,
            ),
            code_drift=(
                step.code_mode == "warn"
                and step.code_hash is not None
                and existing_mat.code_hash is not None
                and step.code_hash != existing_mat.code_hash
            ),
        )
    return StepDecision(
        coordinate=group_coord,
        action="execute",
        input_hash=input_hash,
        output_address=output_address,
        parent_mats=group_mats,
        stale=expired,
    )


def _join_pair_decision(
    session: Session,
    step: StepSpec,
    pair_coord: str,
    parent_mats: Dict[str, Any],
    params_hash: str,
    force: bool,
    accepts_params: bool,
) -> StepDecision:
    """Reuse/execute decision for one matched join tuple (identity = its
    members' content hashes, exactly like a multi-parent map)."""
    input_hash = _compute_step_input_hash(step, pair_coord, "", parent_mats)
    output_address = compute_output_address(
        step.name,
        step.version,
        input_hash,
        params_hash=params_hash if accepts_params else None,
        code_hash=step.code_hash if step.code_mode == "auto" else None,
    )
    existing_mat = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    expired = False
    if existing_mat and step.stale_after is not None:
        freshness = existing_mat.refreshed_at or existing_mat.created_at
        expired = iso_age_seconds(freshness) > step.stale_after

    if existing_mat and not force and not expired:
        return StepDecision(
            coordinate=pair_coord,
            action="reuse",
            input_hash=input_hash,
            output_address=output_address,
            existing=MatRef(
                existing_mat.id,
                existing_mat.output_address,
                existing_mat.output_content_hash,
                existing_mat.content_type,
                filtered=existing_mat.filtered,
            ),
            code_drift=(
                step.code_mode == "warn"
                and step.code_hash is not None
                and existing_mat.code_hash is not None
                and step.code_hash != existing_mat.code_hash
            ),
        )
    return StepDecision(
        coordinate=pair_coord,
        action="execute",
        input_hash=input_hash,
        output_address=output_address,
        parent_mats=parent_mats,
        stale=expired,
    )


def _plan_join(
    session: Session,
    step: StepSpec,
    coord_step_mats: Dict[tuple, Any],
    params_hash: str,
    force: bool,
    accepts_params: bool,
) -> List[StepDecision]:
    """Plan an N-way equijoin.

    Bucket each side's lanes by its `join_on` field value (read from the
    index at plan time), then emit one decision per matched tuple — the
    cartesian product of the sides that share a value. Pair coordinate is the
    members' coordinates joined by '|'.
    """
    import itertools

    deps = list(step.join_on.keys())
    buckets: Dict[str, Dict[str, List[tuple]]] = {dep: {} for dep in deps}
    failed_parents: List[str] = []
    blocked_parents: List[str] = []
    pending = False

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
            field = step.join_on[d]
            values = [
                e.value
                for e in session.query(MaterializationIndexEntry)
                .filter_by(materialization_id=ref.id, field=field)
                .all()
            ]
            if not values:
                raise ValueError(
                    f"join step '{step.name}': side '{d}' has no indexed value "
                    f"for join field '{field}' at lane '{coord}'. Add "
                    f"index=['{field}'] to that step."
                )
            for v in values:
                buckets[d].setdefault(v, []).append((coord, ref))

    if failed_parents or blocked_parents:
        return [
            StepDecision(
                coordinate="@join",
                action="blocked",
                failed_parents=failed_parents,
                blocked_parents=blocked_parents,
            )
        ]
    if pending:
        return [StepDecision(coordinate="@join", action="pending")]

    common = set(buckets[deps[0]])
    for dep in deps[1:]:
        common &= set(buckets[dep])

    decisions: List[StepDecision] = []
    for value in sorted(common):
        for combo in itertools.product(*[buckets[dep][value] for dep in deps]):
            pair_coord = "|".join(coord for coord, _ in combo)
            parent_mats = {dep: ref for dep, (coord, ref) in zip(deps, combo)}
            decisions.append(
                _join_pair_decision(
                    session, step, pair_coord, parent_mats,
                    params_hash, force, accepts_params,
                )
            )
    return decisions


def _plan_step(
    session: Session,
    step: StepSpec,
    scanned_items: List[SourceItem],
    coord_step_mats: Dict[tuple, Any],
    params_hash: str,
    force: bool,
    accepts_params: bool,
) -> List[StepDecision]:
    """Decide the fate of every coordinate for one step. Read-only."""
    if step.shape == "reduce":
        parent_mats: Dict[str, Dict[str, Any]] = {dep: {} for dep in step.depends_on}
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
            return [
                StepDecision(
                    coordinate="@all",
                    action="blocked",
                    failed_parents=failed_parents,
                    blocked_parents=blocked_parents,
                )
            ]
        if pending:
            return [StepDecision(coordinate="@all", action="pending")]

        # One group ("@all") unless group_key partitions the lanes by an
        # indexed field of the parent output.
        if step.group_key is None:
            groups = {"@all": parent_mats}
        else:
            groups = _group_reduce_lanes(session, step, parent_mats)

        return [
            _reduce_group_decision(
                session, step, gcoord, gmats, params_hash, force, accepts_params
            )
            for gcoord, gmats in sorted(groups.items())
        ]

    if step.shape == "join":
        return _plan_join(
            session, step, coord_step_mats, params_hash, force, accepts_params
        )

    if step.shape == "expand" and not step.depends_on:
        # Root expand = source: no parent to cache against, so it always
        # executes (re-scan the world every run). Execution mints the lanes.
        return [StepDecision(coordinate="@root", action="execute", parent_mats={})]

    decisions = []
    
    if not step.depends_on:
        targets = [(it.coordinate, it, it.content_hash) for it in scanned_items]
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
                from .sources import SourceItem
                it = SourceItem(coordinate=c, content_hash="", metadata={})
            targets.append((c, it, it.content_hash))

    for coord, it, sf_content_hash in targets:
        parent_mats: Dict[str, MatRef] = {}
        failed_parents: List[str] = []
        blocked_parents: List[str] = []
        filtered_parents: List[str] = []
        pending = False

        for dep in step.depends_on:
            parent_mat = coord_step_mats.get((coord, dep))
            if parent_mat == "blocked":
                blocked_parents.append(dep)
            elif parent_mat == "failed":
                failed_parents.append(dep)
            elif parent_mat == "pending":
                pending = True
            elif parent_mat == "filtered" or getattr(parent_mat, "filtered", False):
                filtered_parents.append(dep)
            else:
                parent_mats[dep] = parent_mat

        if step.skip_cache:
            # Inline util: no decision, no ledger row. Its identity becomes
            # part of every consumer's cache key; its value is computed
            # lazily (memoized) only if a consumer executes.
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
                        dep: ref.output_content_hash
                        for dep, ref in parent_mats.items()
                    }
                    if step.depends_on
                    else {"source": sf_content_hash},
                }
                if accepts_params:
                    identity["params"] = params_hash
                if step.code_mode == "auto":
                    identity["code"] = step.code_hash
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
            # Cached expansion (anchor + all children live) replays the child
            # lanes without running the fn; otherwise one execute decision per
            # parent lane mints the children (execution writes the anchor).
            reuse = _plan_expand_reuse(
                session, step, parent_mats, params_hash, force, accepts_params
            )
            if reuse is not None:
                decisions.extend(reuse)
            else:
                decisions.append(
                    StepDecision(
                        coordinate=coord,
                        action="execute",
                        item=it,
                        parent_mats=parent_mats,
                    )
                )
            continue

        input_hash = _compute_step_input_hash(step, coord, sf_content_hash, parent_mats)
        output_address = compute_output_address(
            step.name,
            step.version,
            input_hash,
            # Params are part of a step's cache identity only if the step
            # consumes them; downstream steps pick up param changes through
            # the content-hash chain
            params_hash=params_hash if accepts_params else None,
            # code='auto': source edits change identity; code='warn': they
            # don't, but reuse of outdated outputs is flagged
            code_hash=step.code_hash if step.code_mode == "auto" else None,
        )

        existing_mat = (
            session.query(Materialization)
            .filter_by(output_address=output_address, is_live=True)
            .first()
        )

        expired = False
        if existing_mat and step.stale_after is not None:
            freshness = existing_mat.refreshed_at or existing_mat.created_at
            expired = iso_age_seconds(freshness) > step.stale_after

        if existing_mat and not force and not expired:
            decisions.append(
                StepDecision(
                    coordinate=coord,
                    action="reuse",
                    item=it,
                    input_hash=input_hash,
                    output_address=output_address,
                    existing=MatRef(
                        existing_mat.id,
                        existing_mat.output_address,
                        existing_mat.output_content_hash,
                        existing_mat.content_type,
                        filtered=existing_mat.filtered,
                    ),
                    code_drift=(
                        step.code_mode == "warn"
                        and step.code_hash is not None
                        and existing_mat.code_hash is not None
                        and step.code_hash != existing_mat.code_hash
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
