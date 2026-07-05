"""Plan phase: deciding what a run would do, without doing it.

Everything here is read-only with respect to the ledger — the single DB
access is the live-materialization lookup that answers "is this cached?".
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from .hashing import compute_output_address, hash_json
from .models import Materialization
from .spec import PipelineSpec, StepSpec
from .sources import SourceItem
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
        filtered_parents: List[str] = []
        pending = False

        for it in scanned_items:
            coord = it.coordinate
            for dep in step.depends_on:
                ref = coord_step_mats.get((coord, dep))
                if ref == "blocked":
                    blocked_parents.append(f"{dep}:{coord}")
                elif ref == "failed":
                    failed_parents.append(f"{dep}:{coord}")
                elif ref == "pending":
                    pending = True
                elif ref == "filtered" or getattr(ref, "filtered", False):
                    pass
                elif ref is not None:
                    parent_mats[dep][coord] = ref

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
            
        hash_data = {}
        for dep, coords_dict in sorted(parent_mats.items()):
            hash_data[dep] = {
                c: coords_dict[c].output_content_hash for c in sorted(coords_dict.keys())
            }
        input_hash = hash_json(hash_data)
        
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
            return [
                StepDecision(
                    coordinate="@all",
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
            ]
        else:
            return [
                StepDecision(
                    coordinate="@all",
                    action="execute",
                    input_hash=input_hash,
                    output_address=output_address,
                    parent_mats=parent_mats,
                    stale=expired,
                )
            ]

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
            # Coordinate-minting: the child lanes are unknowable until the fn
            # runs, so there is exactly one execute decision per parent lane
            # (no address, no reuse in the MVP). Execution mints the child
            # coordinates and their materializations.
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
