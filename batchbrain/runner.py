import uuid
import json
import traceback
import concurrent.futures
from typing import Any, Optional, List, Dict

from sqlalchemy.orm import Session

from .models import (
    Run,
    RunEvent,
    Manifest,
    ManifestEntry,
    Materialization,
    MaterializationEdge,
    MaterializationLifecycle,
    RunCoordinateStatus,
    RunSummary,
    ProcessResult,
)
from .registry import PipelineSpec, StepSpec
from .db import get_session
from .sources import Source, coerce_source
from .hashing import hash_json, compute_output_address
from .store import stage_and_commit, read_materialization_output
from .util import utcnow_iso


class MatRef:
    def __init__(self, id, output_address, output_content_hash, content_type=None):
        self.id = id
        self.output_address = output_address
        self.output_content_hash = output_content_hash
        self.content_type = content_type


def _emit_event(
    session: Session,
    run_id: str,
    level: str,
    event_type: str,
    processor_name: Optional[str] = None,
    step_name: Optional[str] = None,
    coordinate: Optional[str] = None,
    message: Optional[str] = None,
    data: Optional[dict] = None,
):
    event = RunEvent(
        run_id=run_id,
        timestamp=utcnow_iso(),
        level=level,
        event_type=event_type,
        processor_name=processor_name,
        step_name=step_name,
        coordinate=coordinate,
        message=message,
        data_json=json.dumps(data) if data else None,
    )
    session.add(event)


def topological_sort(pipeline: PipelineSpec) -> List[StepSpec]:
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


def _commit_materialization(
    session: Session,
    *,
    processor_name: str,
    step: StepSpec,
    input_hash: str,
    output_address: str,
    output_content_hash: str,
    content_type: str,
    output_path: str,
    metadata_json: Optional[str],
    run_id: str,
) -> tuple[Materialization, str]:
    """Record an executed result at its address, honoring generations.

    An address may accumulate generations over time; at most one is live.
    Identical bytes are the same fact (reuse the live row, or restore a
    non-live one); different bytes supersede the live generation so that
    downstream input hashes change and dependents recompute. Every liveness
    transition appends a materialization_lifecycle row — the append-only
    truth that the is_live projection caches.

    Returns (materialization, action) with action one of
    created | reused | restored | superseded.
    """
    live = (
        session.query(Materialization)
        .filter_by(output_address=output_address, is_live=True)
        .first()
    )
    superseded = None
    if live:
        if live.output_content_hash == output_content_hash:
            return live, "reused"
        live.is_live = False
        # Demote before inserting the replacement so the one-live-per-address
        # index never sees two live generations
        session.flush()
        superseded = live
    else:
        prior = (
            session.query(Materialization)
            .filter_by(
                output_address=output_address,
                output_content_hash=output_content_hash,
                is_live=False,
            )
            .order_by(Materialization.id.desc())
            .first()
        )
        if prior:
            prior.is_live = True
            session.add(
                MaterializationLifecycle(
                    materialization_id=prior.id,
                    action="restored",
                    run_id=run_id,
                    reason="recompute produced identical output",
                    created_at=utcnow_iso(),
                )
            )
            return prior, "restored"

    mat = Materialization(
        processor_name=processor_name,
        step_name=step.name,
        code_version=step.version,
        config_hash=step.config_hash,
        input_hash=input_hash,
        output_address=output_address,
        output_content_hash=output_content_hash,
        content_type=content_type,
        output_path=output_path,
        metadata_json=metadata_json,
        created_at=utcnow_iso(),
        created_by_run_id=run_id,
        is_live=True,
    )
    session.add(mat)
    session.flush()

    if superseded is not None:
        session.add(
            MaterializationLifecycle(
                materialization_id=superseded.id,
                action="superseded",
                run_id=run_id,
                reason="recompute produced different output",
                superseded_by_id=mat.id,
                created_at=utcnow_iso(),
            )
        )
        return mat, "superseded"
    return mat, "created"


def _step_accepts_inputs(step: StepSpec) -> bool:
    import inspect

    return "inputs" in inspect.signature(step.fn).parameters


def _build_step_inputs(step: StepSpec, inputs: Optional[dict]):
    if step.input_model:
        return step.input_model(**(inputs or {}))
    return inputs or {}


def run_pipeline(
    pipeline: PipelineSpec,
    source: Optional[Source | str] = None,
    config: Optional[dict[str, Any]] = None,
    workers: Optional[int] = None,
    force: bool = False,
    inputs: Optional[dict] = None,
) -> RunSummary:
    source = pipeline.source if source is None else coerce_source(source)
    source_id = source.id
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    config = config or {}
    config_hash = hash_json(config)
    processor_name = pipeline.id

    topo_steps = topological_sort(pipeline)

    with get_session() as session:
        run = Run(
            id=run_id,
            kind="process",
            status="running",
            processor_name=processor_name,
            source_id=source_id,
            config_hash=config_hash,
            started_at=utcnow_iso(),
        )
        session.add(run)
        _emit_event(
            session,
            run_id,
            "info",
            "run_started",
            processor_name=processor_name,
            message=f"Starting run {run_id}",
        )
        session.commit()

        try:
            scanned_items = source.scan()

            now_iso = utcnow_iso()
            manifest_id = f"manifest_{uuid.uuid4().hex[:12]}"

            sorted_items = sorted(scanned_items, key=lambda x: x.coordinate)
            manifest_data = [
                {"coordinate": it.coordinate, "hash": it.content_hash}
                for it in sorted_items
            ]
            manifest_hash_val = hash_json(manifest_data)

            prev_manifest = (
                session.query(Manifest)
                .filter(Manifest.source_id == source_id)
                .order_by(Manifest.created_at.desc())
                .first()
            )

            manifest = Manifest(
                id=manifest_id,
                run_id=run_id,
                source_id=source_id,
                manifest_hash=manifest_hash_val,
                parent_manifest_id=prev_manifest.id if prev_manifest else None,
                created_at=now_iso,
            )
            session.add(manifest)

            for it in scanned_items:
                entry = ManifestEntry(
                    manifest_id=manifest_id,
                    coordinate=it.coordinate,
                    content_hash=it.content_hash,
                    size_bytes=it.metadata.get("size_bytes"),
                    mtime_ns=it.metadata.get("mtime_ns"),
                )
                session.add(entry)

            _emit_event(
                session,
                run_id,
                "info",
                "manifest_created",
                processor_name=processor_name,
                data={
                    "manifest_id": manifest_id,
                    "parent_manifest_id": manifest.parent_manifest_id,
                },
            )
            session.commit()

            scanned_coordinates = {it.coordinate for it in scanned_items}

            removed_count = 0
            if prev_manifest:
                prev_entries = (
                    session.query(ManifestEntry)
                    .filter_by(manifest_id=prev_manifest.id)
                    .all()
                )
                for pe in prev_entries:
                    if pe.coordinate not in scanned_coordinates:
                        for step in topo_steps:
                            last_rc = (
                                session.query(RunCoordinateStatus)
                                .filter(
                                    RunCoordinateStatus.processor_name
                                    == processor_name,
                                    RunCoordinateStatus.source_id == source_id,
                                    RunCoordinateStatus.coordinate == pe.coordinate,
                                    RunCoordinateStatus.step_name == step.name,
                                    RunCoordinateStatus.status.in_(
                                        ["created", "reused"]
                                    ),
                                )
                                .order_by(RunCoordinateStatus.id.desc())
                                .first()
                            )

                            rc = RunCoordinateStatus(
                                run_id=run_id,
                                processor_name=processor_name,
                                step_name=step.name,
                                source_id=source_id,
                                coordinate=pe.coordinate,
                                input_hash=pe.content_hash,  # Best approximation for removed
                                previous_output_address=last_rc.output_address
                                if last_rc
                                else None,
                                previous_materialization_id=last_rc.materialization_id
                                if last_rc
                                else None,
                                status="removed",
                                created_at=utcnow_iso(),
                            )
                            session.add(rc)

                            _emit_event(
                                session,
                                run_id,
                                "info",
                                "coordinate_removed",
                                processor_name=processor_name,
                                step_name=step.name,
                                coordinate=pe.coordinate,
                                message="Coordinate removed because it is absent from the latest manifest",
                            )
                        removed_count += 1
            session.commit()

            total_summary = {
                "created": 0,
                "reused": 0,
                "failed": 0,
                "removed": removed_count,
                "blocked": 0,
            }
            step_summary = {
                s.name: {
                    "created": 0,
                    "reused": 0,
                    "failed": 0,
                    "removed": removed_count,
                    "blocked": 0,
                }
                for s in topo_steps
            }

            # Dictionary mapping (coordinate, step_name) -> materialized Object or 'failed' or 'blocked'
            coord_step_mats = {}

            for step in topo_steps:
                tasks = []
                accepts_inputs = _step_accepts_inputs(step)
                for it in scanned_items:
                    coord = it.coordinate

                    parent_mats = {}
                    failed_parents = []
                    blocked_parents = []

                    for dep in step.depends_on:
                        parent_mat = coord_step_mats.get((coord, dep))
                        if parent_mat == "blocked":
                            blocked_parents.append(dep)
                        elif parent_mat == "failed":
                            failed_parents.append(dep)
                        else:
                            parent_mats[dep] = parent_mat

                    if failed_parents or blocked_parents:
                        coord_step_mats[(coord, step.name)] = "blocked"
                        rc = RunCoordinateStatus(
                            run_id=run_id,
                            processor_name=processor_name,
                            step_name=step.name,
                            source_id=source_id,
                            coordinate=coord,
                            status="blocked",
                            metadata_json=json.dumps(
                                {
                                    "failed_parents": failed_parents,
                                    "blocked_parents": blocked_parents,
                                }
                            ),
                            created_at=utcnow_iso(),
                        )
                        session.add(rc)
                        _emit_event(
                            session,
                            run_id,
                            "info",
                            "step_blocked",
                            processor_name=processor_name,
                            step_name=step.name,
                            coordinate=coord,
                        )
                        total_summary["blocked"] += 1
                        step_summary[step.name]["blocked"] += 1
                        continue

                    input_hash = _compute_step_input_hash(
                        step, coord, it.content_hash, parent_mats
                    )
                    output_address = compute_output_address(
                        step.name, step.version, input_hash, step.config_hash
                    )

                    existing_mat = (
                        session.query(Materialization)
                        .filter_by(output_address=output_address, is_live=True)
                        .first()
                    )

                    if existing_mat and not force:
                        coord_step_mats[(coord, step.name)] = MatRef(
                            existing_mat.id,
                            existing_mat.output_address,
                            existing_mat.output_content_hash,
                            existing_mat.content_type,
                        )
                        rc = RunCoordinateStatus(
                            run_id=run_id,
                            processor_name=processor_name,
                            step_name=step.name,
                            source_id=source_id,
                            coordinate=coord,
                            input_hash=input_hash,
                            output_address=output_address,
                            materialization_id=existing_mat.id,
                            status="reused",
                            created_at=utcnow_iso(),
                        )
                        session.add(rc)
                        _emit_event(
                            session,
                            run_id,
                            "info",
                            "step_cache_hit",
                            processor_name=processor_name,
                            step_name=step.name,
                            coordinate=coord,
                        )
                        total_summary["reused"] += 1
                        step_summary[step.name]["reused"] += 1
                    else:
                        _emit_event(
                            session,
                            run_id,
                            "info",
                            "step_processing_started",
                            processor_name=processor_name,
                            step_name=step.name,
                            coordinate=coord,
                        )

                        tasks.append(
                            {
                                "coordinate": coord,
                                "item": it,
                                "input_hash": input_hash,
                                "output_address": output_address,
                                "parent_mats": parent_mats,
                            }
                        )

                session.commit()

                if tasks:

                    def process_task(task_spec):
                        try:
                            # Root steps get the source payload positionally;
                            # dependent steps get parent outputs by parameter
                            # name. Either kind may declare an `inputs` param.
                            if not step.depends_on:
                                args = [source.load(task_spec["item"])]
                                kwargs = {}
                            else:
                                args = []
                                kwargs = {
                                    dep: read_materialization_output(
                                        task_spec["parent_mats"][dep]
                                    )
                                    for dep in step.depends_on
                                }
                            if accepts_inputs:
                                kwargs["inputs"] = _build_step_inputs(step, inputs)
                            result = step.fn(*args, **kwargs)
                            return True, task_spec, result, None
                        except Exception:
                            return False, task_spec, None, traceback.format_exc()

                    with concurrent.futures.ThreadPoolExecutor(
                        max_workers=workers or step.workers
                    ) as executor:
                        futures = [executor.submit(process_task, t) for t in tasks]
                        for future in concurrent.futures.as_completed(futures):
                            success, task_spec, result, error_trace = future.result()
                            coord = task_spec["coordinate"]
                            output_address = task_spec["output_address"]
                            input_hash = task_spec["input_hash"]

                            with get_session() as task_session:
                                if success:
                                    try:
                                        final_path, output_content_hash, content_type = (
                                            stage_and_commit(run_id, coord, result)
                                        )
                                        metadata_json = None
                                        if (
                                            isinstance(result, ProcessResult)
                                            and result.metadata
                                        ):
                                            metadata_json = json.dumps(result.metadata)

                                        mat, mat_action = _commit_materialization(
                                            task_session,
                                            processor_name=processor_name,
                                            step=step,
                                            input_hash=input_hash,
                                            output_address=output_address,
                                            output_content_hash=output_content_hash,
                                            content_type=content_type,
                                            output_path=final_path,
                                            metadata_json=metadata_json,
                                            run_id=run_id,
                                        )

                                        for dep_name, p_mat in task_spec[
                                            "parent_mats"
                                        ].items():
                                            # A reused or resurrected generation
                                            # already has its lineage edges
                                            edge_exists = (
                                                task_session.query(MaterializationEdge)
                                                .filter_by(
                                                    parent_id=p_mat.id,
                                                    child_id=mat.id,
                                                )
                                                .first()
                                            )
                                            if not edge_exists:
                                                task_session.add(
                                                    MaterializationEdge(
                                                        parent_id=p_mat.id,
                                                        child_id=mat.id,
                                                    )
                                                )

                                        rc = RunCoordinateStatus(
                                            run_id=run_id,
                                            processor_name=processor_name,
                                            step_name=step.name,
                                            source_id=source_id,
                                            coordinate=coord,
                                            input_hash=input_hash,
                                            output_address=output_address,
                                            materialization_id=mat.id,
                                            status="created",
                                            created_at=utcnow_iso(),
                                        )
                                        task_session.add(rc)

                                        _emit_event(
                                            task_session,
                                            run_id,
                                            "info",
                                            f"materialization_{mat_action}",
                                            processor_name=processor_name,
                                            step_name=step.name,
                                            coordinate=coord,
                                            data={"materialization_id": mat.id},
                                        )
                                        total_summary["created"] += 1
                                        step_summary[step.name]["created"] += 1
                                        coord_step_mats[(coord, step.name)] = MatRef(
                                            mat.id,
                                            mat.output_address,
                                            mat.output_content_hash,
                                            mat.content_type,
                                        )

                                    except Exception as e:
                                        task_session.rollback()
                                        error_msg = traceback.format_exc()
                                        rc = RunCoordinateStatus(
                                            run_id=run_id,
                                            processor_name=processor_name,
                                            step_name=step.name,
                                            source_id=source_id,
                                            coordinate=coord,
                                            input_hash=input_hash,
                                            output_address=output_address,
                                            status="failed",
                                            error_message=error_msg,
                                            error_type="StagingError",
                                            created_at=utcnow_iso(),
                                        )
                                        task_session.add(rc)
                                        _emit_event(
                                            task_session,
                                            run_id,
                                            "error",
                                            "step_failed",
                                            processor_name=processor_name,
                                            step_name=step.name,
                                            coordinate=coord,
                                            message=str(e),
                                        )
                                        total_summary["failed"] += 1
                                        step_summary[step.name]["failed"] += 1
                                        coord_step_mats[(coord, step.name)] = "failed"
                                else:
                                    rc = RunCoordinateStatus(
                                        run_id=run_id,
                                        processor_name=processor_name,
                                        step_name=step.name,
                                        source_id=source_id,
                                        coordinate=coord,
                                        input_hash=input_hash,
                                        output_address=output_address,
                                        status="failed",
                                        error_message=error_trace,
                                        error_type="ExecutionError",
                                        created_at=utcnow_iso(),
                                    )
                                    task_session.add(rc)
                                    _emit_event(
                                        task_session,
                                        run_id,
                                        "error",
                                        "step_failed",
                                        processor_name=processor_name,
                                        step_name=step.name,
                                        coordinate=coord,
                                        message=error_trace,
                                    )
                                    total_summary["failed"] += 1
                                    step_summary[step.name]["failed"] += 1
                                    coord_step_mats[(coord, step.name)] = "failed"

                                task_session.commit()

            # Finish run
            full_summary = {
                "created": total_summary["created"],
                "reused": total_summary["reused"],
                "failed": total_summary["failed"],
                "removed": total_summary["removed"],
                "blocked": total_summary["blocked"],
                "total": total_summary,
                "by_step": step_summary,
            }

            with get_session() as final_session:
                final_run = final_session.query(Run).filter_by(id=run_id).first()
                if total_summary["failed"] == 0 and total_summary["blocked"] == 0:
                    final_run.status = "completed"
                elif total_summary["created"] == 0 and total_summary["reused"] == 0:
                    final_run.status = "failed"
                else:
                    final_run.status = "completed_with_failures"

                final_status = final_run.status
                final_run.finished_at = utcnow_iso()
                final_run.summary_json = json.dumps(full_summary)
                _emit_event(
                    final_session,
                    run_id,
                    "info",
                    "run_completed" if final_status != "failed" else "run_failed",
                    processor_name=processor_name,
                    message=f"Run finished with status {final_run.status}",
                )
                final_session.commit()

            return RunSummary(
                run_id=run_id,
                status=final_status,
                created_count=total_summary["created"],
                reused_count=total_summary["reused"],
                failed_count=total_summary["failed"],
                removed_count=total_summary["removed"],
            )

        except Exception as e:
            with get_session() as err_session:
                err_run = err_session.query(Run).filter_by(id=run_id).first()
                if err_run:
                    err_run.status = "failed"
                    err_run.error_message = traceback.format_exc()
                    err_run.finished_at = utcnow_iso()
                    _emit_event(
                        err_session,
                        run_id,
                        "error",
                        "run_failed",
                        processor_name=processor_name,
                        message=str(e),
                    )
                    err_session.commit()
            raise
