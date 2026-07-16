"""
Invalidation logic for marking outputs as no longer live.
"""
import json
import uuid
from .models import (
    Run,
    Materialization,
    InputHashUsage,
    RunCoordinateStatus,
    RunEvent,
)
from . import lane_store
from .db import get_session
from .selection import Selection, get_selection_materialization_ids
from .trace import _bfs
from .util import utcnow_iso


def _mat_lane_key(session, mat_id: int) -> str:
    """Recover the lane_key (coordinate) for a Materialization via the
    run-coordinate join.  Used only during the parallel-write migration;
    once materializations is deleted the lane_key is the primary identity
    and this lookup disappears."""
    row = (
        session.query(RunCoordinateStatus.coordinate)
        .filter(RunCoordinateStatus.materialization_id == mat_id)
        .order_by(RunCoordinateStatus.id.desc())
        .first()
    )
    return row[0] if row else ""


def invalidate(selection: Selection, reason: str, downstream: bool = False) -> dict:
    """
    Invalidate materializations matching the given selection.

    Args:
        selection (Selection): The criteria for what to invalidate.
        reason (str): The reason for invalidation.
        downstream (bool): Also invalidate the full downstream closure of the
            selection's live matches — everything derived from them, walked
            over MaterializationEdge exactly like trace(). Preview the blast
            radius first with trace()/`rubedo trace` on the same selection.
            Upstream is never touched; recovery is lazy (the next run
            recomputes the invalidated lanes).

    Returns:
        dict: A summary of the invalidation run.
    """
    run_id = f"run_{uuid.uuid4().hex[:12]}"

    with get_session() as session:
        # Create invalidate run
        now = utcnow_iso()
        run = Run(
            id=run_id,
            kind="invalidate",
            selection_json=selection.model_dump_json(),
            params_json=json.dumps({"downstream": True}) if downstream else None,
            started_at=now,
            last_heartbeat_at=now,
        )
        session.add(run)

        # Log event
        event = RunEvent(
            run_id=run_id,
            timestamp=utcnow_iso(),
            level="info",
            event_type="run_started",
            message=f"Starting invalidation {run_id}",
        )
        session.add(event)
        session.commit()

        try:
            mat_ids = get_selection_materialization_ids(session, selection)

            # Downstream closure: seed on the selection's *live* matches
            # (mirrors trace's default seeding), then walk derivation edges
            # with trace's own BFS. Traversal passes through non-live nodes
            # (edges are the truth of derivation) but only live ones flip.
            descendant_ids: list[int] = []
            if downstream:
                live_rows = (
                    session.query(Materialization.id)
                    .filter(Materialization.id.in_(mat_ids), Materialization.is_live)
                    .all()
                )
                seed_ids = {int(r.id) for r in live_rows}
                reached, _ = _bfs(session, seed_ids, downstream=True)
                descendant_ids = sorted(reached)

            def _flip(mat_id: int) -> bool:
                mat = session.get(Materialization, mat_id)
                if mat is None or not mat.is_live:
                    return False
                mat.is_live = False  # type: ignore[assignment]
                # The tombstone: flip fulfilled=False on input_hash_usages.
                # The Arrow row stays as history, but the next run sees
                # fulfilled=False and recomputes.  See notes/arrow-storage.md.
                # (MaterializationLifecycle write removed — liveness is
                # input_hash_usages.fulfilled, not is_live + lifecycle rows.)
                lane_key = _mat_lane_key(session, int(mat.id))
                usage = (
                    session.query(InputHashUsage)
                    .filter_by(
                        address=str(mat.output_address),
                        step_name=str(mat.step_name),
                        pipeline_id=str(mat.pipeline_id),
                    )
                    .first()
                )
                if usage:
                    usage.fulfilled = False  # type: ignore
                    usage.last_run_id = run_id  # type: ignore
                    usage.claimed_at = utcnow_iso()  # type: ignore
                else:
                    session.add(
                        InputHashUsage(
                            address=str(mat.output_address),
                            lane_key=lane_key,
                            step_name=str(mat.step_name),
                            pipeline_id=str(mat.pipeline_id),
                            last_run_id=run_id,
                            claimed_at=utcnow_iso(),
                            fulfilled=False,
                        )
                    )
                return True

            flipped_ids: list[int] = []
            seed_count = 0
            for mat_id in mat_ids:
                if _flip(mat_id):
                    seed_count += 1
                    flipped_ids.append(mat_id)
            downstream_count = 0
            for mat_id in descendant_ids:
                if _flip(mat_id):
                    downstream_count += 1
                    flipped_ids.append(mat_id)
            invalidated_count = seed_count + downstream_count

            run.status = "completed"  # type: ignore
            run.finished_at = utcnow_iso()  # type: ignore

            event = RunEvent(
                run_id=run_id,
                timestamp=utcnow_iso(),
                level="info",
                event_type="run_completed",
                message=f"Invalidation {run_id} finished, invalidated {invalidated_count} materializations",
            )
            session.add(event)
            session.commit()
            lane_store.flush_all()

            return {
                "run_id": run_id,
                "invalidated_count": invalidated_count,
                "seed_count": seed_count,
                "downstream_count": downstream_count,
                "materialization_ids": flipped_ids if downstream else mat_ids,
            }
        except Exception as e:
            session.rollback()
            run.status = "failed"  # type: ignore
            run.error_message = str(e)  # type: ignore
            run.finished_at = utcnow_iso()  # type: ignore
            session.commit()
            raise e

