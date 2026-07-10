"""
Invalidation logic for marking outputs as no longer live.
"""
import json
import uuid
from .models import (
    Run,
    Materialization,
    MaterializationLifecycle,
    RunEvent,
)
from .db import get_session
from .selection import Selection, get_selection_materialization_ids
from .trace import _bfs
from .util import utcnow_iso


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
                mat.is_live = False  # type: ignore
                session.add(
                    MaterializationLifecycle(
                        materialization_id=mat.id,
                        action="invalidated",
                        run_id=run_id,
                        reason=reason,
                        created_at=utcnow_iso(),
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

