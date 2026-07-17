"""
Invalidation logic for marking outputs as no longer live.
"""
import json
import uuid
from .models import (
    Run,
    InputHashUsage,
    RunEvent,
)
from . import lane_store
from .db import get_session
from .selection import Selection, get_selection_addresses
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
            addresses = get_selection_addresses(session, selection)

            # Downstream closure: seed on the selection's *live* matches
            # (mirrors trace's default seeding), then walk derivation edges
            # with trace's own BFS. Traversal passes through non-live nodes
            # (edges are the truth of derivation) but only live ones flip.
            descendant_addrs: list[str] = []
            if downstream:
                # Live = fulfilled=True.  Filter the seed addresses.
                fulfilled = {
                    str(u.address) for u in session.query(InputHashUsage)
                    .filter(InputHashUsage.fulfilled.is_(True))
                    .all()
                }
                live_seed_addrs = {a for a in addresses if a in fulfilled}
                reached, _ = _bfs(session, live_seed_addrs, downstream=True)
                descendant_addrs = sorted(reached)

            def _flip(addr: str) -> bool:
                # Check liveness via IHU.fulfilled — only flip if currently live.
                usage = (
                    session.query(InputHashUsage)
                    .filter_by(address=addr)
                    .first()
                )
                if usage is None or not usage.fulfilled:
                    return False
                # The tombstone: flip fulfilled=False.
                # The Arrow row stays as history, but the next run sees
                # fulfilled=False and recomputes.  See notes/arrow-storage.md.
                # Also evict from the in-memory fulfilled cache so a
                # subsequent .plan() (without a run in between) sees the
                # invalidation.
                usage.fulfilled = False  # type: ignore
                usage.last_run_id = run_id  # type: ignore
                lane_store.mark_unfulfilled(addr)
                return True

            flipped_addrs: list[str] = []
            seed_count = 0
            for addr in addresses:
                if _flip(addr):
                    seed_count += 1
                    flipped_addrs.append(addr)
            downstream_count = 0
            for addr in descendant_addrs:
                if _flip(addr):
                    downstream_count += 1
                    flipped_addrs.append(addr)
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
                "addresses": flipped_addrs,
            }
        except Exception as e:
            session.rollback()
            run.status = "failed"  # type: ignore
            run.error_message = str(e)  # type: ignore
            run.finished_at = utcnow_iso()  # type: ignore
            session.commit()
            raise e

