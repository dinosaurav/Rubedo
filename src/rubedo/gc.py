"""Retention GC: the byte-deleting garbage collector.

In the new model (notes/arrow-storage.md), "live" = fulfilled=True in
input_hash_usages.  Demote = flip fulfilled=False (same mechanism as
invalidation).  The sweep still refcounts object store entries via
content_hash — but the content hashes now come from the Arrow lane_store
rows (via the parallel-written Materialization rows during the transition),
not from a SQLite is_live column.

Two policies, no others: per-pipeline keep-last-N-runs
(`pipeline(retention=N)`) and a global byte budget (`gc(max_bytes=...)`).
Both run through the same two phases:

  demote  Flip fulfilled=False on input_hash_usages entries outside the
          keep-set.  The old Materialization.is_live flip + lifecycle row
          still fires (parallel write, transitional).
  sweep   Delete an object file only when *every* reference is non-live
          once demote is applied.  Log in object_reclamations.

Expand anchors: always kept (pruning one would silently re-run the
expand fn).  Under the new model, anchors are input_hash_usages entries
with fulfilled=True that have no RunCoordinateStatus reference — same
structural detection, different substrate.
"""

import os
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .db import get_session
from .models import (
    InputHashUsage,
    Materialization,
    MaterializationLifecycle,
    ObjectReclamation,
    Run,
    RunCoordinateStatus,
    effective_run_status,
)
from .store import _get_object_path
from .util import utcnow_iso

# Warn-only threshold: at the end of an *unconfigured* run, if the store is
# bigger than this, print one line pointing at retention= / rubedo gc.
DEFAULT_WARN_THRESHOLD_BYTES = 1024 * 1024 * 1024  # 1 GiB

# How long a cached cheap store-size estimate stays valid, so the warn check
# never pays a full per-object stat walk on every run of a huge store.
_STORE_SIZE_CACHE_TTL_SECONDS = 3600.0


@dataclass
class GcReport:
    """What a gc()/auto-prune pass did (or, dry-run, would do).

    demoted_mat_ids and reclaimed are computed identically for dry-run and
    delete, so a dry-run lists exactly what a subsequent --delete performs.
    """

    applied: bool  # True if writes were performed (delete=True and not refused)
    demoted_mat_ids: List[int] = field(default_factory=list)
    # (content_hash, bytes) for each object swept (or that would be swept)
    reclaimed: List[Tuple[str, int]] = field(default_factory=list)
    refused: Optional[str] = None  # non-None if a live run blocked deletion
    max_bytes: Optional[int] = None
    total_bytes_before: int = 0

    @property
    def reclaimed_bytes(self) -> int:
        return sum(b for _, b in self.reclaimed)

    @property
    def demoted_count(self) -> int:
        return len(self.demoted_mat_ids)

    def __str__(self) -> str:
        verb = "Pruned" if self.applied else "Would prune"
        if self.refused:
            return f"GC refused: {self.refused}"
        lines = [
            f"{verb} {self.demoted_count} materialization(s); "
            f"{verb.lower()} {len(self.reclaimed)} object(s) "
            f"/ {self.reclaimed_bytes} bytes"
            + ("" if self.applied else " (dry-run — nothing deleted)")
        ]
        if self.max_bytes is not None:
            projected = self.total_bytes_before - self.reclaimed_bytes
            lines.append(
                f"  budget: {self.total_bytes_before} B before -> "
                f"~{projected} B after (max {self.max_bytes} B)"
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ledger reads: keep-sets, anchors, running runs
# ---------------------------------------------------------------------------


def _running_run_ids(session: Session, exclude_run_id: Optional[str] = None) -> List[str]:
    """Run ids whose effective status is 'running' (fresh heartbeat, no
    terminal status), excluding one run (the caller's own, for auto-prune)."""
    live: List[str] = []
    for run in session.query(Run).filter(Run.status.is_(None)).all():
        if run.id == exclude_run_id:
            continue
        if effective_run_status(run) == "running":
            live.append(str(run.id))
    return live


def _anchor_mat_ids(session: Session) -> Set[int]:
    """Live materializations referenced by *no* RunCoordinateStatus — exactly
    the expand cache anchors.  Under the new model, 'live' = fulfilled=True
    in input_hash_usages; the mat_id comes from the parallel-written
    Materialization row (transitional)."""
    referenced = {
        int(r.materialization_id)
        for r in session.query(RunCoordinateStatus.materialization_id)
        .filter(RunCoordinateStatus.materialization_id.isnot(None))
        .distinct()
        .all()
    }
    # Live = fulfilled=True in input_hash_usages.  Cross-reference with
    # Materialization for the integer ids (transitional — once
    # materializations is deleted, anchors are detected by: fulfilled=True
    # AND no RCS row for the same address).
    fulfilled_addrs = {
        u.address for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }
    live_ids = {
        int(m.id)
        for m in session.query(Materialization.id)
        .filter(
            Materialization.output_address.in_(fulfilled_addrs),
            Materialization.is_live.is_(True),
        )
        .all()
    }
    return live_ids - referenced


def _terminal_runs(session: Session, pipeline_id: str, limit: Optional[int] = None) -> List[Run]:
    """A pipeline's terminal runs (status set), newest first."""
    q = (
        session.query(Run)
        .filter(Run.pipeline_id == pipeline_id, Run.status.isnot(None))
        .order_by(Run.started_at.desc(), Run.id.desc())
    )
    if limit is not None:
        q = q.limit(limit)
    return q.all()


def _mat_ids_for_runs(session: Session, run_ids: List[str]) -> Set[int]:
    """Materializations any of these runs referenced (created or reused)."""
    if not run_ids:
        return set()
    rows = (
        session.query(RunCoordinateStatus.materialization_id)
        .filter(
            RunCoordinateStatus.run_id.in_(run_ids),
            RunCoordinateStatus.materialization_id.isnot(None),
        )
        .distinct()
        .all()
    )
    return {int(r.materialization_id) for r in rows}


def retention_policies(session: Session) -> Dict[str, int]:
    """Each pipeline's retention N, read from its latest run's definition_json.

    Never imports user code (same rule as server.py) — the policy rides the
    definition snapshot the run already recorded.
    """
    import json

    policies: Dict[str, int] = {}
    seen: Set[str] = set()
    # Latest run per pipeline first: order by started_at desc and take the
    # first definition we see for each pipeline id.
    for run in (
        session.query(Run)
        .filter(Run.pipeline_id.isnot(None), Run.definition_json.isnot(None))
        .order_by(Run.started_at.desc(), Run.id.desc())
        .all()
    ):
        pid = str(run.pipeline_id)
        if pid in seen:
            continue
        seen.add(pid)
        try:
            n = json.loads(str(run.definition_json)).get("retention")
        except (ValueError, TypeError):
            n = None
        if isinstance(n, int) and not isinstance(n, bool) and n >= 1:
            policies[pid] = n
    return policies


# ---------------------------------------------------------------------------
# Demote/sweep planning (pure — no writes)
# ---------------------------------------------------------------------------


def _retention_demote_ids(
    session: Session, policies: Dict[str, int], anchors: Set[int]
) -> Set[int]:
    """Live materializations to demote so each pipeline keeps only its last N
    runs' outputs.  Under the new model, 'live' = fulfilled=True in
    input_hash_usages, cross-referenced with Materialization for integer ids
    (transitional)."""
    demote: Set[int] = set()
    for pipeline_id, n in policies.items():
        runs = _terminal_runs(session, pipeline_id, limit=n)
        keep = _mat_ids_for_runs(session, [str(r.id) for r in runs]) | anchors
        # Live = fulfilled=True for this pipeline
        fulfilled_addrs = {
            u.address for u in session.query(InputHashUsage)
            .filter(
                InputHashUsage.pipeline_id == pipeline_id,
                InputHashUsage.fulfilled.is_(True),
            )
            .all()
        }
        live_ids = {
            int(m.id)
            for m in session.query(Materialization.id)
            .filter(
                Materialization.pipeline_id == pipeline_id,
                Materialization.output_address.in_(fulfilled_addrs),
                Materialization.is_live.is_(True),
            )
            .all()
        }
        demote |= live_ids - keep
    return demote


def _object_sizes_and_refs(
    session: Session,
) -> Tuple[Dict[str, int], Dict[str, List[Tuple[int, bool]]]]:
    """(size_of, refs_by_hash): present-on-disk byte size per content hash, and
    the (mat_id, is_live) references to each hash across *all* pipelines."""
    rows = session.query(
        Materialization.id,
        Materialization.output_content_hash,
        Materialization.is_live,
    ).all()
    size_of: Dict[str, int] = {}
    refs_by_hash: Dict[str, List[Tuple[int, bool]]] = {}
    for mat_id, content_hash, is_live in rows:
        refs_by_hash.setdefault(content_hash, []).append((int(mat_id), bool(is_live)))
        if content_hash not in size_of:
            try:
                size_of[content_hash] = os.path.getsize(_get_object_path(content_hash))
            except OSError:
                size_of[content_hash] = -1  # absent; can't be reclaimed
    return size_of, refs_by_hash


def _reclaimable_objects(
    refs_by_hash: Dict[str, List[Tuple[int, bool]]],
    size_of: Dict[str, int],
    demote: Set[int],
) -> List[Tuple[str, int]]:
    """Objects present on disk whose every reference is non-live once `demote`
    is applied — the shared-object rule (one live reference anywhere keeps the
    bytes). Returns (content_hash, bytes)."""
    out: List[Tuple[str, int]] = []
    for content_hash, refs in refs_by_hash.items():
        size = size_of.get(content_hash, -1)
        if size < 0:
            continue  # absent from disk: nothing to reclaim
        still_live = any(
            is_live and mat_id not in demote for (mat_id, is_live) in refs
        )
        if not still_live:
            out.append((content_hash, size))
    return out


def _budget_demote_ids(
    session: Session,
    already_demoted: Set[int],
    anchors: Set[int],
    size_of: Dict[str, int],
    refs_by_hash: Dict[str, List[Tuple[int, bool]]],
    max_bytes: int,
) -> Set[int]:
    """Extend the demotion set oldest-run-first until the projected reclaimable
    bytes bring the store under budget. Candidates exclude anchors and anything
    a pipeline's latest terminal run references."""
    total_bytes = sum(s for s in size_of.values() if s >= 0)
    reclaimed_now = sum(b for _, b in _reclaimable_objects(refs_by_hash, size_of, already_demoted))
    if total_bytes - reclaimed_now <= max_bytes:
        return set()

    # Protected: referenced by any pipeline's latest terminal run.
    protected: Set[int] = set()
    for pipeline_id in {
        str(r.pipeline_id)
        for r in session.query(Run.pipeline_id)
        .filter(Run.pipeline_id.isnot(None), Run.status.isnot(None))
        .distinct()
        .all()
    }:
        latest = _terminal_runs(session, pipeline_id, limit=1)
        protected |= _mat_ids_for_runs(session, [str(r.id) for r in latest])

    # Most recent referencing run per live materialization (oldest first).
    ref_run_at: Dict[int, str] = {}
    for mat_id, started in (
        session.query(RunCoordinateStatus.materialization_id, Run.started_at)
        .join(Run, Run.id == RunCoordinateStatus.run_id)
        .filter(RunCoordinateStatus.materialization_id.isnot(None))
        .all()
    ):
        mid = int(mat_id)
        if started is not None and (mid not in ref_run_at or str(started) > ref_run_at[mid]):
            ref_run_at[mid] = str(started)

    live_ids = {
        int(m.id)
        for m in session.query(Materialization.id)
        .filter(
            Materialization.is_live.is_(True),
            Materialization.output_address.in_(
                {u.address for u in session.query(InputHashUsage)
                 .filter(InputHashUsage.fulfilled.is_(True))
                 .all()}
            ),
        )
        .all()
    }
    candidates = sorted(
        (
            mid
            for mid in live_ids
            if mid not in already_demoted
            and mid not in protected
            and mid not in anchors
        ),
        key=lambda mid: (ref_run_at.get(mid, ""), mid),
    )

    demote = set(already_demoted)
    for mid in candidates:
        demote.add(mid)
        reclaimed = sum(b for _, b in _reclaimable_objects(refs_by_hash, size_of, demote))
        if total_bytes - reclaimed <= max_bytes:
            break
    return demote - already_demoted


# ---------------------------------------------------------------------------
# Apply (writes) — shared by gc() and auto_prune()
# ---------------------------------------------------------------------------


def _plan(
    session: Session, policies: Dict[str, int], max_bytes: Optional[int]
) -> Tuple[Set[int], List[Tuple[str, int]], int]:
    """Compute (demote_ids, reclaimed_objects, total_bytes_before) — pure."""
    anchors = _anchor_mat_ids(session)
    demote = _retention_demote_ids(session, policies, anchors)
    size_of, refs_by_hash = _object_sizes_and_refs(session)
    if max_bytes is not None:
        demote |= _budget_demote_ids(
            session, demote, anchors, size_of, refs_by_hash, max_bytes
        )
    reclaimed = _reclaimable_objects(refs_by_hash, size_of, demote)
    total_bytes_before = sum(s for s in size_of.values() if s >= 0)
    return demote, reclaimed, total_bytes_before


def _apply(
    session: Session,
    demote: Set[int],
    reclaimed: List[Tuple[str, int]],
    *,
    trigger: str,
    run_id: str,
) -> None:
    """Flip demotions (with paired pruned lifecycle rows), log reclamations,
    commit, then delete the physical files. The ledger is committed first so it
    stays the truth about what the store contains — a lingering file after a
    failed unlink is harmless (du reads the reclamation row)."""
    for mat_id in sorted(demote):
        mat = session.get(Materialization, mat_id)
        if mat is None or not mat.is_live:
            continue
        mat.is_live = False  # type: ignore
        session.add(
            MaterializationLifecycle(
                materialization_id=mat.id,
                action="pruned",
                run_id=run_id,
                reason=f"retention GC ({trigger})",
                created_at=utcnow_iso(),
            )
        )
        # Parallel write: flip fulfilled=False on input_hash_usages
        # (the new liveness gate).  Same mechanism as invalidation.
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
            usage.fulfilled = False  # type: ignore[assignment]
            usage.last_run_id = run_id  # type: ignore[assignment]
            usage.claimed_at = utcnow_iso()  # type: ignore[assignment]
    for content_hash, size in reclaimed:
        session.add(
            ObjectReclamation(
                content_hash=content_hash,
                bytes=size,
                trigger=trigger,
                run_id=run_id,
                created_at=utcnow_iso(),
            )
        )
    session.commit()  # pairing guard validates the pruned rows (notes/invariants.md)
    for content_hash, _ in reclaimed:
        try:
            os.remove(_get_object_path(content_hash))
        except OSError:
            pass  # already gone, or unwritable: ledger already records it


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def gc(
    delete: bool = False,
    max_bytes: Optional[int] = None,
    home: Optional[str] = None,
) -> GcReport:
    """Apply every recorded retention policy, then (if max_bytes) prune
    oldest-first across pipelines until the store fits — the ops entry point
    for `rubedo gc`.

    Dry-run is the default: with delete=False nothing is written or deleted; the
    returned report lists exactly what a subsequent delete=True would do. With
    delete=True the demotions and object deletions are performed — unless
    another run's heartbeat is live, in which case GC refuses (the restore race,
    trap 3).
    """
    if home is not None:
        from .runner import _init_home

        _init_home(home)

    with get_session() as session:
        if delete:
            running = _running_run_ids(session)
            if running:
                return GcReport(
                    applied=False,
                    refused=f"{len(running)} run(s) still running "
                    f"({', '.join(running)}); retry when idle",
                    max_bytes=max_bytes,
                )

        policies = retention_policies(session)
        demote, reclaimed, total_before = _plan(session, policies, max_bytes)

        if delete and (demote or reclaimed):
            run_id = f"run_{uuid.uuid4().hex[:12]}"
            now = utcnow_iso()
            session.add(
                Run(
                    id=run_id,
                    kind="gc",
                    status="completed",
                    started_at=now,
                    finished_at=now,
                )
            )
            session.commit()
            _apply(session, demote, reclaimed, trigger="gc", run_id=run_id)

        return GcReport(
            applied=delete,
            demoted_mat_ids=sorted(demote),
            reclaimed=reclaimed,
            max_bytes=max_bytes,
            total_bytes_before=total_before,
        )


def auto_prune(
    session: Session, pipeline_id: str, run_id: str, retention: int
) -> Optional[GcReport]:
    """End-of-run hook: prune this one pipeline to its retention window.

    Set-and-forget — always applies (delete). Skips (returns None) instead of
    erroring if *another* run's heartbeat is live (the current run is excluded,
    since it is still finishing). Reuses the finished run's id as the trigger."""
    running = _running_run_ids(session, exclude_run_id=run_id)
    if running:
        return None
    demote, reclaimed, total_before = _plan(session, {pipeline_id: retention}, None)
    if demote or reclaimed:
        _apply(session, demote, reclaimed, trigger="auto_prune", run_id=run_id)
    return GcReport(
        applied=True,
        demoted_mat_ids=sorted(demote),
        reclaimed=reclaimed,
        total_bytes_before=total_before,
    )


def cheap_store_bytes(home: Optional[str] = None) -> int:
    """A cached, cheap estimate of total object-store bytes for the warn check.

    A full scandir sum is walked at most once per TTL and cached in a sidecar,
    so the warn-threshold check never pays an O(store) stat storm on every run.
    """
    import json

    from .store import OBJECTS_DIR
    from .util import iso_age_seconds

    root = home if home is not None else os.path.dirname(OBJECTS_DIR)
    objects_dir = os.path.join(root, "objects") if home is not None else OBJECTS_DIR
    cache_path = os.path.join(root, ".store_size_cache.json")
    try:
        with open(cache_path) as f:
            cached = json.load(f)
        if iso_age_seconds(cached["computed_at"]) < _STORE_SIZE_CACHE_TTL_SECONDS:
            return int(cached["bytes"])
    except (OSError, ValueError, KeyError):
        pass

    total = 0
    for dirpath, _dirs, files in os.walk(objects_dir):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                pass
    try:
        with open(cache_path, "w") as f:
            json.dump({"bytes": total, "computed_at": utcnow_iso()}, f)
    except OSError:
        pass
    return total
