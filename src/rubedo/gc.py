"""Retention GC: the byte-deleting garbage collector.

In the new model (notes/arrow-storage.md), "live" = fulfilled=True in
input_hash_usages.  Demote = flip fulfilled=False (same mechanism as
invalidation).  The sweep refcounts object store entries via
content_hash — the content hashes come from the Arrow lane_store rows,
not from a SQLite column.

Two policies, no others: per-pipeline keep-last-N-runs
(`pipeline(retention=N)`) and a global byte budget (`gc(max_bytes=...)`).
Both run through the same two phases:

  demote  Flip fulfilled=False on input_hash_usages entries outside the
          keep-set.
  sweep   Delete an object file only when *every* reference is non-live
          once demote is applied.  Log in object_reclamations.

Expand anchors: always kept (pruning one would silently re-run the
expand fn).  Anchors are fulfilled addresses that have no
RunCoordinateStatus reference.
"""

import os
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy.orm import Session

from .home import Home
from .models import (
    InputHashUsage,
    ObjectReclamation,
    Run,
    RunCoordinateStatus,
    effective_run_status,
)
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

    demoted_addresses and reclaimed are computed identically for dry-run and
    delete, so a dry-run lists exactly what a subsequent --delete performs.
    """

    applied: bool  # True if writes were performed (delete=True and not refused)
    demoted_addresses: List[str] = field(default_factory=list)
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
        return len(self.demoted_addresses)

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


def _anchor_addresses(session: Session) -> Set[str]:
    """Live (fulfilled=True) addresses referenced by *no*
    RunCoordinateStatus — exactly the expand cache anchors."""
    referenced = {
        str(r.output_address)
        for r in session.query(RunCoordinateStatus.output_address)
        .filter(RunCoordinateStatus.output_address.isnot(None))
        .distinct()
        .all()
    }
    fulfilled_addrs: Set[str] = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }
    return fulfilled_addrs - referenced


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


def _addresses_for_runs(session: Session, run_ids: List[str]) -> Set[str]:
    """Output addresses any of these runs referenced (created or reused).
    Uses RunCoordinateStatus.output_address (already a column)."""
    if not run_ids:
        return set()
    rows = (
        session.query(RunCoordinateStatus.output_address)
        .filter(
            RunCoordinateStatus.run_id.in_(run_ids),
            RunCoordinateStatus.output_address.isnot(None),
        )
        .distinct()
        .all()
    )
    return {str(r.output_address) for r in rows}


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


def _fulfilled_addrs_by_pipeline(
    session: Session,
    home: Home,
) -> Dict[str, Set[str]]:
    """Fulfilled addresses grouped by pipeline_id.  IHU doesn't store
    pipeline_id, so we cross-reference with the Arrow lane_store rows
    (which carry pipeline_id from their file path) and/or
    RunCoordinateStatus."""
    fulfilled = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }
    if not fulfilled:
        return {}
    # Group by pipeline via the Arrow rows
    by_pipe: Dict[str, Set[str]] = {}
    for row in home.lanes.all_filled_rows():
        addr = row.get("address")
        if addr and addr in fulfilled:
            by_pipe.setdefault(row["pipeline_id"], set()).add(addr)
    return by_pipe


def _retention_demote_addresses(
    session: Session, home: Home, policies: Dict[str, int], anchors: Set[str]
) -> Set[str]:
    """Live (fulfilled) addresses to demote so each pipeline keeps only its
    last N runs' outputs."""
    demote: Set[str] = set()
    fulfilled_by_pipe = _fulfilled_addrs_by_pipeline(session, home)
    for pipeline_id, n in policies.items():
        runs = _terminal_runs(session, pipeline_id, limit=n)
        keep = _addresses_for_runs(session, [str(r.id) for r in runs]) | anchors
        live = fulfilled_by_pipe.get(pipeline_id, set())
        demote |= live - keep
    return demote


def _object_sizes_and_refs(
    session: Session,
    home: Home,
) -> Tuple[Dict[str, int], Dict[str, List[Tuple[str, bool]]]]:
    """(size_of, refs_by_hash): present-on-disk byte size per content hash,
    and the (address, fulfilled) references to each hash across *all*
    pipelines.  Content hashes come from the Arrow lane_store rows;
    liveness comes from input_hash_usages.fulfilled."""
    fulfilled = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }
    size_of: Dict[str, int] = {}
    refs_by_hash: Dict[str, List[Tuple[str, bool]]] = {}
    for row in home.lanes.all_filled_rows():
        addr = row.get("address", "")
        output = row.get("output")
        # Parse ref strings from the output column — native inline values
        # (dicts, ints) have no object bytes to GC; only "objects:<hash>"
        # ref strings do.
        if not isinstance(output, str) or not output.startswith("objects:"):
            continue
        content_hash = output[len("objects:"):]
        is_live = addr in fulfilled
        refs_by_hash.setdefault(content_hash, []).append((addr, is_live))
        if content_hash not in size_of:
            try:
                size_of[content_hash] = os.path.getsize(home.store.object_path(content_hash))
            except OSError:
                size_of[content_hash] = -1  # absent; can't be reclaimed
    return size_of, refs_by_hash


def _reclaimable_objects(
    refs_by_hash: Dict[str, List[Tuple[str, bool]]],
    size_of: Dict[str, int],
    demote: Set[str],
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
            is_live and addr not in demote for (addr, is_live) in refs
        )
        if not still_live:
            out.append((content_hash, size))
    return out


def _budget_demote_addresses(
    session: Session,
    already_demoted: Set[str],
    anchors: Set[str],
    size_of: Dict[str, int],
    refs_by_hash: Dict[str, List[Tuple[str, bool]]],
    max_bytes: int,
) -> Set[str]:
    """Extend the demotion set oldest-run-first until the projected
    reclaimable bytes bring the store under budget. Candidates exclude
    anchors and anything a pipeline's latest terminal run references."""
    total_bytes = sum(s for s in size_of.values() if s >= 0)
    reclaimed_now = sum(b for _, b in _reclaimable_objects(refs_by_hash, size_of, already_demoted))
    if total_bytes - reclaimed_now <= max_bytes:
        return set()

    # Protected: referenced by any pipeline's latest terminal run.
    protected: Set[str] = set()
    for pipeline_id in {
        str(r.pipeline_id)
        for r in session.query(Run.pipeline_id)
        .filter(Run.pipeline_id.isnot(None), Run.status.isnot(None))
        .distinct()
        .all()
    }:
        latest = _terminal_runs(session, pipeline_id, limit=1)
        protected |= _addresses_for_runs(session, [str(r.id) for r in latest])

    # Most recent referencing run per live address (oldest first).
    ref_run_at: Dict[str, str] = {}
    for addr, started in (
        session.query(RunCoordinateStatus.output_address, Run.started_at)
        .join(Run, Run.id == RunCoordinateStatus.run_id)
        .filter(RunCoordinateStatus.output_address.isnot(None))
        .all()
    ):
        a = str(addr)
        if started is not None and (a not in ref_run_at or str(started) > ref_run_at[a]):
            ref_run_at[a] = str(started)

    fulfilled = {
        str(u.address) for u in session.query(InputHashUsage)
        .filter(InputHashUsage.fulfilled.is_(True))
        .all()
    }
    candidates = sorted(
        (
            addr
            for addr in fulfilled
            if addr not in already_demoted
            and addr not in protected
            and addr not in anchors
        ),
        key=lambda addr: (ref_run_at.get(addr, ""), addr),
    )

    demote = set(already_demoted)
    for addr in candidates:
        demote.add(addr)
        reclaimed = sum(b for _, b in _reclaimable_objects(refs_by_hash, size_of, demote))
        if total_bytes - reclaimed <= max_bytes:
            break
    return demote - already_demoted


# ---------------------------------------------------------------------------
# Apply (writes) — shared by gc() and auto_prune()
# ---------------------------------------------------------------------------


def _plan(
    session: Session, home: Home, policies: Dict[str, int], max_bytes: Optional[int]
) -> Tuple[Set[str], List[Tuple[str, int]], int]:
    """Compute (demote_addresses, reclaimed_objects, total_bytes_before) — pure."""
    anchors = _anchor_addresses(session)
    demote = _retention_demote_addresses(session, home, policies, anchors)
    size_of, refs_by_hash = _object_sizes_and_refs(session, home)
    if max_bytes is not None:
        demote |= _budget_demote_addresses(
            session, demote, anchors, size_of, refs_by_hash, max_bytes
        )
    reclaimed = _reclaimable_objects(refs_by_hash, size_of, demote)
    total_bytes_before = sum(s for s in size_of.values() if s >= 0)
    return demote, reclaimed, total_bytes_before


def _apply(
    session: Session,
    home: Home,
    demote: Set[str],
    reclaimed: List[Tuple[str, int]],
    *,
    trigger: str,
    run_id: str,
) -> None:
    """Flip demotions, log reclamations, commit, then delete the physical
    files.  The ledger is committed first so it stays the truth about what
    the store contains — a lingering file after a failed unlink is harmless
    (du reads the reclamation row).

    Liveness is input_hash_usages.fulfilled.
    """
    for addr in sorted(demote):
        # Flip fulfilled=False on input_hash_usages (the liveness gate)
        usage = (
            session.query(InputHashUsage)
            .filter_by(address=addr)
            .first()
        )
        if usage:
            usage.fulfilled = False  # type: ignore[assignment]
            usage.last_run_id = run_id  # type: ignore[assignment]
            home.lanes.mark_unfulfilled(addr)
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
    session.commit()
    for content_hash, _ in reclaimed:
        try:
            os.remove(home.store.object_path(content_hash))
        except OSError:
            pass  # already gone, or unwritable: ledger already records it


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def gc(
    delete: bool = False,
    max_bytes: Optional[int] = None,
    home: Optional[Home] = None,
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
    home = home or Home.default()

    with home.session() as session:
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
        demote, reclaimed, total_before = _plan(session, home, policies, max_bytes)

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
            _apply(session, home, demote, reclaimed, trigger="gc", run_id=run_id)

        return GcReport(
            applied=delete,
            demoted_addresses=sorted(demote),
            reclaimed=reclaimed,
            max_bytes=max_bytes,
            total_bytes_before=total_before,
        )


def auto_prune(
    session: Session,
    pipeline_id: str,
    run_id: str,
    retention: int,
    home: Optional[Home] = None,
) -> Optional[GcReport]:
    """End-of-run hook: prune this one pipeline to its retention window.

    Set-and-forget — always applies (delete). Skips (returns None) instead of
    erroring if *another* run's heartbeat is live (the current run is excluded,
    since it is still finishing). Reuses the finished run's id as the trigger."""
    home = home or Home.default()
    running = _running_run_ids(session, exclude_run_id=run_id)
    if running:
        return None
    demote, reclaimed, total_before = _plan(session, home, {pipeline_id: retention}, None)
    if demote or reclaimed:
        _apply(session, home, demote, reclaimed, trigger="auto_prune", run_id=run_id)
    return GcReport(
        applied=True,
        demoted_addresses=sorted(demote),
        reclaimed=reclaimed,
        total_bytes_before=total_before,
    )


def cheap_store_bytes(home: Optional[Home] = None) -> int:
    """A cached, cheap estimate of total object-store bytes for the warn check.

    A full scandir sum is walked at most once per TTL and cached in a sidecar,
    so the warn-threshold check never pays an O(store) stat storm on every run.
    """
    import json

    from .util import iso_age_seconds

    home = home or Home.default()
    root = home.store.root
    objects_dir = home.store.objects_dir
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
