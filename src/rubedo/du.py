"""Read-only storage observability: what the object store holds, per the ledger.

storage_report() walks the Arrow lane_store rows + input_hash_usages —
never the store directory (direction-of-truth rule: the ledger is the
truth about what the store contains; enumerating `objects/` would invert
that). File sizes are read via os.path.getsize on ledger-named paths,
which is fine: the ledger names the path, the filesystem only supplies
the byte count.

The load-bearing subtlety is sharing. The store is content-addressed and
dedupes identical bytes, so one physical object (keyed by
output_content_hash) can back many materializations — different output
addresses, different steps, even different pipelines. Ref-counting therefore
groups by output_content_hash across *all* materializations: an object is
"reclaimable" only if every materialization referencing it is non-live. One
live reference anywhere keeps the object. Nothing here deletes anything —
the reclaimable numbers are a dry-run report (the audit 10b would build on).

Because groups share objects, per-step and per-pipeline byte totals are
each deduped within their own scope and can sum to more than total_bytes —
that is the honest reading of shared storage, not a bug.

A ledger-named path missing from disk is counted and reported as missing,
never a crash; missing objects contribute zero bytes and are excluded from
the reclaimable estimate (there are no bytes to reclaim).

Retention GC (10b) deletes objects *deliberately* and logs each in the
append-only object_reclamations table. So an absent object is disambiguated:
absent + logged in object_reclamations = **reclaimed** (pruned on purpose);
absent + not logged = **missing** (corruption). A reclaimed hash that a later
lazy-heal re-wrote is present again and accounts as a normal live object — the
old reclamation row is simply ignored once the bytes are back.
"""
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Set

from .db import get_session
from .models import InputHashUsage, ObjectReclamation
from .store import _get_object_path
from . import lane_store


@dataclass
class StepUsage:
    """Storage referenced by one step's materializations (objects deduped
    within the step)."""

    step_name: str
    materializations: int = 0
    live_materializations: int = 0
    objects: int = 0
    bytes: int = 0


@dataclass
class PipelineUsage:
    """Storage referenced by one pipeline (objects deduped within the
    pipeline; steps may share objects, so step bytes can sum to more)."""

    pipeline_id: str
    materializations: int = 0
    live_materializations: int = 0
    objects: int = 0
    bytes: int = 0
    steps: List[StepUsage] = field(default_factory=list)


@dataclass
class StorageReport:
    """The full du report. Totals count distinct physical objects; the
    reclaimable numbers are a dry-run estimate — nothing is deleted."""

    total_objects: int
    total_bytes: int
    total_materializations: int
    live_materializations: int
    missing_objects: int
    reclaimable_objects: int
    reclaimable_bytes: int
    pipelines: List[PipelineUsage]
    # Objects deliberately deleted by retention GC (logged in
    # object_reclamations), distinct from missing (corruption). Excluded from
    # total_objects/total_bytes — they are no longer part of the store.
    reclaimed_objects: int = 0
    reclaimed_bytes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def __str__(self) -> str:
        lines = [
            f"Object store: {self.total_objects} objects, "
            f"{_human_bytes(self.total_bytes)} "
            f"({self.total_materializations} materializations, "
            f"{self.live_materializations} live)"
        ]
        if self.missing_objects:
            lines.append(f"  {self.missing_objects} object(s) missing from disk")
        if self.reclaimed_objects:
            lines.append(
                f"  {self.reclaimed_objects} object(s) / "
                f"{_human_bytes(self.reclaimed_bytes)} reclaimed by retention GC"
            )
        for p in self.pipelines:
            lines.append(
                f"  {p.pipeline_id}: {_human_bytes(p.bytes)} across "
                f"{p.objects} objects ({p.materializations} materializations, "
                f"{p.live_materializations} live)"
            )
            for s in p.steps:
                lines.append(
                    f"    {s.step_name}: {_human_bytes(s.bytes)} / {s.objects} "
                    f"objects ({s.materializations} materializations, "
                    f"{s.live_materializations} live)"
                )
        lines.append(
            f"Reclaimable (dry-run; nothing is deleted): "
            f"{self.reclaimable_objects} objects / "
            f"{_human_bytes(self.reclaimable_bytes)} have zero live references"
        )
        return "\n".join(lines)


def _human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{int(n)} B"  # pragma: no cover


def storage_report(home: Optional[str] = None) -> StorageReport:
    """Compute the storage report by walking the ledger (read-only).

    home (optional): point the DB and object store at a custom root for this
    call, mirroring trace()/run().
    """
    from .runner import _check_home_guard

    _check_home_guard(home)
    if home is not None:
        from .runner import _init_home

        _init_home(home)

    with get_session() as session:
        # Bytes logged at deletion for each reclaimed hash (latest row wins, so
        # a re-reclaimed object reflects its most recent deletion).
        reclaimed_bytes_of: Dict[str, int] = {}
        for r in (
            session.query(ObjectReclamation)
            .order_by(ObjectReclamation.id.asc())
            .all()
        ):
            reclaimed_bytes_of[str(r.content_hash)] = int(r.bytes)

        # Liveness from input_hash_usages.fulfilled
        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(True))
            .all()
        }

    # Content hashes + pipeline/step from the Arrow lane_store rows.
    arrow_rows = lane_store.all_filled_rows()

    # Build (pipeline_id, step_name, content_hash, is_live) tuples.
    # content_hash is parsed from the `output` ref string for spilled
    # values; inline (JSON) outputs have content_hash=None — they count as
    # materializations but occupy no object-store bytes.
    rows: List[tuple] = []
    for row in arrow_rows:
        addr = row.get("address", "")
        output = row.get("output")
        content_hash = (
            output[len("objects:"):]
            if isinstance(output, str) and output.startswith("objects:")
            else None
        )
        is_live = addr in fulfilled_addrs
        rows.append((row.get("pipeline_id", ""), row.get("step_name", ""), content_hash, is_live))

    # Physical objects: group spilled materializations by content hash.
    size_of: Dict[str, int] = {}
    missing: Set[str] = set()
    reclaimed: Set[str] = set()
    live_refs: Dict[str, int] = {}
    for _, _, content_hash, is_live in rows:
        if content_hash is None:
            continue  # inline value: no object-store bytes
        if (
            content_hash not in size_of
            and content_hash not in missing
            and content_hash not in reclaimed
        ):
            try:
                size_of[content_hash] = os.path.getsize(_get_object_path(content_hash))
            except OSError:
                # Absent: a logged deletion is a deliberate reclaim; otherwise
                # the object is genuinely missing (corruption).
                if content_hash in reclaimed_bytes_of:
                    reclaimed.add(content_hash)
                else:
                    missing.add(content_hash)
        live_refs[content_hash] = live_refs.get(content_hash, 0) + (1 if is_live else 0)

    reclaimable = [
        h
        for h, live in live_refs.items()
        if live == 0 and h not in missing and h not in reclaimed
    ]

    # Per-pipeline / per-step breakdown, objects deduped within each scope.
    pipe_hashes: Dict[str, Set[str]] = {}
    step_hashes: Dict[str, Dict[str, Set[str]]] = {}
    pipelines: Dict[str, PipelineUsage] = {}
    for pipeline_id, step_name, content_hash, is_live in rows:
        pipe = pipelines.setdefault(pipeline_id, PipelineUsage(pipeline_id=pipeline_id))
        steps = step_hashes.setdefault(pipeline_id, {})
        step = next((s for s in pipe.steps if s.step_name == step_name), None)
        if step is None:
            step = StepUsage(step_name=step_name)
            pipe.steps.append(step)
        for usage, seen in (
            (pipe, pipe_hashes.setdefault(pipeline_id, set())),
            (step, steps.setdefault(step_name, set())),
        ):
            usage.materializations += 1
            usage.live_materializations += 1 if is_live else 0
            if content_hash is not None and content_hash not in seen:
                seen.add(content_hash)
                usage.objects += 1
                usage.bytes += size_of.get(content_hash, 0)

    ordered = sorted(pipelines.values(), key=lambda p: p.pipeline_id)
    for p in ordered:
        p.steps.sort(key=lambda s: s.step_name)

    return StorageReport(
        total_objects=len(size_of) + len(missing),
        total_bytes=sum(size_of.values()),
        total_materializations=len(rows),
        live_materializations=sum(1 for r in rows if r[3]),
        missing_objects=len(missing),
        reclaimable_objects=len(reclaimable),
        reclaimable_bytes=sum(size_of[h] for h in reclaimable),
        pipelines=ordered,
        reclaimed_objects=len(reclaimed),
        reclaimed_bytes=sum(reclaimed_bytes_of[h] for h in reclaimed),
    )
