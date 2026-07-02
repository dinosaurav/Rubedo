import json
from typing import Any, Optional, List
from pydantic import BaseModel
from sqlalchemy.orm import Session
import fnmatch

from .models import Materialization, RunCoordinateStatus


class MetadataFilter(BaseModel):
    key: str
    op: str  # "equals", "not equals", "greater than", "less than", "exists", "does not exist"
    value: Any = None


class Selection(BaseModel):
    source_id: Optional[str] = None
    coordinate_glob: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    output_address: Optional[str] = None
    output_content_hash: Optional[str] = None
    metadata: Optional[List[MetadataFilter]] = None
    invalidated: Optional[bool] = None
    coordinates: Optional[List[str]] = None


def get_selection_materialization_ids(
    session: Session, selection: Selection
) -> List[int]:
    query = session.query(Materialization)

    if selection.step:
        query = query.filter(Materialization.step_name == selection.step)
    if selection.code_version:
        query = query.filter(Materialization.code_version == selection.code_version)
    if selection.output_address:
        query = query.filter(Materialization.output_address == selection.output_address)
    if selection.output_content_hash:
        query = query.filter(
            Materialization.output_content_hash == selection.output_content_hash
        )
    if selection.invalidated is not None:
        query = query.filter(Materialization.is_live.is_(not selection.invalidated))

    if selection.source_id or selection.coordinate_glob or selection.coordinates:
        # Join with RunCoordinateStatus to filter by coordinate or source_id
        query = query.join(
            RunCoordinateStatus,
            RunCoordinateStatus.materialization_id == Materialization.id,
        )
        if selection.source_id:
            query = query.filter(
                RunCoordinateStatus.source_id == selection.source_id
            )
        if selection.coordinates:
            query = query.filter(
                RunCoordinateStatus.coordinate.in_(selection.coordinates)
            )

    mats = query.all()

    # Python-side filtering for coordinate glob and metadata since SQLite json querying is complex/not always compiled in
    result_ids = []
    for m in mats:
        # Coordinate glob check
        if selection.coordinate_glob:
            # We need the coordinate for this materialization. We joined above if glob was present.
            # Easiest way is to fetch current output for it
            co = (
                session.query(RunCoordinateStatus)
                .filter_by(materialization_id=m.id)
                .order_by(RunCoordinateStatus.id.desc())
                .first()
            )
            if not co or not fnmatch.fnmatch(co.coordinate, selection.coordinate_glob):
                continue

        # Metadata check
        if selection.metadata:
            if not m.metadata_json:
                continue
            try:
                meta_dict = json.loads(m.metadata_json)
            except Exception:
                continue

            passed = True
            for f in selection.metadata:
                if f.op == "exists":
                    if f.key not in meta_dict:
                        passed = False
                        break
                elif f.op == "does not exist":
                    if f.key in meta_dict:
                        passed = False
                        break
                else:
                    if f.key not in meta_dict:
                        passed = False
                        break
                    val = meta_dict[f.key]
                    if f.op == "equals" and val != f.value:
                        passed = False
                        break
                    if f.op == "not equals" and val == f.value:
                        passed = False
                        break
                    if f.op == "greater than":
                        try:
                            if not (val > f.value):
                                passed = False
                                break
                        except TypeError:
                            passed = False
                            break
                    if f.op == "less than":
                        try:
                            if not (val < f.value):
                                passed = False
                                break
                        except TypeError:
                            passed = False
                            break

            if not passed:
                continue

        result_ids.append(m.id)

    return result_ids
