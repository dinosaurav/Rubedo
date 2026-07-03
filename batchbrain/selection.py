from typing import Any, Dict, Optional, List
from pydantic import BaseModel
from sqlalchemy.orm import Session
import fnmatch

from .models import Materialization, MaterializationIndexEntry, RunCoordinateStatus


class Selection(BaseModel):
    source_id: Optional[str] = None
    coordinate_glob: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    output_address: Optional[str] = None
    invalidated: Optional[bool] = None
    # Indexed value fields (@step(index=[...])): all pairs must match
    index: Optional[Dict[str, str]] = None

    @classmethod
    def parse(cls, query: str) -> "Selection":
        """Parse the selection language: whitespace-separated key:value terms.

        Reserved prefixes map to engine facts —
          source:<id>  coord:<glob>  step:<name>  version:<v>
          address:<output_address>  content:<hash>  live:true|false
          meta.<key>:<value> (equality; meta.<key>:* for existence)
        Any other field:value term matches an indexed output field
        (@step(index=[...])) — indexed data is the language's open
        vocabulary. Quote values containing spaces: company:"acme corp".
        """
        import shlex

        fields: Dict[str, Any] = {}
        index: Dict[str, str] = {}

        for term in shlex.split(query):
            key, sep, value = term.partition(":")
            if not sep or not key or not value:
                raise ValueError(
                    f"Invalid selection term {term!r}: expected key:value "
                    "(e.g. coord:*.txt, company:acme)"
                )

            if key == "source":
                fields["source_id"] = value
            elif key in ("coord", "coordinate"):
                fields["coordinate_glob"] = value
            elif key == "step":
                fields["step"] = value
            elif key == "version":
                fields["code_version"] = value
            elif key == "address":
                fields["output_address"] = value
            elif key == "live":
                if value not in ("true", "false"):
                    raise ValueError(f"live: expects true or false, got {value!r}")
                fields["invalidated"] = value == "false"
            else:
                index[key] = value

        if index:
            fields["index"] = index
        return cls(**fields)


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
    if selection.invalidated is not None:
        query = query.filter(Materialization.is_live.is_(not selection.invalidated))

    if selection.index:
        for field, value in selection.index.items():
            matching = (
                session.query(MaterializationIndexEntry.materialization_id)
                .filter_by(field=field, value=str(value))
            )
            query = query.filter(Materialization.id.in_(matching))

    if selection.source_id or selection.coordinate_glob:
        # Join with RunCoordinateStatus to filter by coordinate or source_id
        query = query.join(
            RunCoordinateStatus,
            RunCoordinateStatus.materialization_id == Materialization.id,
        )
        if selection.source_id:
            query = query.filter(
                RunCoordinateStatus.source_id == selection.source_id
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

        result_ids.append(m.id)

    return result_ids
