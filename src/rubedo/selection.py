"""
Selection language and querying logic for invalidation and UI previews.
"""
from typing import Any, Dict, Optional, List
from pydantic import BaseModel
from sqlalchemy.orm import Session
import fnmatch

from .models import (
    Materialization,
    MaterializationIndexEntry,
    RunCoordinateStatus,
    InputHashUsage,
)


class Selection(BaseModel):
    """Criteria for selecting materializations, either programmatically or via a query string."""
    source_id: Optional[str] = None
    coordinate_glob: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    version_range: Optional[str] = None
    output_address: Optional[str] = None
    invalidated: Optional[bool] = None
    pipeline_id: Optional[str] = None
    # Indexed value fields (@step(index=[...])): all pairs must match
    index: Optional[Dict[str, str]] = None

    @classmethod
    def parse(cls, query: str) -> "Selection":
        """Parse the selection language: whitespace-separated key:value terms.

        Reserved prefixes map to engine facts —
          source:<id>  coord:<glob>  step:<name>  version:<v>
          address:<output_address>  live:true|false
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
            elif key == "pipeline":
                fields["pipeline_id"] = value
            elif key == "version":
                if value.startswith(("<", ">", "=", "!")):
                    fields["version_range"] = value
                else:
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
    """Retrieve materialization IDs that match the given selection criteria."""
    query = session.query(Materialization).distinct()

    if selection.step:
        query = query.filter(Materialization.step_name == selection.step)
    if selection.pipeline_id:
        query = query.filter(Materialization.pipeline_id == selection.pipeline_id)
    if selection.code_version:
        query = query.filter(Materialization.code_version == selection.code_version)
    if selection.output_address:
        query = query.filter(Materialization.output_address == selection.output_address)
    if selection.invalidated is not None:
        # Under the new model, liveness = input_hash_usages.fulfilled.
        # Join to InputHashUsage on output_address and filter by fulfilled.
        query = query.join(
            InputHashUsage,
            (InputHashUsage.address == Materialization.output_address)
            & (InputHashUsage.step_name == Materialization.step_name)
            & (InputHashUsage.pipeline_id == Materialization.pipeline_id),
        ).filter(InputHashUsage.fulfilled.is_(not selection.invalidated))

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

    # Batch fetch the latest coordinate for all matching materializations
    latest_coords = {}
    if selection.coordinate_glob and mats:
        mat_ids = [m.id for m in mats]
        # Order by asc, so the last one inserted into the dict is the latest
        rows = (
            session.query(RunCoordinateStatus.materialization_id, RunCoordinateStatus.coordinate)
            .filter(RunCoordinateStatus.materialization_id.in_(mat_ids))
            .order_by(RunCoordinateStatus.id.asc())
            .all()
        )
        for mat_id, coord in rows:
            latest_coords[mat_id] = coord

    # Coordinate-glob and version-range filtering happen in Python (glob
    # matching and PEP 440 specifiers don't map cleanly to SQL).
    result_ids = []
    for m in mats:
        # Coordinate glob check
        if selection.coordinate_glob:
            coord = latest_coords.get(m.id)
            if not coord or not fnmatch.fnmatch(str(coord), str(selection.coordinate_glob)):
                continue
                
        # Version range check
        if selection.version_range:
            from packaging.version import Version, InvalidVersion
            from packaging.specifiers import SpecifierSet
            try:
                specifier_set = SpecifierSet(selection.version_range)
                parsed_version = Version(str(m.code_version))
                if parsed_version not in specifier_set:
                    continue
            except InvalidVersion:
                continue
            except ValueError:
                # If the specifier itself is invalid, we might want to fail earlier, but for now we skip.
                continue

        result_ids.append(m.id)

    return result_ids  # type: ignore

