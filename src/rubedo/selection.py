"""
Selection language and querying logic for invalidation and UI previews.
"""
from typing import Any, Dict, Optional, List
from pydantic import BaseModel
from sqlalchemy.orm import Session
import fnmatch

from .models import (
    RunCoordinateStatus,
    InputHashUsage,
)
from . import lane_store


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


def _arrow_rows_matching(
    pipeline_id: Optional[str],
    step_name: Optional[str],
    code_version: Optional[str],
    output_address: Optional[str],
) -> List[Dict[str, Any]]:
    """Arrow lane_store rows matching the given filters.  Each row dict
    carries ``address``, ``code_version``, ``pipeline_id``,
    ``step_name``, ``content_hash``, ``filtered``, etc."""
    rows = lane_store.all_filled_rows()
    out = []
    for row in rows:
        if pipeline_id and row.get("pipeline_id") != pipeline_id:
            continue
        if step_name and row.get("step_name") != step_name:
            continue
        if code_version and row.get("code_version") != code_version:
            continue
        if output_address and row.get("address") != output_address:
            continue
        out.append(row)
    return out


def get_selection_addresses(
    session: Session, selection: Selection
) -> List[str]:
    """Output addresses matching the given selection criteria.

    Builds the candidate set from Arrow lane_store rows (filtered by
    pipeline/step/version/address), then applies liveness (IHU
    fulfilled), indexed-field (Arrow index_values), and
    coordinate/source_id (RunCoordinateStatus.output_address) filters.
    Version-range and coordinate-glob filtering happen in Python (glob
    matching and PEP 440 specifiers don't map cleanly to SQL).
    """
    # 1. Candidate addresses from Arrow rows
    arrow_rows = _arrow_rows_matching(
        selection.pipeline_id,
        selection.step,
        selection.code_version,
        selection.output_address,
    )
    if not arrow_rows:
        return []

    # 2. Liveness filter (input_hash_usages.fulfilled)
    if selection.invalidated is not None:
        fulfilled_addrs = {
            str(u.address) for u in session.query(InputHashUsage)
            .filter(InputHashUsage.fulfilled.is_(not selection.invalidated))
            .all()
        }
        arrow_rows = [
            r for r in arrow_rows if r.get("address") in fulfilled_addrs
        ]

    # 3. Indexed-field filter — scan the struct output column for steps
    #    that declared index= on the field, fall back to index_values
    #    for string output or spilled values.  Without an index=
    #    declaration, a field is not searchable via selection (it's
    #    still available for join/group_key matching directly from the
    #    parent's output dict).
    if selection.index:
        for field, value in selection.index.items():
            # Try struct sub-field scan: only for rows whose step declared
            # index= on this field (index_values has the field key, even if
            # the struct scan is what actually reads the value)
            struct_matches: set = set()
            for r in arrow_rows:
                # Check if this step declared index= on this field
                iv = r.get("index_values")
                if not iv or field not in iv:
                    continue
                output = r.get("output")
                if isinstance(output, dict):
                    val = output.get(field)
                    if val is None:
                        continue
                    check_vals = [str(v) for v in val] if isinstance(val, (list, tuple)) else [str(val)]
                    if value in check_vals:
                        struct_matches.add(r.get("address"))
            if struct_matches:
                arrow_rows = [r for r in arrow_rows if r.get("address") in struct_matches]
            else:
                # Fall back to index_values map column
                if selection.pipeline_id and selection.step:
                    addrs = set(lane_store.scan_indexed_field(
                        selection.pipeline_id, selection.step, field, value
                    ))
                else:
                    addrs = set(lane_store.scan_indexed_field_all(field, value))
                arrow_rows = [r for r in arrow_rows if r.get("address") in addrs]

    # 4. Coordinate / source_id filter (RunCoordinateStatus.output_address)
    if selection.source_id or selection.coordinate_glob:
        rcs_addrs = {
            str(r.output_address)
            for r in session.query(RunCoordinateStatus.output_address)
            .filter(RunCoordinateStatus.output_address.isnot(None))
            .all()
        }
        if selection.source_id:
            rcs_addrs = {
                str(r.output_address)
                for r in session.query(RunCoordinateStatus.output_address)
                .filter(
                    RunCoordinateStatus.output_address.isnot(None),
                    RunCoordinateStatus.source_id == selection.source_id,
                )
                .all()
            }
        arrow_rows = [r for r in arrow_rows if r.get("address") in rcs_addrs]

    # 5. Coordinate-glob + version-range filtering (in Python)
    if selection.coordinate_glob:
        # Build address → latest coordinate from RCS
        addr_coords: Dict[str, str] = {}
        for addr, coord in (
            session.query(
                RunCoordinateStatus.output_address,
                RunCoordinateStatus.coordinate,
            )
            .filter(
                RunCoordinateStatus.output_address.isnot(None),
                RunCoordinateStatus.output_address.in_(
                    [r.get("address", "") for r in arrow_rows]
                ),
            )
            .order_by(RunCoordinateStatus.id.asc())
            .all()
        ):
            addr_coords[str(addr)] = str(coord)
        arrow_rows = [
            r for r in arrow_rows
            if fnmatch.fnmatch(
                str(addr_coords.get(r.get("address", ""), "")),
                str(selection.coordinate_glob),
            )
        ]

    if selection.version_range:
        from packaging.specifiers import SpecifierSet

        try:
            specifier_set = SpecifierSet(selection.version_range)
        except ValueError:
            pass
        else:
            arrow_rows = [
                r for r in arrow_rows
                if _matches_version(r.get("code_version"), specifier_set)
            ]

    return [r.get("address", "") for r in arrow_rows if r.get("address")]


def _matches_version(version_str: Optional[str], specifier_set: Any) -> bool:
    """Check if a version string matches a PEP 440 specifier set."""
    if not version_str:
        return False
    from packaging.version import Version, InvalidVersion

    try:
        return Version(str(version_str)) in specifier_set
    except InvalidVersion:
        return False
