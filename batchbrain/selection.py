import json
from typing import Any, Optional, List
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_
import glob
import fnmatch

from .models import Materialization, RunCoordinate

class MetadataFilter(BaseModel):
    key: str
    op: str # "equals", "not equals", "greater than", "less than", "exists", "does not exist"
    value: Any = None

class Selection(BaseModel):
    source_folder: Optional[str] = None
    coordinate_glob: Optional[str] = None
    step: Optional[str] = None
    code_version: Optional[str] = None
    output_address: Optional[str] = None
    output_content_hash: Optional[str] = None
    metadata: Optional[List[MetadataFilter]] = None
    invalidated: Optional[bool] = None

def get_selection_materialization_ids(session: Session, selection: Selection) -> List[int]:
    query = session.query(Materialization)
    
    if selection.step:
        query = query.filter(Materialization.step == selection.step)
    if selection.code_version:
        query = query.filter(Materialization.code_version == selection.code_version)
    if selection.output_address:
        query = query.filter(Materialization.output_address == selection.output_address)
    if selection.output_content_hash:
        query = query.filter(Materialization.output_content_hash == selection.output_content_hash)
    if selection.invalidated is not None:
        if selection.invalidated:
            query = query.filter(Materialization.invalidated_at.isnot(None))
        else:
            query = query.filter(Materialization.invalidated_at.is_(None))
            
    if selection.source_folder or selection.coordinate_glob:
        # Join with RunCoordinate to filter by coordinate or source_folder
        query = query.join(RunCoordinate, RunCoordinate.materialization_id == Materialization.id)
        if selection.source_folder:
            query = query.filter(RunCoordinate.source_folder == selection.source_folder)
            
    mats = query.all()
    
    # Python-side filtering for coordinate glob and metadata since SQLite json querying is complex/not always compiled in
    result_ids = []
    for m in mats:
        # Coordinate glob check
        if selection.coordinate_glob:
            # We need the coordinate for this materialization. We joined above if glob was present.
            # Easiest way is to fetch current output for it
            co = session.query(RunCoordinate).filter_by(materialization_id=m.id).order_by(RunCoordinate.id.desc()).first()
            if not co or not fnmatch.fnmatch(co.coordinate, selection.coordinate_glob):
                continue
                
        # Metadata check
        if selection.metadata:
            if not m.metadata_json:
                continue
            try:
                meta_dict = json.loads(m.metadata_json)
            except:
                continue
                
            passed = True
            for f in selection.metadata:
                if f.op == "exists":
                    if f.key not in meta_dict:
                        passed = False; break
                elif f.op == "does not exist":
                    if f.key in meta_dict:
                        passed = False; break
                else:
                    if f.key not in meta_dict:
                        passed = False; break
                    val = meta_dict[f.key]
                    if f.op == "equals" and val != f.value:
                        passed = False; break
                    if f.op == "not equals" and val == f.value:
                        passed = False; break
                    if f.op == "greater than" and not (val > f.value):
                        passed = False; break
                    if f.op == "less than" and not (val < f.value):
                        passed = False; break
            
            if not passed:
                continue
                
        result_ids.append(m.id)
        
    return result_ids
