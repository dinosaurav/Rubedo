"""
Data sources for Rubedo pipelines. Provides mechanisms for iterating over files, CSVs, and database tables.
"""
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .hashing import hash_file


@dataclass
class SourceItem:
    """One coordinate produced by a Source scan.

    `ref` is an opaque handle the owning Source uses in load(); it never
    participates in identity — only `coordinate` and `content_hash` do.
    """

    coordinate: str
    content_hash: str
    ref: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

def _disambiguate(
    rows: List[tuple[str, str, Any, Dict[str, Any]]]
) -> List[SourceItem]:
    """Resolve duplicate keys into single coordinates or content-suffixed coordinates."""
    from collections import defaultdict
    
    # base lane key -> {content_hash: (ref, metadata)}; identical (key, content)
    # rows are indistinguishable units of work and collapse to one lane
    groups: Dict[str, Dict[str, tuple]] = defaultdict(dict)
    
    for base, content_hash, ref, meta in rows:
        groups[base].setdefault(content_hash, (ref, meta))
        
    items: List[SourceItem] = []
    for base, variants in groups.items():
        collided = len(variants) > 1
        for content_hash, (ref, meta) in variants.items():
            coordinate = f"{base}#{content_hash[:6]}" if collided else base
            metadata = dict(meta)
            if collided:
                metadata["key_collision"] = True
            items.append(
                SourceItem(
                    coordinate=coordinate,
                    content_hash=content_hash,
                    ref=ref,
                    metadata=metadata,
                )
            )
            
    return items


class Source(ABC):
    """Anything that can enumerate coordinates and load their payloads.

    A coordinate must be stable across scans: the same logical item keeps
    the same coordinate even when its content changes, so the engine can
    tell "changed" (same coordinate, new hash) from "removed + added".
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """Stable identity string, recorded as source_id on runs/manifests."""

    @abstractmethod
    def scan(self) -> List[SourceItem]:
        """Snapshot enumeration of all current coordinates."""

    @abstractmethod
    def load(self, item: SourceItem) -> Any:
        """Payload handed to root steps for this coordinate."""


class FolderSource(Source):
    """Files under a folder. Coordinate = relative path, payload = absolute path."""

    def __init__(self, path: str):
        self.path = path

    @property
    def id(self) -> str:
        return f"folder:{self.path}"

    def scan(self) -> List[SourceItem]:
        items: List[SourceItem] = []
        base_path = os.path.abspath(self.path)

        if not os.path.exists(base_path):
            return items

        for root, _, filenames in os.walk(base_path):
            for name in filenames:
                abs_path = os.path.join(root, name)
                rel_path = os.path.relpath(abs_path, base_path)
                # Use forward slashes for coordinate
                coordinate = rel_path.replace(os.sep, "/")

                st = os.stat(abs_path)
                items.append(
                    SourceItem(
                        coordinate=coordinate,
                        content_hash=hash_file(abs_path),
                        ref=abs_path,
                        metadata={"size_bytes": st.st_size, "mtime_ns": st.st_mtime_ns},
                    )
                )

        return items

    def load(self, item: SourceItem) -> str:
        return item.ref


class CsvSource(Source):
    """Rows of a CSV file. Coordinate = key column value(s), payload = row dict.

    `key` is deliberately required: coordinates must stay stable when rows are
    inserted or edited, and only the caller knows which column(s) identify a
    row. Pass key=None to opt into content-addressed coordinates, where an
    edited row shows up as removed + created rather than changed.

    The coordinate is a lane key, not a uniqueness constraint on your data:
    duplicate keys are handled mechanically. Rows sharing a key *and* content
    are indistinguishable units of work and collapse into one lane; rows
    sharing a key with different content get content-suffixed lanes
    ("bob#3f2a9c"), where an edit reads as removed + created. Searching by
    the human-facing key value is the job of indexed fields, not the lane.
    """

    def __init__(self, path: str, *, key):
        self.path = path
        if isinstance(key, str):
            key = [key]
        self.key: Optional[List[str]] = key

    @property
    def id(self) -> str:
        key_part = ",".join(self.key) if self.key else "@content"
        return f"csv:{self.path}#key={key_part}"

    def scan(self) -> List[SourceItem]:
        import csv
        from .hashing import hash_json

        if not os.path.exists(self.path):
            return []

        rows = []
        with open(self.path, newline="", encoding="utf-8") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                content_hash = hash_json(row)

                if self.key:
                    missing = [k for k in self.key if k not in row]
                    if missing:
                        raise ValueError(
                            f"{self.path}: key column(s) {missing} not found in CSV header"
                        )
                    base = "|".join(str(row[k]) for k in self.key)
                else:
                    base = f"row-{content_hash[:12]}"

                rows.append((base, content_hash, row, {"line": row_num}))

        return _disambiguate(rows)

    def load(self, item: SourceItem) -> Dict[str, str]:
        return item.ref


def coerce_source(source) -> Source:
    """Accept a Source or a folder path string (sugar for FolderSource)."""
    if isinstance(source, Source):
        return source
    if isinstance(source, str):
        return FolderSource(source)
    raise TypeError(f"Expected a Source or folder path string, got {type(source)!r}")

def _jsonable(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a dictionary containing complex types into JSON-serializable primitives."""
    import datetime
    from decimal import Decimal
    
    out = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = str(v)
        elif isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
            out[k] = v.isoformat()
        elif isinstance(v, bytes):
            out[k] = v.hex()
        elif hasattr(v, '__dict__'):
            # Just stringify complex objects that slipped through
            out[k] = str(v)
        else:
            out[k] = v
    return out

class TableSource(Source):
    """Rows of a SQL table. Coordinate = key column value(s), payload = row dict.

    By default the whole table is read once during `scan()` and each row's
    payload rides along in the SourceItem (like CsvSource) — one query, and
    `load()` is a passthrough.

    Pass `batch_size=N` when the table is too large to hold in memory. Then
    `scan()` streams the rows in server-side chunks of `N`, keeping only the
    `(key, content_hash)` of each and discarding the payload; `load()`
    re-fetches a single row by key when its lane actually runs. This bounds
    memory to ~N payloads at a time, at the cost of one query per lane and
    the row having to still exist at load time (a row deleted between scan
    and load raises — the same exposure FolderSource has with files).

    `batch_size` is an operational knob: it changes *how* rows are read, not
    which coordinates or content exist, so it is deliberately absent from
    `id` and toggling it never invalidates the cache.
    """

    def __init__(
        self,
        engine_url: str,
        *,
        table: str,
        key,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
    ):
        self.engine_url = engine_url
        self.table = table
        if isinstance(key, str):
            key = [key]
        self.key: List[str] = key
        self.columns = columns
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive int, got {batch_size!r}")
        self.batch_size = batch_size
        self._engine = None

    @property
    def id(self) -> str:
        from urllib.parse import urlparse

        # Parse the URL and remove credentials for the id
        parsed = urlparse(self.engine_url)
        safe_netloc = parsed.hostname or ""
        if parsed.port:
            safe_netloc += f":{parsed.port}"

        # Build dialect+host+database+table#key=id
        safe_url = f"{parsed.scheme}://{safe_netloc}{parsed.path}"
        key_part = ",".join(self.key)
        return f"table:{safe_url}/{self.table}#key={key_part}"

    def _get_engine(self):
        # Cached and reused: streaming mode re-enters the DB on every load(),
        # so a fresh engine per call would mean a new connection pool per lane.
        if self._engine is None:
            from sqlalchemy import create_engine

            self._engine = create_engine(self.engine_url)
        return self._engine

    def _cols(self) -> str:
        # Unsafe interpolation is accepted here: table/column names are an
        # internal schema declaration from pipeline code, never user input.
        return "*" if not self.columns else ", ".join(self.columns)

    def scan(self) -> List[SourceItem]:
        from sqlalchemy import text
        from .hashing import hash_json

        conn = self._get_engine().connect()
        if self.batch_size is not None:
            conn = conn.execution_options(yield_per=self.batch_size)
        try:
            query = text(f"SELECT {self._cols()} FROM {self.table}")
            rows_data = []
            for row in conn.execute(query).mappings():
                row_dict = _jsonable(dict(row))
                content_hash = hash_json(row_dict)

                missing = [k for k in self.key if k not in row_dict]
                if missing:
                    raise ValueError(
                        f"{self.table}: key column(s) {missing} not found in row"
                    )
                base = "|".join(str(row_dict[k]) for k in self.key)

                if self.batch_size is None:
                    # Eager: carry the payload so load() is a passthrough.
                    ref: Any = row_dict
                else:
                    # Streaming: keep only what re-fetches the row later. Raw
                    # DB values (not the jsonable copy) so binds match types.
                    ref = {k: row[k] for k in self.key}
                rows_data.append((base, content_hash, ref, {}))

            return _disambiguate(rows_data)
        finally:
            conn.close()

    def load(self, item: SourceItem) -> Dict[str, Any]:
        if self.batch_size is None:
            return item.ref

        from sqlalchemy import text
        from .hashing import hash_json

        where = " AND ".join(f"{k} = :k{i}" for i, k in enumerate(self.key))
        binds = {f"k{i}": item.ref[k] for i, k in enumerate(self.key)}
        query = text(f"SELECT {self._cols()} FROM {self.table} WHERE {where}")
        with self._get_engine().connect() as conn:
            candidates = [_jsonable(dict(r)) for r in conn.execute(query, binds).mappings()]

        if not candidates:
            raise ValueError(
                f"{self.table}: row for key {item.ref} is gone since the scan"
            )
        if len(candidates) == 1:
            return candidates[0]
        # Duplicate key: content-suffixed lanes share a key, so pick the row
        # whose bytes match the coordinate we planned.
        for row_dict in candidates:
            if hash_json(row_dict) == item.content_hash:
                return row_dict
        raise ValueError(
            f"{self.table}: no row for key {item.ref} matches planned content"
        )
