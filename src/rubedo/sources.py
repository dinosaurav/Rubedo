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

def _finalize(
    rows: List[tuple[str, str, Any, Dict[str, Any]]]
) -> List[SourceItem]:
    """Turn (coordinate, content_hash, ref, meta) rows into SourceItems.

    Identical (coordinate, content) rows are indistinguishable units of work
    and collapse to a single lane. A coordinate that maps to two *different*
    contents means a declared key is not unique — that is an error, not a
    silent content-suffix. Omit key= for content-addressed lanes, where
    distinct rows are simply distinct coordinates and identical rows collapse.
    """
    seen: Dict[str, str] = {}
    items: List[SourceItem] = []
    for coordinate, content_hash, ref, meta in rows:
        prior = seen.get(coordinate)
        if prior is not None:
            if prior != content_hash:
                raise ValueError(
                    f"coordinate {coordinate!r} maps to two different rows: a "
                    "declared key must be unique. Omit key= for "
                    "content-addressed lanes."
                )
            continue  # identical unit of work — one lane
        seen[coordinate] = content_hash
        items.append(
            SourceItem(
                coordinate=coordinate,
                content_hash=content_hash,
                ref=ref,
                metadata=meta,
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
        """Stable identity string, recorded as source_id on runs."""

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
    """Rows of a CSV file. Payload = row dict; content-addressed lanes.

    Each row is a lane keyed by its content (`row-<hash>`): identical rows
    collapse to one lane, and an edited row reads as removed + created. To find
    or track a row by a human field (email, id), index it downstream with
    `@step(index=[...])` and query — the coordinate is never a human key.
    """

    def __init__(self, path: str):
        self.path = path

    @property
    def id(self) -> str:
        return f"csv:{self.path}"

    def scan(self) -> List[SourceItem]:
        import csv
        from .hashing import hash_json

        if not os.path.exists(self.path):
            return []

        rows = []
        with open(self.path, newline="", encoding="utf-8") as f:
            for row_num, row in enumerate(csv.DictReader(f), start=2):
                content_hash = hash_json(row)
                rows.append(
                    (f"row-{content_hash[:12]}", content_hash, row, {"line": row_num})
                )

        return _finalize(rows)

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

    Lanes are content-addressed (`row-<hash>`) like CsvSource. `key` is *not* a
    lane key — it names the column(s) `load()` re-fetches a streamed row by, so
    it is required only when `batch_size` is set and is otherwise unused. It
    never affects the coordinate.
    """

    def __init__(
        self,
        engine_url: str,
        *,
        table: str,
        key=None,
        columns: Optional[List[str]] = None,
        batch_size: Optional[int] = None,
    ):
        self.engine_url = engine_url
        self.table = table
        if isinstance(key, str):
            key = [key]
        self.key: Optional[List[str]] = key
        self.columns = columns
        if batch_size is not None and batch_size < 1:
            raise ValueError(f"batch_size must be a positive int, got {batch_size!r}")
        if batch_size is not None and not key:
            raise ValueError(
                "TableSource streaming (batch_size) requires key= — the "
                "column(s) load() re-fetches a row by (not the lane key; lanes "
                "are content-addressed). Omit batch_size for eager mode."
            )
        self.batch_size = batch_size
        self._engine: Any = None

    @property
    def id(self) -> str:
        from urllib.parse import urlparse

        # Parse the URL and remove credentials for the id
        parsed = urlparse(self.engine_url)
        safe_netloc = parsed.hostname or ""
        if parsed.port:
            safe_netloc += f":{parsed.port}"

        # Build dialect+host+database+table (key is a re-fetch detail, not identity)
        safe_url = f"{parsed.scheme}://{safe_netloc}{parsed.path}"
        return f"table:{safe_url}/{self.table}"

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
            rows_data: List[tuple[str, str, Any, Dict[str, Any]]] = []
            for row in conn.execute(query).mappings():
                row_dict = _jsonable(dict(row))
                content_hash = hash_json(row_dict)
                base = f"row-{content_hash[:12]}"

                if self.batch_size is None:
                    # Eager: carry the payload so load() is a passthrough.
                    ref: Any = row_dict
                else:
                    # Streaming: keep only what re-fetches the row later. Raw
                    # DB values (not the jsonable copy) so binds match types.
                    assert self.key is not None
                    ref = {k: row[k] for k in self.key}
                rows_data.append((base, content_hash, ref, {}))

            return _finalize(rows_data)
        finally:
            conn.close()

    def load(self, item: SourceItem) -> Dict[str, Any]:
        if self.batch_size is None:
            return item.ref

        from sqlalchemy import text
        from .hashing import hash_json

        assert self.key is not None
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
        # The re-fetch key matched more than one row (non-unique key, or the row
        # changed since scan): pick the one whose bytes match the planned lane.
        for row_dict in candidates:
            if hash_json(row_dict) == item.content_hash:
                return row_dict
        raise ValueError(
            f"{self.table}: no row for key {item.ref} matches planned content"
        )
