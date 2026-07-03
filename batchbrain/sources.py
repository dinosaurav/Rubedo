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
        from collections import defaultdict

        from .hashing import hash_json

        if not os.path.exists(self.path):
            return []

        # base lane key -> {content_hash: (row, line)}; identical (key, content)
        # rows are indistinguishable units of work and collapse to one lane
        groups: Dict[str, Dict[str, tuple]] = defaultdict(dict)

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

                groups[base].setdefault(content_hash, (row, row_num))

        items: List[SourceItem] = []
        for base, variants in groups.items():
            collided = len(variants) > 1
            for content_hash, (row, line) in variants.items():
                coordinate = f"{base}#{content_hash[:6]}" if collided else base
                metadata = {"line": line}
                if collided:
                    metadata["key_collision"] = True
                items.append(
                    SourceItem(
                        coordinate=coordinate,
                        content_hash=content_hash,
                        ref=row,
                        metadata=metadata,
                    )
                )

        return items

    def load(self, item: SourceItem) -> Dict[str, str]:
        return item.ref


def coerce_source(source) -> Source:
    """Accept a Source or a folder path string (sugar for FolderSource)."""
    if isinstance(source, Source):
        return source
    if isinstance(source, str):
        return FolderSource(source)
    raise TypeError(f"Expected a Source or folder path string, got {type(source)!r}")
