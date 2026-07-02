import os
from dataclasses import dataclass
from .hashing import hash_file


@dataclass
class ScannedFile:
    coordinate: str
    absolute_path: str
    size_bytes: int
    mtime_ns: int
    content_hash: str


def scan_folder(folder: str) -> list[ScannedFile]:
    files = []
    base_path = os.path.abspath(folder)

    if not os.path.exists(base_path):
        return files

    for root, _, filenames in os.walk(base_path):
        for name in filenames:
            abs_path = os.path.join(root, name)
            rel_path = os.path.relpath(abs_path, base_path)
            # Use forward slashes for coordinate
            coordinate = rel_path.replace(os.sep, "/")

            st = os.stat(abs_path)
            content_hash = hash_file(abs_path)

            files.append(
                ScannedFile(
                    coordinate=coordinate,
                    absolute_path=abs_path,
                    size_bytes=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                    content_hash=content_hash,
                )
            )

    return files
