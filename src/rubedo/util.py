"""
Utility functions for time and date handling in the Rubedo engine.
"""
import os
from datetime import datetime, timezone


def utcnow_iso() -> str:
    """Timezone-aware UTC timestamp in sortable ISO-8601 form with a Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_age_seconds(iso: str) -> float:
    """Seconds elapsed since an utcnow_iso()-style timestamp."""
    then = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - then).total_seconds()


def _ensure_gitignore(directory: str):
    """
    Ensure the given directory has a .gitignore file that ignores all contents except itself.
    """
    if not directory:
        return
    gitignore_path = os.path.join(directory, ".gitignore")
    if not os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, "w") as f:
                f.write(
                    "# Ignore everything in this directory\n*\n# Except this file\n!.gitignore\n"
                )
        except Exception:
            pass
