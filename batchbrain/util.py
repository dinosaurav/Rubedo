from datetime import datetime, timezone


def utcnow_iso() -> str:
    """Timezone-aware UTC timestamp in sortable ISO-8601 form with a Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def iso_age_seconds(iso: str) -> float:
    """Seconds elapsed since an utcnow_iso()-style timestamp."""
    then = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - then).total_seconds()
