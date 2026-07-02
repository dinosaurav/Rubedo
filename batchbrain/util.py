from datetime import datetime, timezone


def utcnow_iso() -> str:
    """Timezone-aware UTC timestamp in sortable ISO-8601 form with a Z suffix."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
