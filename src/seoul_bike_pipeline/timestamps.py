"""UTC timestamps for manifest records (PRD §6).

``observed_at`` is the collection instant of a source version and is always
stored as UTC (``YYYY-MM-DDTHH:MM:SSZ``), so a record never depends on the
local timezone of the machine that registered it.
"""

from __future__ import annotations

from datetime import datetime, timezone

_UTC = timezone.utc
_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def utc_now() -> str:
    """Current UTC instant in the manifest's canonical form."""
    return datetime.now(_UTC).strftime(_FORMAT)


def parse_iso8601(value: str) -> datetime:
    """Parse an ISO-8601 timestamp, reading a missing offset as UTC."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_UTC)
    return parsed


def to_utc(value: str) -> str:
    """Normalize an ISO-8601 timestamp to the canonical UTC form."""
    return parse_iso8601(value).astimezone(_UTC).strftime(_FORMAT)
