"""Helpers for interpreting auto_* time window syntax."""
from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Tuple

AUTO_PREFIX = "auto_"
_AUTO_PATTERN = re.compile(r"(\d+)([dhms])", flags=re.IGNORECASE)
_UNIT_MAP = {
    "d": "days",
    "h": "hours",
    "m": "minutes",
    "s": "seconds",
}


class AutoTimeParseError(ValueError):
    """Raised when auto_* strings cannot be parsed."""


def parse_auto_time(spec: str, *, reference: datetime | None = None) -> datetime:
    """Convert an auto_* spec into an absolute UTC datetime."""

    if not isinstance(spec, str):
        raise AutoTimeParseError("Time spec must be a string.")
    spec_lower = spec.lower()
    if not spec_lower.startswith(AUTO_PREFIX):
        raise AutoTimeParseError("Time spec must start with 'auto_'.")

    tail = spec_lower[len(AUTO_PREFIX) :]
    if not tail:
        raise AutoTimeParseError("auto_ specification requires at least one unit.")

    total = timedelta()
    index = 0
    for match in _AUTO_PATTERN.finditer(tail):
        value = int(match.group(1))
        unit = match.group(2).lower()
        total += timedelta(**{_UNIT_MAP[unit]: value})
        index = match.end()

    if index != len(tail):
        raise AutoTimeParseError(f"Invalid auto spec component near '{tail[index:]}'")

    ref = reference or datetime.utcnow()
    return ref - total


def resolve_auto_window(
    start_spec: str,
    end_spec: str,
    *,
    reference: datetime | None = None,
) -> Tuple[datetime, datetime]:
    """Return (start, end) datetimes based on auto specs."""

    ref = reference or datetime.utcnow()
    start = parse_auto_time(start_spec, reference=ref)
    end = parse_auto_time(end_spec, reference=ref)
    if start >= end:
        raise AutoTimeParseError("start_time must be earlier than end_time")
    return start, end


__all__ = ["AutoTimeParseError", "parse_auto_time", "resolve_auto_window"]
