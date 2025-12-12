from __future__ import annotations

from datetime import date, datetime, time, timezone
from typing import Optional, Union

from zoneinfo import ZoneInfo

MYT = ZoneInfo("Asia/Kuala_Lumpur")


DatetimeLike = Optional[Union[datetime, date]]


def now_myt() -> datetime:
    return datetime.now(tz=MYT)


def ensure_myt_datetime(value: DatetimeLike) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime.combine(value, time.min, tzinfo=MYT)
    if value.tzinfo is None:
        # treat naive timestamps as already in MYT so we do not shift the value
        return value.replace(tzinfo=MYT)
    return value.astimezone(MYT)


def to_myt_datetime(value: DatetimeLike) -> Optional[datetime]:
    return ensure_myt_datetime(value)


def format_myt(value: DatetimeLike, fmt: str = "%Y-%m-%d %H:%M") -> str:
    if value is None:
        return "-"
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    converted = to_myt_datetime(value)
    if converted is None:
        return "-"
    return converted.strftime(fmt)


def parse_myt_range_value(raw: str, *, is_range_end: bool = False) -> datetime:
    """Parse date/datetime strings and convert them into MYT-aware datetimes."""
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty date value")
    try:
        # Common case: YYYY-MM-DD input from date pickers
        parsed_date = date.fromisoformat(text)
        base_time = time.max if is_range_end else time.min
        return datetime.combine(parsed_date, base_time, tzinfo=MYT)
    except ValueError:
        pass
    try:
        parsed_dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("Invalid date format") from exc
    converted = ensure_myt_datetime(parsed_dt)
    if converted is None:
        raise ValueError("Unable to convert date")
    return converted
