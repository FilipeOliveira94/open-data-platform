"""Time helpers: the agency's local time is a fixed UTC-3 offset.

Brazil has had no daylight saving time since 2019, so local time needs no
time-zone database. Internally every instant is an integer of epoch seconds.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

LOCAL_OFFSET_SECONDS = -3 * 3600
LOCAL_SUFFIX = "-03:00"
DAY_SECONDS = 86400
_TWO_DIGITS = tuple(f"{n:02d}" for n in range(60))


def utc_midnight(day: date) -> int:
    """Epoch seconds of 00:00 UTC on ``day``."""
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())


def local_midnight(day: date) -> int:
    """Epoch seconds of 00:00 local time (03:00 UTC) on ``day``."""
    return utc_midnight(day) - LOCAL_OFFSET_SECONDS


def local_date_of(epoch: int) -> date:
    """The local calendar date of an instant."""
    return datetime.fromtimestamp(epoch + LOCAL_OFFSET_SECONDS, UTC).date()


def utc_date_of(epoch: int) -> date:
    return datetime.fromtimestamp(epoch, UTC).date()


def format_local(epoch: int) -> str:
    """ISO-8601 local time with the explicit offset, second precision."""
    moment = datetime.fromtimestamp(epoch + LOCAL_OFFSET_SECONDS, UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S") + LOCAL_SUFFIX


def format_utc_ms(epoch_ms: int) -> str:
    """ISO-8601 UTC time with millisecond precision and a ``Z`` suffix."""
    seconds, millis = divmod(epoch_ms, 1000)
    moment = datetime.fromtimestamp(seconds, UTC)
    return f"{moment.strftime('%Y-%m-%dT%H:%M:%S')}.{millis:03d}Z"


def days(first: date, last: date) -> list[date]:
    """Every date from ``first`` to ``last``, both included."""
    count = (last - first).days + 1
    return [first + timedelta(days=offset) for offset in range(max(count, 0))]


class LocalClock:
    """Pre-formatted local timestamps for whole local days, one per second.

    Big tables format millions of timestamps per day; a list lookup is much
    cheaper than formatting a datetime each time. Instants outside the span
    fall back to ``format_local``.
    """

    __slots__ = ("base", "_cache")

    def __init__(self, first_day: date, day_count: int) -> None:
        self.base = local_midnight(first_day)
        cache: list[str] = []
        for offset in range(day_count):
            day_text = (first_day + timedelta(days=offset)).isoformat()
            for hour in range(24):
                for minute in range(60):
                    prefix = f"{day_text}T{_TWO_DIGITS[hour]}:{_TWO_DIGITS[minute]}:"
                    cache.extend(
                        prefix + second + LOCAL_SUFFIX for second in _TWO_DIGITS
                    )
        self._cache = cache

    def fmt(self, epoch: int) -> str:
        index = epoch - self.base
        if 0 <= index < len(self._cache):
            return self._cache[index]
        return format_local(epoch)
