import heapq
from bisect import bisect_left, bisect_right
from datetime import date, datetime, timedelta, timezone

from parsers.common import EST

UTC = timezone.utc


def est_day(start: datetime) -> date:
    """IESO's delivery date of an hour: the EST calendar date of its start."""
    return start.astimezone(EST).date()


def est_midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=EST).astimezone(UTC)


class Series:
    """Hourly Ontario demand: (hour start in UTC, MW). Knows nothing about *when* each value became public;
    callers pass a decision time and the series only answers from days that were complete by then."""

    def __init__(self, points):
        self.points = sorted(points)
        self._times = [t for t, _ in self.points]
        self._by_start = dict(self.points)
        daily = {}
        for start, mw in self.points:
            day = est_day(start)
            daily[day] = max(daily.get(day, mw), mw)
        self._days = sorted(daily)
        self._peaks = [daily[d] for d in self._days]
        self._day_ends = [est_midnight(d + timedelta(days=1)) for d in self._days]
        self._cache = {}

    def demand_at(self, start: datetime):
        return self._by_start.get(start)

    def peak_of(self, day: date):
        index = bisect_left(self._days, day)
        return self._peaks[index] if index < len(self._days) and self._days[index] == day else None

    def times_between(self, first: datetime, last: datetime):
        """Hour starts in [first, last)."""
        return self._times[bisect_left(self._times, first):bisect_left(self._times, last)]

    def fifth_highest(self, complete_by: datetime, base_start: date):
        """The 5th-highest daily peak among days from base_start that were complete by `complete_by`.
        None until five such days exist. A day's peak only depends on that day, so it is safe to use once it is over."""
        hi = bisect_right(self._day_ends, complete_by)
        lo = bisect_left(self._days, base_start)
        if (lo, hi) not in self._cache:
            values = self._peaks[lo:hi]
            self._cache[(lo, hi)] = heapq.nlargest(5, values)[-1] if len(values) >= 5 else None
        return self._cache[(lo, hi)]
