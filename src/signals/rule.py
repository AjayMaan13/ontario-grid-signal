"""The peak-risk rule. A pure function: given what was known at the decision time, flag the hour or not."""
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from parsers.common import EST
from signals.series import Series, est_day, est_midnight

# An hour's demand is public about 12 minutes after it ends (measured on this project's own data: median 11.7 min,
# 135 of 137 normal hours within 20 minutes). 15 minutes is used everywhere "known" matters.
LAG = timedelta(minutes=15)
HOUR = timedelta(hours=1)

# How the rule guesses an hour's demand ahead of time:
#   oracle     the hour's own demand. NOT usable live: a ceiling that shows how selective the rule is.
#   persistN   the latest published hour, which is N hours old (about N-1.25 hours of notice)
#   day_ahead  yesterday's peak, known as soon as the day is over (many hours of notice)
STALENESS = {"persist2": 2, "persist4": 4}
VARIANTS = ("oracle", "persist2", "persist4", "day_ahead")


@dataclass(frozen=True)
class Window:
    """When a peak is plausible, learned from the training peaks. Hours are IESO hour-ending (EST)."""
    months: frozenset
    hour_min: int
    hour_max: int


@dataclass(frozen=True)
class Params:
    margin: float  # flag when the estimate reaches margin x the bar
    window: Window


@dataclass(frozen=True)
class Flag:
    risk: bool
    reason: str
    estimate: float | None
    bar: float | None
    decided_at: datetime


def hour_ending(start: datetime) -> int:
    return start.astimezone(EST).hour + 1


def in_window(start: datetime, window: Window) -> bool:
    return start.astimezone(EST).month in window.months and window.hour_min <= hour_ending(start) <= window.hour_max


def base_start(day: date) -> date:
    """The ICI base period runs 1 May to 30 April."""
    return date(day.year if day.month >= 5 else day.year - 1, 5, 1)


def decision_time(variant: str, start: datetime) -> datetime:
    """The moment the decision about the hour starting at `start` is made."""
    if variant == "oracle":
        return start
    if variant == "day_ahead":
        return est_midnight(est_day(start)) + LAG  # just after yesterday ended
    return start - (STALENESS[variant] - 1) * HOUR + LAG  # just after the latest usable hour was published


def estimate(variant: str, series: Series, start: datetime):
    if variant == "oracle":
        return series.demand_at(start)
    if variant == "day_ahead":
        return series.peak_of(est_day(start) - timedelta(days=1))
    return series.demand_at(start - STALENESS[variant] * HOUR)


def decide(series: Series, start: datetime, variant: str, params: Params) -> Flag:
    """Flag the hour starting at `start`, using only what was known at its decision time."""
    when = decision_time(variant, start)
    if not in_window(start, params.window):
        return Flag(False, "outside the plausible peak window", None, None, when)
    guess = estimate(variant, series, start)
    if guess is None:
        return Flag(False, "no demand data to estimate from", None, None, when)
    bar = series.fifth_highest(when - LAG, base_start(est_day(start)))
    if bar is None:
        return Flag(False, "warm-up: fewer than 5 complete days in this base period", guess, None, when)
    if guess >= params.margin * bar:
        return Flag(True, f"estimate {guess:,.0f} MW reaches {params.margin:.2f} x {bar:,.0f} MW (5th-highest daily peak so far)", guess, bar, when)
    return Flag(False, f"estimate {guess:,.0f} MW is below {params.margin:.2f} x {bar:,.0f} MW", guess, bar, when)
