from datetime import date
from pathlib import Path

from parsers.ici_demand import parse_ici_demand
from parsers.ici_peak_tracker import parse_ici_peak_tracker
from signals.series import Series, est_day, est_midnight

DATA = Path(__file__).resolve().parents[2] / "backtest" / "data"

# Where each base period's five peaks come from. 2024's tracker file actually holds May 2025, so it is derived.
SOURCE = {2022: "IESO Peak Tracker", 2023: "IESO Peak Tracker", 2024: "derived from ICIDemand", 2025: "IESO Peak Tracker"}


def load_series(year: int) -> Series:
    rows = parse_ici_demand((DATA / f"PUB_ICIDemand_{year}.csv").read_text())
    return Series([(row.interval_start, row.demand_mw) for row in rows])


def derive_peaks(series: Series, year: int):
    """The five highest daily peaks of the base period: each day's highest hour, then the top five days.
    This reproduces IESO's own tracker exactly for 2022, 2023 and 2025 (see tests)."""
    first, last = est_midnight(date(year, 5, 1)), est_midnight(date(year + 1, 5, 1))
    best = {}
    for start in series.times_between(first, last):
        mw = series.demand_at(start)
        day = est_day(start)
        if day not in best or mw > best[day][0]:
            best[day] = (mw, start)
    return sorted((start for mw, start in sorted(best.values(), key=lambda v: (-v[0], v[1]))[:5]))


def tracker_peaks(year: int):
    peaks = parse_ici_peak_tracker((DATA / f"PUB_ICIPeakTracker_{year}.xml").read_text())
    return sorted(p.interval_start for p in peaks[:5])


def peaks(year: int, series: Series):
    return tracker_peaks(year) if SOURCE[year].startswith("IESO") else derive_peaks(series, year)
