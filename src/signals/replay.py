"""python -m signals.replay 2025-06-24

Shows what the live rule would have flagged around a real past day, using the same code path and the backtest's frozen
choices. No cloud needed. Hours are IESO hour-ending in EST.
"""
import sys
from datetime import date, datetime, timedelta

from parsers.common import EST
from signals.groundtruth import load_series
from signals.live import evaluate, load_params
from signals.rule import HOUR, hour_ending
from signals.series import est_day, est_midnight


def replay(day: date):
    series = load_series(day.year if day.month >= 5 else day.year - 1)
    params = load_params()
    peak_hour = max((t for t in series.times_between(est_midnight(day), est_midnight(day + timedelta(days=1)))), key=series.demand_at)
    print(f"{day}: the day's actual peak was {series.demand_at(peak_hour):,.0f} MW at hour-ending {hour_ending(peak_hour)}\n")
    shown = 0
    for x in series.times_between(est_midnight(day - timedelta(days=1)), est_midnight(day + timedelta(days=1))):
        decided_when = x + HOUR + timedelta(minutes=15)  # the moment hour x became public
        for row in evaluate(series, x, params, decided_when):
            if row["risk"]:
                candidate = datetime.fromisoformat(row["candidate_hour"].replace("Z", "+00:00")).astimezone(EST)
                made = datetime.fromisoformat(row["decided_at"].replace("Z", "+00:00")).astimezone(EST)
                print(f"decided {made:%a %H:%M} EST -> flag hour-ending {candidate.hour + 1} on {candidate:%d %b} ({row['variant']}, {row['notice_hours']} h notice): {row['reason']}")
                shown += 1
    print("\nno flags" if not shown else f"\n{shown} flags")


if __name__ == "__main__":
    replay(date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date(2025, 6, 24))
