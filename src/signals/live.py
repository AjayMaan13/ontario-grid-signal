"""The live evaluator's logic: the SAME decide() as the backtest, run as each new hour of demand becomes public."""
import json
from datetime import datetime, timedelta
from pathlib import Path

from producer.events import iso
from signals.rule import HOUR, STALENESS, Params, Window, decide, hour_ending, in_window
from signals.series import est_day, est_midnight

RESULTS = Path(__file__).resolve().parents[2] / "backtest" / "results.json"
LIVE_VARIANTS = ("persist2", "persist4", "day_ahead")  # the oracle is a backtest ceiling and is never run live


def load_params(path=RESULTS) -> dict:
    """The window and margins the backtest chose from 2022-23. Live uses exactly these, never its own."""
    data = json.loads(Path(path).read_text())
    window = Window(frozenset(data["window"]["months"]), *data["window"]["hour_ending"])
    return {variant: Params(data["variants"][variant]["margin"], window) for variant in LIVE_VARIANTS}


def candidates(latest_start: datetime, window: Window):
    """(variant, hour to decide about), given the hour that has just become public.

    persistN looks N hours past the newest hour: that is the moment its estimate is first available.
    day_ahead decides the whole next day's window once the last hour of a day (hour-ending 24) is public.
    """
    out = [(variant, latest_start + hours * HOUR) for variant, hours in STALENESS.items()]
    if hour_ending(latest_start) == 24:
        midnight = est_midnight(est_day(latest_start) + timedelta(days=1))
        hours = [midnight + (he - 1) * HOUR for he in range(window.hour_min, window.hour_max + 1)]
        out += [("day_ahead", hour) for hour in hours if in_window(hour, window)]
    return out


def evaluate(series, latest_start: datetime, params: dict, evaluated_at: datetime) -> list[dict]:
    """One row per decision made now. Off-season hours are recorded too (risk false): the table also shows the system is alive."""
    rows = []
    for variant, candidate in candidates(latest_start, params["persist2"].window):
        flag = decide(series, candidate, variant, params[variant])
        rows.append({
            "variant": variant, "candidate_hour": iso(candidate), "decided_at": iso(flag.decided_at), "risk": flag.risk, "reason": flag.reason,
            "estimate_mw": flag.estimate, "bar_mw": flag.bar, "margin": params[variant].margin,
            "notice_hours": round((candidate - flag.decided_at).total_seconds() / 3600, 2), "evaluated_at": iso(evaluated_at),
        })
    return rows
