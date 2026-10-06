"""Replays whole base periods hour by hour through the rule and scores it against the real top-5 peaks.

    python -m signals.backtest          # tune on 2022-23, evaluate once on 2024-25, write backtest/results.json

Walk-forward: the window and the margin are chosen from 2022-2023 ONLY. 2024-2025 are scored with those frozen choices.
"""
import json
import statistics
import sys
from datetime import date
from pathlib import Path

from parsers.common import EST
from signals.groundtruth import SOURCE, load_series, peaks
from signals.rule import VARIANTS, Params, Window, decide, decision_time, hour_ending, in_window
from signals.series import est_day, est_midnight

TRAIN, HOLDOUT = [2022, 2023], [2024, 2025]
GRID = [round(0.80 + 0.01 * i, 2) for i in range(21)]  # margins 0.80 .. 1.00
TARGET = 8  # tuning goal: catch at least 8 of the 10 training peaks, with as few alert-hours as possible
RESULTS = Path(__file__).resolve().parents[2] / "backtest" / "results.json"


def derive_window(train_peaks) -> Window:
    """Months and hours seen in the training peaks, widened by one hour each side (fixed in advance, not tuned)."""
    months = frozenset(p.astimezone(EST).month for p in train_peaks)
    hours = [hour_ending(p) for p in train_peaks]
    return Window(months, max(1, min(hours) - 1), min(24, max(hours) + 1))


def evaluate(series, year: int, variant: str, params: Params, truth) -> dict:
    """Score one base period. Every hour is decided using only what was known at that hour's decision time."""
    first, last = est_midnight(date(year, 5, 1)), est_midnight(date(year + 1, 5, 1))
    flagged, in_window_hours = [], 0
    for start in series.times_between(first, last):
        if in_window(start, params.window):
            in_window_hours += 1
        if decide(series, start, variant, params).risk:
            flagged.append(start)
    caught = [p for p in truth if p in set(flagged)]
    leads = [(p - decision_time(variant, p)).total_seconds() / 3600 for p in caught]
    peaks_in_window = sum(in_window(p, params.window) for p in truth)
    return {
        "captured": len(caught), "peaks": len(truth), "peaks_in_window": peaks_in_window,
        "alert_hours": len(flagged), "alert_days": len({est_day(s) for s in flagged}), "in_window_hours": in_window_hours,
        "chance_captured": round(peaks_in_window * len(flagged) / in_window_hours, 2) if in_window_hours else 0,
        "median_lead_hours": round(statistics.median(leads), 1) if leads else None,
        "missed": [p.isoformat() for p in truth if p not in caught],
    }


def tune(series, truth, variant: str, window: Window):
    """Pick the margin with the fewest alert-hours that still catches TARGET of the training peaks."""
    frontier = []
    for margin in GRID:
        results = [evaluate(series[y], y, variant, Params(margin, window), truth[y]) for y in TRAIN]
        frontier.append({"margin": margin, "captured": sum(r["captured"] for r in results), "alert_hours": sum(r["alert_hours"] for r in results)})
    good = [f for f in frontier if f["captured"] >= TARGET]
    chosen = min(good, key=lambda f: (f["alert_hours"], -f["margin"])) if good else max(frontier, key=lambda f: (f["captured"], -f["alert_hours"]))
    return frontier, chosen["margin"]


def run_all() -> dict:
    series = {year: load_series(year) for year in TRAIN + HOLDOUT}
    truth = {year: peaks(year, series[year]) for year in series}
    window = derive_window([p for y in TRAIN for p in truth[y]])
    out = {"split": {"train": TRAIN, "holdout": HOLDOUT}, "ground_truth": {str(y): SOURCE[y] for y in series},
           "window": {"months": sorted(window.months), "hour_ending": [window.hour_min, window.hour_max]},
           "training_target": f">= {TARGET} of {10 * 1} training peaks", "variants": {}}
    for variant in VARIANTS:
        frontier, margin = tune(series, truth, variant, window)
        params = Params(margin, window)
        train = {str(y): evaluate(series[y], y, variant, params, truth[y]) for y in TRAIN}
        holdout = {str(y): evaluate(series[y], y, variant, params, truth[y]) for y in HOLDOUT}
        total = {k: round(sum(r[k] for r in holdout.values()), 2) for k in ("captured", "peaks", "alert_hours", "chance_captured")}
        out["variants"][variant] = {"margin": margin, "training_frontier": frontier, "train": train, "holdout": holdout, "holdout_total": total}
    return out


def markdown(results: dict) -> str:
    w = results["window"]
    lines = [f"Window learned from 2022-23 peaks: months {w['months']}, hour-ending {w['hour_ending'][0]}-{w['hour_ending'][1]}", "",
             "| Variant | Margin | Train captured | Held-out captured | Held-out alert-hours | By chance | Median notice (h) |", "|---|---|---|---|---|---|---|"]
    for name, v in results["variants"].items():
        train = sum(r["captured"] for r in v["train"].values())
        leads = [r["median_lead_hours"] for r in v["holdout"].values() if r["median_lead_hours"] is not None]
        t = v["holdout_total"]
        lines.append(f"| {name} | {v['margin']:.2f} | {train}/10 | {int(t['captured'])}/{int(t['peaks'])} | {int(t['alert_hours'])} | {t['chance_captured']:.1f} | {statistics.median(leads) if leads else '-'} |")
    return "\n".join(lines)


if __name__ == "__main__":
    results = run_all()
    RESULTS.write_text(json.dumps(results, indent=1, sort_keys=True) + "\n")
    print(markdown(results))
    sys.exit(0)
