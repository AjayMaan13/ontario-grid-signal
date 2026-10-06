import json
from datetime import datetime, timedelta, timezone

import pytest

from parsers.common import EST
from producer.events import iso
from signals import groundtruth
from signals.live import LIVE_VARIANTS, RESULTS, candidates, evaluate, load_params
from signals.rule import HOUR, decide
from signals.run import handle
from signals.series import Series, est_midnight

PARAMS = load_params()
WINDOW = PARAMS["persist2"].window
SERIES_2025 = groundtruth.load_series(2025)
NOW = datetime(2026, 10, 6, 17, 0, tzinfo=timezone.utc)


def est(year, month, day, hour):
    return datetime(year, month, day, hour, tzinfo=EST).astimezone(timezone.utc)


# ---- parameters: the backtest's, never the live system's own ----

def test_live_uses_exactly_the_margins_and_window_the_backtest_chose():
    results = json.loads(RESULTS.read_text())
    assert set(PARAMS) == set(LIVE_VARIANTS) and "oracle" not in PARAMS
    assert {v: p.margin for v, p in PARAMS.items()} == {v: results["variants"][v]["margin"] for v in LIVE_VARIANTS}
    assert sorted(WINDOW.months) == results["window"]["months"] and [WINDOW.hour_min, WINDOW.hour_max] == results["window"]["hour_ending"]


# ---- which decisions are made when an hour becomes public ----

def test_a_new_hour_triggers_the_two_hour_ahead_decisions_only():
    latest = est(2025, 6, 23, 10)
    assert candidates(latest, WINDOW) == [("persist2", latest + 2 * HOUR), ("persist4", latest + 4 * HOUR)]


def test_the_last_hour_of_a_summer_day_also_triggers_the_whole_next_days_day_ahead_decisions():
    latest = est(2025, 6, 23, 23)  # hour-ending 24
    day_ahead = [c for v, c in candidates(latest, WINDOW) if v == "day_ahead"]
    assert len(day_ahead) == WINDOW.hour_max - WINDOW.hour_min + 1
    assert day_ahead[0] == est(2025, 6, 24, WINDOW.hour_min - 1) and day_ahead[-1] == est(2025, 6, 24, WINDOW.hour_max - 1)


def test_no_day_ahead_decisions_in_the_off_season():
    assert [v for v, _ in candidates(est(2025, 10, 20, 23), WINDOW) if v == "day_ahead"] == []


# ---- the live path IS the backtest's function ----

def test_every_live_decision_equals_what_the_backtest_function_decides():
    checked = 0
    for latest in SERIES_2025.times_between(est(2025, 6, 23, 0), est(2025, 6, 25, 0)):
        for row in evaluate(SERIES_2025, latest, PARAMS, NOW):
            candidate = datetime.fromisoformat(row["candidate_hour"].replace("Z", "+00:00"))
            flag = decide(SERIES_2025, candidate, row["variant"], PARAMS[row["variant"]])
            assert (row["risk"], row["estimate_mw"], row["bar_mw"]) == (flag.risk, flag.estimate, flag.bar)
            checked += 1
    assert checked > 100


def test_the_live_rule_flags_the_2025_peak_hour_with_notice():
    peak = est(2025, 6, 24, 18)  # hour-ending 19, 24,862 MW: the year's highest
    rows = [r for latest in SERIES_2025.times_between(est(2025, 6, 23, 0), est(2025, 6, 25, 0)) for r in evaluate(SERIES_2025, latest, PARAMS, NOW)
            if r["candidate_hour"] == iso(peak) and r["risk"]]
    assert {r["variant"] for r in rows} >= {"persist2", "day_ahead"}
    assert max(r["notice_hours"] for r in rows) > 15  # the day-ahead flag came the evening before


def test_notice_hours_match_each_variants_definition():
    latest = est(2025, 6, 23, 23)  # the day's last hour: it triggers persist2, persist4 and the next day's day-ahead decisions
    rows = evaluate(SERIES_2025, latest, PARAMS, NOW)
    notice = {(r["variant"], r["candidate_hour"]): r["notice_hours"] for r in rows}
    assert notice[("persist2", iso(latest + 2 * HOUR))] == 0.75
    assert notice[("persist4", iso(latest + 4 * HOUR))] == 2.75
    assert notice[("day_ahead", iso(est(2025, 6, 24, 18)))] == 17.75  # decided 00:15, the hour starts at 18:00


def test_off_season_decisions_are_recorded_as_not_at_risk_so_the_table_shows_it_is_alive():
    rows = evaluate(SERIES_2025, est(2025, 10, 20, 10), PARAMS, NOW)
    assert len(rows) == 2 and not any(r["risk"] for r in rows) and all("window" in r["reason"] for r in rows)


# ---- the event handler ----

def event(start, mw, reason="live", ingested=NOW - timedelta(minutes=10)):
    return {"interval_start": iso(start), "value_mw": mw, "reason": reason, "ingested_at": iso(ingested)}


def history_before(latest):
    return {t: mw for t, mw in SERIES_2025.points if t < latest}


def test_a_recent_live_hour_produces_decisions_built_from_the_history():
    latest = est(2025, 6, 23, 14)
    points = history_before(latest)
    rows = handle(points, event(latest, SERIES_2025.demand_at(latest)), PARAMS, NOW)
    assert [r["variant"] for r in rows] == ["persist2", "persist4"] and latest in points


def test_backfilled_hours_only_add_to_the_history():
    latest = est(2025, 6, 23, 14)
    points = history_before(latest)
    assert handle(points, event(latest, 20000.0, reason="backfill"), PARAMS, NOW) == [] and points[latest] == 20000.0


def test_an_old_live_hour_is_not_announced_again_after_a_restart_replay():
    latest = est(2025, 6, 23, 14)
    assert handle(history_before(latest), event(latest, 20000.0, ingested=NOW - timedelta(hours=5)), PARAMS, NOW) == []
