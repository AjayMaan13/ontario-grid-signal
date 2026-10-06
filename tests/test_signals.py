import json
import random
from datetime import date, datetime, timedelta, timezone

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from signals import backtest, groundtruth
from signals.rule import HOUR, LAG, VARIANTS, Params, Window, base_start, decide, decision_time, hour_ending, in_window
from signals.series import Series, est_day

START = datetime(2026, 5, 1, 5, tzinfo=timezone.utc)  # 1 May, 00:00 EST
ALL_YEAR = Window(frozenset(range(1, 13)), 1, 24)
HE17_MAY = Window(frozenset({5}), 17, 17)


def series_with_peaks(peaks, base=15000.0):
    """One day per entry; the day's only high hour is HE17 (16:00-17:00 EST)."""
    return Series([(START + timedelta(days=d) + h * HOUR, peak if h == 16 else base) for d, peak in enumerate(peaks) for h in range(24)])


def he17(day):
    return START + timedelta(days=day, hours=16)


# ---- the rule, branch by branch ----

PEAKS = [20000, 21000, 22000, 23000, 24000, 19000, 18000, 17000, 16000, 24500]  # 5th-highest of these is 21,000


def test_flags_an_hour_when_the_estimate_reaches_the_bar():
    flag = decide(series_with_peaks(PEAKS + [0]), he17(10), "day_ahead", Params(1.0, HE17_MAY))
    assert flag.risk and flag.estimate == 24500 and flag.bar == 21000
    assert "reaches" in flag.reason


def test_does_not_flag_when_the_estimate_is_below_the_bar():
    flag = decide(series_with_peaks(PEAKS[:-1] + [15000, 0]), he17(10), "day_ahead", Params(1.0, HE17_MAY))
    assert not flag.risk and "below" in flag.reason


def test_a_margin_below_one_flags_estimates_that_come_close():
    peaks = PEAKS[:-1] + [19900, 0]  # yesterday: 19,900 MW against a 20,000 MW bar (99.5%)
    assert not decide(series_with_peaks(peaks), he17(10), "day_ahead", Params(1.0, HE17_MAY)).risk
    assert decide(series_with_peaks(peaks), he17(10), "day_ahead", Params(0.99, HE17_MAY)).risk


def test_nothing_is_flagged_during_warm_up():
    flag = decide(series_with_peaks([24000, 23000, 22000, 0]), he17(3), "day_ahead", Params(1.0, HE17_MAY))
    assert not flag.risk and "warm-up" in flag.reason


def test_nothing_is_flagged_outside_the_window():
    other_hour = he17(10) + HOUR  # HE18
    flag = decide(series_with_peaks(PEAKS + [0]), other_hour, "day_ahead", Params(1.0, HE17_MAY))
    assert not flag.risk and "window" in flag.reason


def test_no_flag_and_a_clear_reason_when_there_is_no_data_to_estimate_from():
    gappy = Series([p for p in series_with_peaks(PEAKS + [0]).points if est_day(p[0]) != date(2026, 5, 10)])  # yesterday is missing
    flag = decide(gappy, he17(10), "day_ahead", Params(1.0, HE17_MAY))
    assert not flag.risk and "no demand data" in flag.reason


def test_the_base_period_runs_from_1_may_to_30_april():
    assert base_start(date(2026, 4, 30)) == date(2025, 5, 1)
    assert base_start(date(2026, 5, 1)) == date(2026, 5, 1)
    assert base_start(date(2026, 12, 31)) == date(2026, 5, 1)


def test_hour_ending_is_iesos_convention():
    assert hour_ending(START) == 1  # 00:00-01:00 EST is HE1
    assert hour_ending(he17(0)) == 17
    assert in_window(he17(0), HE17_MAY) and not in_window(he17(0) + HOUR, HE17_MAY)


def test_decision_times_follow_what_is_published_when():
    start = he17(10)  # hour starts 16:00 EST
    assert decision_time("oracle", start) == start
    assert decision_time("persist2", start) == start - HOUR + LAG  # 15:15 EST: the hour ending 15:00 has just been published
    assert decision_time("persist4", start) == start - 3 * HOUR + LAG
    assert decision_time("day_ahead", start) == START + timedelta(days=10) + LAG  # 00:15 EST that day


def test_the_bar_ignores_a_day_that_is_not_over_yet():
    series = series_with_peaks(PEAKS + [99999])  # day 10 has a huge peak, but it has not ended when day 10 is decided
    assert decide(series, he17(10), "day_ahead", Params(1.0, HE17_MAY)).bar == 21000


# ---- no look-ahead: the property that matters ----

def make_series(values):
    return Series([(START + i * HOUR, v) for i, v in enumerate(values)])


@settings(max_examples=60, deadline=None)
@given(seed=st.integers(0, 10**9), days=st.integers(12, 40), where=st.floats(0.3, 0.999), variant=st.sampled_from(["persist2", "persist4", "day_ahead"]))
def test_a_decision_never_changes_when_data_published_after_it_changes(seed, days, where, variant):
    rng = random.Random(seed)
    values = [rng.uniform(10000, 26000) for _ in range(24 * days)]
    other = [rng.uniform(10000, 26000) for _ in range(24 * days)]
    index = max(24 * 8, int(where * len(values)) - 1)
    start = START + index * HOUR
    when = decision_time(variant, start)
    params = Params(0.95, ALL_YEAR)
    original = make_series(values)
    # rewrite every hour that was NOT yet public at the decision time
    rewritten = Series([(t, mw if t + HOUR + LAG <= when else other[i]) for i, (t, mw) in enumerate(original.points)])
    assert decide(original, start, variant, params) == decide(rewritten, start, variant, params)


def test_that_property_would_catch_a_rule_that_peeks():
    """A bar computed from ALL days (the classic mistake) changes with future data; ours does not."""
    series = series_with_peaks(PEAKS + [0, 0, 0])
    future_changed = series_with_peaks(PEAKS + [0, 90000, 90000])
    when = decision_time("day_ahead", he17(10))
    forever = datetime(2100, 1, 1, tzinfo=timezone.utc)
    assert series.fifth_highest(when - LAG, date(2026, 5, 1)) == future_changed.fifth_highest(when - LAG, date(2026, 5, 1))
    assert series.fifth_highest(forever, date(2026, 5, 1)) != future_changed.fifth_highest(forever, date(2026, 5, 1))


def test_the_oracle_is_openly_not_causal():
    """The ceiling variant uses the hour's own demand, so it must change when that hour changes. It is never used live."""
    base = series_with_peaks(PEAKS + [24900])
    changed = series_with_peaks(PEAKS + [10000])
    assert decide(base, he17(10), "oracle", Params(1.0, HE17_MAY)).risk
    assert not decide(changed, he17(10), "oracle", Params(1.0, HE17_MAY)).risk


# ---- ground truth ----

@pytest.mark.parametrize("year", [2022, 2023, 2025])
def test_the_derived_top_five_matches_iesos_own_peak_tracker(year):
    assert groundtruth.derive_peaks(groundtruth.load_series(year), year) == groundtruth.tracker_peaks(year)


def test_2024_has_no_usable_tracker_so_its_peaks_are_derived_and_labelled_so():
    assert groundtruth.SOURCE[2024] == "derived from ICIDemand"
    days = [(est_day(p), hour_ending(p)) for p in groundtruth.derive_peaks(groundtruth.load_series(2024), 2024)]
    assert days == [(date(2024, 6, 18), 16), (date(2024, 6, 19), 17), (date(2024, 7, 31), 17), (date(2024, 8, 1), 17), (date(2024, 8, 27), 17)]


# ---- the backtest ----

def test_the_backtest_result_has_not_changed_silently():
    """backtest/results.json is the committed result. If a code change alters it, this fails: re-run `make backtest`,
    read the difference, and commit the new file on purpose."""
    expected = json.loads(backtest.RESULTS.read_text())
    assert json.loads(json.dumps(backtest.run_all(), sort_keys=True)) == expected


def test_tuning_never_looks_at_the_held_out_years(monkeypatch):
    """Change the 2024-25 data completely: the window and every margin must stay exactly the same."""
    baseline = backtest.run_all()
    real_load = groundtruth.load_series

    def scrambled(year):
        series = real_load(year)
        return Series([(t, mw * 0.5) for t, mw in series.points]) if year in backtest.HOLDOUT else series

    monkeypatch.setattr(backtest, "load_series", scrambled)
    monkeypatch.setattr(backtest, "peaks", lambda year, series: groundtruth.peaks(year, real_load(year)))
    changed = backtest.run_all()
    assert changed["window"] == baseline["window"]
    assert {v: d["margin"] for v, d in changed["variants"].items()} == {v: d["margin"] for v, d in baseline["variants"].items()}
