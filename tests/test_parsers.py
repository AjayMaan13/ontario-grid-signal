from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from parsers.common import hour_ending_to_utc
from parsers.ici_demand import parse_ici_demand
from parsers.ici_peak_tracker import parse_ici_peak_tracker
from parsers.predisp_totals import parse_predisp_totals
from parsers.realtime_totals import parse_realtime_totals

FIXTURES = Path(__file__).parent / "fixtures"


def read(name: str) -> str:
    return (FIXTURES / name).read_text()


def utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


# ---- time conversion ----

def test_hour_ending_is_converted_to_the_start_of_the_hour_in_utc():
    # HE1 on 28 Sep = 00:00-01:00 EST = 05:00Z; HE24 ends at midnight EST.
    assert hour_ending_to_utc(date(2026, 9, 28), 1) == utc(2026, 9, 28, 5)
    assert hour_ending_to_utc(date(2026, 9, 28), 24) == utc(2026, 9, 29, 4)


def test_hour_ending_out_of_range_is_rejected():
    with pytest.raises(ValueError):
        hour_ending_to_utc(date(2026, 9, 28), 25)


# ---- ICIDemand ----

def test_ici_demand_full_year():
    rows = parse_ici_demand(read("PUB_ICIDemand_2022.csv"))
    assert len(rows) == 8760  # 365 days x 24 hours
    peak = next(r for r in rows if r.delivery_date == date(2022, 7, 19) and r.hour_ending == 18)
    assert peak.demand_mw == 22607
    assert peak.interval_start == utc(2022, 7, 19, 22)  # HE18 = 17:00-18:00 EST


@pytest.mark.parametrize("dst_day", [date(2022, 11, 6), date(2023, 3, 12)])
def test_ici_demand_has_24_hours_on_daylight_saving_change_days(dst_day):
    rows = parse_ici_demand(read("PUB_ICIDemand_2022.csv"))
    assert sum(r.delivery_date == dst_day for r in rows) == 24


def test_ici_demand_utc_hours_are_continuous_across_the_whole_year():
    starts = [r.interval_start for r in parse_ici_demand(read("PUB_ICIDemand_2022.csv"))]
    assert all(b - a == timedelta(hours=1) for a, b in zip(starts, starts[1:]))


def test_ici_demand_header_only_file_fails_loudly():
    header_only = "\n".join(read("PUB_ICIDemand_2022.csv").splitlines()[:4])
    with pytest.raises(ValueError, match="no data rows"):
        parse_ici_demand(header_only)


def test_ici_demand_truncated_row_fails_loudly():
    truncated = read("PUB_ICIDemand_2022.csv").rsplit(",", 1)[0]  # last row loses its MW value
    with pytest.raises(ValueError):
        parse_ici_demand(truncated)


# ---- ICIPeakTracker ----

def test_peak_tracker_2022():
    peaks = parse_ici_peak_tracker(read("PUB_ICIPeakTracker_2022.xml"))
    assert len(peaks) == 10  # zero-valued placeholder row is dropped
    top5 = [(p.delivery_date, p.hour_ending) for p in peaks[:5]]
    assert top5 == [
        (date(2022, 7, 19), 18),
        (date(2022, 6, 22), 17),
        (date(2022, 8, 29), 17),
        (date(2022, 7, 20), 16),
        (date(2022, 8, 7), 17),
    ]
    assert peaks[0].demand_mw == pytest.approx(22607.3666666667)
    assert all(p.status == "Final" for p in peaks)


def test_peak_tracker_truncated_file_fails_loudly():
    with pytest.raises(ValueError):
        parse_ici_peak_tracker(read("PUB_ICIPeakTracker_2022.xml")[:2000])


# ---- RealtimeTotals ----

def test_realtime_totals_hour():
    rows = parse_realtime_totals(read("PUB_RealtimeTotals_2026092808_v12.xml"))
    assert len(rows) == 12  # version 12 has all 12 five-minute intervals
    assert rows[0].interval_start == utc(2026, 9, 28, 12)  # HE8 starts 07:00 EST
    assert rows[1].interval_start - rows[0].interval_start == timedelta(minutes=5)
    assert rows[11].total_load_mw == 17574.9
    assert rows[11].ontario_demand_mw == 16244.1  # total load also counts exports


def test_realtime_totals_truncated_file_fails_loudly():
    with pytest.raises(ValueError):
        parse_realtime_totals(read("PUB_RealtimeTotals_2026092808_v12.xml")[:3000])


# ---- PredispTotals ----

def test_predisp_totals_day():
    rows = parse_predisp_totals(read("PUB_PredispTotals_20260927_v14.csv"))
    assert len(rows) == 24
    assert rows[0].interval_start == utc(2026, 9, 27, 5)
    assert rows[0].total_load_mw == 15583
    assert rows[18].total_load_mw == 18269  # HE19


def test_predisp_totals_missing_hours_fail_loudly():
    lines = read("PUB_PredispTotals_20260927_v14.csv").splitlines()
    with pytest.raises(ValueError):
        parse_predisp_totals("\n".join(lines[:-3]))


def test_predisp_totals_xml_matches_the_csv_of_the_same_version():
    from parsers.predisp_totals import parse_predisp_totals_xml

    from_xml = parse_predisp_totals_xml(read("PUB_PredispTotals_20260927_v14.xml"))
    from_csv = parse_predisp_totals(read("PUB_PredispTotals_20260927_v14.csv"))
    assert from_xml == from_csv


def test_predisp_totals_xml_truncated_file_fails_loudly():
    from parsers.predisp_totals import parse_predisp_totals_xml

    with pytest.raises(ValueError):
        parse_predisp_totals_xml(read("PUB_PredispTotals_20260927_v14.xml")[:3000])
