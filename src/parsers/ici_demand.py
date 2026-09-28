from dataclasses import dataclass
from datetime import date, datetime

from parsers.common import hour_ending_to_utc


@dataclass
class IciDemand:
    interval_start: datetime  # UTC
    delivery_date: date  # original EST date
    hour_ending: int  # original HE
    demand_mw: float


def parse_ici_demand(text: str) -> list[IciDemand]:
    """Parse PUB_ICIDemand_<year>.csv: 3 comment lines, a header, then Date,Hour,MW rows."""
    lines = text.splitlines()
    if len(lines) < 4 or lines[3] != "Date,Hour,Ontario Demand":
        raise ValueError("unexpected header, not an ICIDemand file")

    rows = []
    for number, line in enumerate(lines[4:], start=5):
        parts = line.split(",")
        if len(parts) != 3:
            raise ValueError(f"line {number}: expected 3 columns, got {line!r}")
        try:
            day = date.fromisoformat(parts[0])
            hour = int(parts[1])
            mw = float(parts[2])
            start = hour_ending_to_utc(day, hour)
        except ValueError as err:
            raise ValueError(f"line {number}: bad row {line!r}") from err
        rows.append(IciDemand(start, day, hour, mw))

    # ICIDemand_2021.csv is header-only. Fail loudly instead of returning [].
    if not rows:
        raise ValueError("file has a header but no data rows")
    return rows
