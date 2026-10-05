import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime

from parsers.common import hour_ending_to_utc

NS = "{http://www.ieso.ca/schema}"


@dataclass
class PredispHour:
    interval_start: datetime  # UTC
    delivery_date: date
    hour_ending: int
    total_load_mw: float  # forecast; includes exports, so it is NOT Ontario demand


def parse_predisp_totals(text: str) -> list[PredispHour]:
    """Parse PUB_PredispTotals_<yyyymmdd>[_vN].csv: 4 preamble lines, a header, 24 hourly rows."""
    lines = text.splitlines()
    if len(lines) < 5 or not lines[4].startswith("HOUR,INTERVAL,TOTAL ENERGY"):
        raise ValueError("unexpected header, not a PredispTotals file")

    # line 4 looks like: \\CREATED AT 2026/09/27 09:08:15 FOR 2026/09/27
    year, month, day = lines[3].rsplit(" FOR ", 1)[1].split("/")
    delivery_date = date(int(year), int(month), int(day))

    columns = lines[4].split(",")
    load_col = columns.index("TOTAL LOAD")
    rows = []
    for number, line in enumerate(lines[5:], start=6):
        parts = line.split(",")
        if len(parts) != len(columns):
            raise ValueError(f"line {number}: expected {len(columns)} columns, got {line!r}")
        hour = int(parts[0])
        rows.append(PredispHour(hour_ending_to_utc(delivery_date, hour), delivery_date, hour, float(parts[load_col])))

    if [r.hour_ending for r in rows] != list(range(1, 25)):
        raise ValueError("expected hours 1..24 in order")
    return rows


def parse_predisp_totals_xml(xml_text: str) -> list[PredispHour]:
    """Same data as the CSV, from PUB_PredispTotals_<yyyymmdd>[_vN].xml (the archiver keeps the XML)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as err:
        raise ValueError(f"bad XML: {err}") from err

    day = date.fromisoformat(root.findtext(f".//{NS}DeliveryDate"))
    rows = []
    for block in root.iter(NS + "HourlyConstrainedEnergy"):
        hour = int(block.findtext(NS + "DeliveryHour"))
        values = {mq.findtext(NS + "MarketQuantity"): float(mq.findtext(NS + "EnergyMW")) for mq in block.findall(NS + "MQ")}
        rows.append(PredispHour(hour_ending_to_utc(day, hour), day, hour, values["Total Load"]))

    if [r.hour_ending for r in rows] != list(range(1, 25)):
        raise ValueError("expected hours 1..24 in order")
    return rows
