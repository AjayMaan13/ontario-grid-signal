import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from parsers.common import hour_ending_to_utc

NS = "{http://www.ieso.ca/schema}"


@dataclass
class RealtimeInterval:
    interval_start: datetime  # UTC, 5 minutes long
    delivery_date: date
    hour_ending: int
    interval: int  # 1..12 inside the hour
    total_load_mw: float
    ontario_demand_mw: float  # only in the XML, not the CSV


def parse_realtime_totals(xml_text: str) -> list[RealtimeInterval]:
    """Parse PUB_RealtimeTotals_<yyyymmddHH>[_vN].xml. Version N holds N intervals."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as err:
        raise ValueError(f"bad XML: {err}") from err

    day = date.fromisoformat(root.findtext(f".//{NS}DeliveryDate"))
    hour = int(root.findtext(f".//{NS}DeliveryHour"))
    hour_start = hour_ending_to_utc(day, hour)

    rows = []
    for block in root.iter(NS + "IntervalEnergy"):
        interval = int(block.findtext(NS + "Interval"))
        values = {mq.findtext(NS + "MarketQuantity"): float(mq.findtext(NS + "EnergyMW")) for mq in block.findall(NS + "MQ")}
        start = hour_start + timedelta(minutes=5 * (interval - 1))
        rows.append(RealtimeInterval(start, day, hour, interval, values["Total Load"], values["ONTARIO DEMAND"]))

    if not rows:
        raise ValueError("no intervals found")
    if [r.interval for r in rows] != list(range(1, len(rows) + 1)):
        raise ValueError("intervals are not 1..N in order")
    return rows
