import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date, datetime

from parsers.common import hour_ending_to_utc

NS = "{http://www.ieso.ca/schema}"


@dataclass
class PeakDay:
    rank: int
    interval_start: datetime  # UTC
    delivery_date: date
    hour_ending: int
    demand_mw: float
    status: str  # "Final" or "Initial"


def parse_ici_peak_tracker(xml_text: str) -> list[PeakDay]:
    """Return the top Ontario demand days, highest first (the file lists 10; ICI uses the top 5)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as err:
        raise ValueError(f"bad XML: {err}") from err

    datasets = [d for d in root.iter(NS + "dataset") if d.get("datapointName") == "TOP_ONTARIO_DEMAND"]
    if len(datasets) != 1:
        raise ValueError("expected exactly one TOP_ONTARIO_DEMAND dataset")

    peaks = []
    for point in datasets[0].findall(NS + "datapoint"):
        value = float(point.findtext(NS + "value"))
        status = point.findtext(NS + "status").strip()
        if value == 0 and status == "":
            continue  # the file ends with a zero-valued placeholder row
        day = date.fromisoformat(point.findtext(f"{NS}datetimeInfo/{NS}deliveryDate"))
        hour = int(point.findtext(f"{NS}datetimeInfo/{NS}deliveryHour"))
        peaks.append(PeakDay(len(peaks) + 1, hour_ending_to_utc(day, hour), day, hour, value, status))

    if not peaks:
        raise ValueError("no peak rows found")
    return peaks
