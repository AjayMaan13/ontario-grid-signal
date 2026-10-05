import hashlib
import json
import re
from datetime import datetime, timezone

SCHEMA_VERSION = 1
ZONE = "ONTARIO"

# report -> (Kafka topic, what the number means, which parsed field holds it)
REPORTS = {
    "RealtimeTotals": ("ieso.demand.realtime", "ontario_demand", "ontario_demand_mw"),
    "PredispTotals": ("ieso.demand.predispatch", "total_load", "total_load_mw"),
    "ICIDemand": ("ieso.demand.ici", "ontario_demand", "demand_mw"),
}


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def version_from_filename(filename: str) -> int:
    """PUB_RealtimeTotals_2026092808_v12.xml -> 12.

    Unversioned base files are refused: their content keeps changing, so they cannot be
    told apart from their _vN twin.
    """
    match = re.search(r"_v(\d+)\.\w+$", filename)
    if not match:
        raise ValueError(f"not a versioned file: {filename}")
    return int(match.group(1))


def to_events(report, filename, rows, published_at, source_uri, reason, ingested_at=None):
    """One event per parsed row of one file."""
    _, measure, field = REPORTS[report]
    version = version_from_filename(filename)
    ingested_at = ingested_at or datetime.now(timezone.utc)

    points = [(iso(row.interval_start), getattr(row, field)) for row in rows]
    # Hash of the parsed values, not the raw bytes: a restated file can change only its header line.
    sha = hashlib.sha256(json.dumps(points).encode()).hexdigest()

    return [
        {
            "schema_version": SCHEMA_VERSION,
            "report": report,
            "measure": measure,
            "interval_start": start,
            "zone": ZONE,
            "version": version,
            "value_mw": value,
            "published_at": iso(published_at),
            "ingested_at": iso(ingested_at),
            "source_uri": source_uri,
            "content_sha256": sha,
            "reason": reason,
        }
        for start, value in points
    ]


def topic_for(event: dict) -> str:
    return REPORTS[event["report"]][0]


def key_for(event: dict) -> str:
    return f"{event['report']}|{event['zone']}"
