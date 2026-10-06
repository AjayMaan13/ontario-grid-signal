"""python -m reconcile.cli seed

Simulates the live producer having MISSED one complete hour of RealtimeTotals, for the end-to-end test:
it forgets that hour in the producer's checkpoint and deletes the hour's rows from BigQuery.
Run the DAG afterwards: it must find the hour again, send it, and leave BigQuery exactly as it was.
Stop the producer and consumer first, so nothing writes while this runs.
"""
import os
import re
import sys
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

from parsers.common import EST
from parsers.realtime_totals import parse_realtime_totals
from producer import store
from producer.events import to_events
from producer.sources import GCSSource
from reconcile.compare import group_of

CHECKPOINT_URI = os.environ.get("CHECKPOINT_URI", "gs://ontario-grid-signal-state/producer/checkpoint.json")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "ontario-grid-signal-raw")
PROJECT, DATASET = os.environ.get("GCP_PROJECT", "ontario-grid-signal"), os.environ.get("BQ_DATASET", "grid")


def pick_hour(checkpoint, hours_old=72):
    """The newest complete RealtimeTotals hour (every version processed) that is at least 3 days old."""
    groups = {}
    for entry in checkpoint.files:
        report, name = entry.split("/", 1)
        if report == "RealtimeTotals":
            groups.setdefault(group_of(name), []).append(name)
    stamp = lambda group: re.search(r"_(\d{10})\.", group).group(1)  # yyyymmddHH, in EST like IESO's file names
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours_old)).astimezone(EST).strftime("%Y%m%d%H")
    chosen = max((g for g in groups if len(groups[g]) >= 12 and stamp(g) <= cutoff), key=stamp)
    return chosen, sorted(groups[chosen])


def seed():
    checkpoint = store.load(CHECKPOINT_URI)
    group, names = pick_hour(checkpoint)
    source = GCSSource(RAW_BUCKET)
    listing = {f.name: f for f in source.list_files("RealtimeTotals")}  # list the bucket once, not once per file
    starts = set()
    for name in names:
        file = listing[name]
        for event in to_events("RealtimeTotals", name, parse_realtime_totals(source.read("RealtimeTotals", name)), file.published_at, file.uri, "backfill"):
            starts.add(event["interval_start"])
        checkpoint.files.discard(f"RealtimeTotals/{name}")
    for start in starts:
        checkpoint.seen.pop(f"RealtimeTotals|ONTARIO|{start}", None)
    store.save(CHECKPOINT_URI, checkpoint)

    client = bigquery.Client(project=PROJECT)
    in_list = ", ".join(f"TIMESTAMP '{s}'" for s in sorted(starts))
    for table in ("raw_demand_versions", "current_demand"):
        client.query(f"DELETE FROM `{PROJECT}.{DATASET}.{table}` WHERE report = 'RealtimeTotals' AND interval_start IN ({in_list})").result()
    print(f"forgot hour {group}: {len(names)} files, {len(starts)} intervals ({min(starts)} .. {max(starts)})")


if __name__ == "__main__":
    if sys.argv[1:] == ["seed"]:
        seed()
    else:
        sys.exit("usage: python -m reconcile.cli seed")
