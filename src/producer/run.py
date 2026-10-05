import json
import logging
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator

from parsers.ici_demand import parse_ici_demand
from parsers.predisp_totals import parse_predisp_totals_xml
from parsers.realtime_totals import parse_realtime_totals
from producer import store
from producer.events import key_for, to_events, topic_for
from producer.sources import sort_key

PARSERS = {
    "ICIDemand": parse_ici_demand,
    "PredispTotals": parse_predisp_totals_xml,
    "RealtimeTotals": parse_realtime_totals,
}
SCHEMA = json.loads((Path(__file__).resolve().parents[2] / "schemas" / "demand_event_v1.json").read_text())
VALIDATOR = Draft202012Validator(SCHEMA)
SAVE_EVERY = 20  # files between checkpoint saves; a crash re-sends at most this many files


def say(event, **fields):
    """One JSON line per log entry."""
    print(json.dumps({"time": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event, **fields}), flush=True)


class Metrics:
    def __init__(self):
        self.counts = Counter()
        self.latencies = []  # seconds from IESO publishing a file to our event being delivered

    def summary(self):
        out = dict(self.counts)
        if self.latencies:
            ordered = sorted(self.latencies)
            out["latency_p50_s"] = round(ordered[len(ordered) // 2])
            out["latency_p95_s"] = round(ordered[int(len(ordered) * 0.95)])
            out["latency_max_s"] = round(ordered[-1])
        return out


def send(producer, events):
    """Produce, wait for Kafka to confirm every event, or raise. Nothing is marked done before this returns."""
    errors = []
    for event in events:
        producer.produce(topic_for(event), key=key_for(event), value=json.dumps(event).encode(),
                         on_delivery=lambda err, msg: errors.append(err) if err else None)
    still_queued = producer.flush(30)
    if errors or still_queued:
        raise RuntimeError(f"Kafka delivery failed: {len(errors)} errors, {still_queued} undelivered")


def process_file(source, report, file, checkpoint, producer, reason, metrics):
    if checkpoint.is_file_done(report, file.name):
        metrics.counts["files_skipped"] += 1
        return False

    try:
        rows = PARSERS[report](source.read(report, file.name))
        events = to_events(report, file.name, rows, file.published_at, file.uri, reason)
    except ValueError as err:  # a bad file must not stop the others, but it must be counted and shown
        say("file_failed", report=report, file=file.name, error=str(err))
        metrics.counts["files_failed"] += 1
        return False
    metrics.counts["files_parsed"] += 1

    fresh = [e for e in events if checkpoint.is_new(e)]
    for event in fresh:
        VALIDATOR.validate(event)  # contract check: a bad event stops the producer before it reaches Kafka
    send(producer, fresh)

    for event in fresh:  # only now, after delivery, is it safe to remember them
        checkpoint.mark(event)
    checkpoint.mark_file_done(report, file.name)
    metrics.counts["events_produced"] += len(fresh)
    metrics.counts["events_deduplicated"] += len(events) - len(fresh)
    if reason == "live":
        metrics.latencies += [(datetime.now(timezone.utc) - file.published_at).total_seconds()] * len(fresh)
    return True


def run_once(source, checkpoint, checkpoint_uri, producer, reason, metrics):
    since_save = 0
    for report in PARSERS:
        files = sorted(source.list_files(report), key=sort_key)
        for file in files:
            metrics.counts["files_seen"] += 1
            if process_file(source, report, file, checkpoint, producer, reason, metrics):
                since_save += 1
                if since_save >= SAVE_EVERY:
                    store.save(checkpoint_uri, checkpoint)
                    since_save = 0
    store.save(checkpoint_uri, checkpoint)


def main():
    from confluent_kafka import Producer

    logging.basicConfig(level=logging.WARNING)
    mode = os.environ.get("MODE", "live")  # "live" reads IESO, "backfill" reads the archive bucket
    uri = os.environ["CHECKPOINT_URI"]
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "enable.idempotence": True, "acks": "all"})
    checkpoint = store.load(uri)

    if mode == "backfill":
        from producer.sources import GCSSource

        metrics = Metrics()
        run_once(GCSSource(os.environ["RAW_BUCKET"]), checkpoint, uri, producer, "backfill", metrics)
        say("backfill_done", **metrics.summary())
        return

    from producer.sources import IESOSource

    source = IESOSource(int(os.environ.get("LOOKBACK_HOURS", "48")))
    while True:
        metrics = Metrics()
        run_once(source, checkpoint, uri, producer, "live", metrics)
        say("cycle_done", **metrics.summary())
        time.sleep(int(os.environ.get("POLL_SECONDS", "600")))  # IESO's listing is 1.6 MB: be polite


if __name__ == "__main__":
    main()
