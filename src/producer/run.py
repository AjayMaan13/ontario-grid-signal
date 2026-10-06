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
from observability.statsd import Statsd
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


class _EmittingCounter(Counter):
    """A Counter that also reports every increase, so the code keeps writing `metrics.counts["x"] += 1`."""

    def __init__(self, emit):
        super().__init__()
        self._emit = emit

    def __setitem__(self, key, value):
        delta = value - self.get(key, 0)
        super().__setitem__(key, value)
        if delta > 0:
            self._emit(key, delta)


class Metrics:
    def __init__(self, statsd=None, prefix="app"):
        self.statsd, self.prefix = statsd or Statsd(host=""), prefix
        self.counts = _EmittingCounter(lambda name, delta: self.statsd.incr(f"{prefix}.{name}", delta))
        self.latencies = []  # seconds from IESO publishing a file to our event being delivered
        self.newest_published = None  # IESO's publish time of the newest file seen: the data's age is measured from it
        self._last_heartbeat = 0.0

    def observe_latency(self, seconds):
        self.latencies.append(seconds)
        self.statsd.histogram(f"{self.prefix}.latency_s", seconds)

    def see_published(self, when):
        self.newest_published = when if self.newest_published is None else max(self.newest_published, when)

    def heartbeat(self, consumer=None, every=15):
        """Gauges that must keep arriving even when nothing else happens: how old the newest data is, and the consumer's lag.

        If the data stops coming, the age keeps growing, which is what a freshness alert needs to see.
        """
        now = time.monotonic()
        if now - self._last_heartbeat < every:
            return
        self._last_heartbeat = now
        try:
            self.statsd.gauge(f"{self.prefix}.up", 1)  # one series per pod, so summing them counts the replicas
            if self.newest_published is not None:
                self.statsd.gauge(f"{self.prefix}.data_age_s", (datetime.now(timezone.utc) - self.newest_published).total_seconds())
            if consumer is not None and hasattr(consumer, "assignment"):
                total = 0
                for partition in consumer.assignment():
                    low, high = consumer.get_watermark_offsets(partition, cached=False)
                    position = consumer.position([partition])[0].offset
                    lag = max(0, high - position) if position >= 0 else high - low
                    total += lag
                    self.statsd.gauge(f"{self.prefix}.lag", lag, [f"topic:{partition.topic}", f"partition:{partition.partition}"])
                self.statsd.gauge(f"{self.prefix}.lag_total", total)
        except Exception:  # noqa: BLE001 - a metrics problem must never stop the pipeline
            pass

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
        for _ in fresh:
            metrics.observe_latency((datetime.now(timezone.utc) - file.published_at).total_seconds())
    return True


def run_once(source, checkpoint, checkpoint_uri, producer, reason, metrics):
    since_save = 0
    for report in PARSERS:
        files = sorted(source.list_files(report), key=sort_key)
        for file in files:
            metrics.see_published(file.published_at)
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

    statsd = Statsd(tags=["service:producer"])
    if mode == "backfill":
        from producer.sources import GCSSource

        metrics = Metrics(statsd, "producer")
        run_once(GCSSource(os.environ["RAW_BUCKET"]), checkpoint, uri, producer, "backfill", metrics)
        say("backfill_done", **metrics.summary())
        return

    from producer.sources import IESOSource

    source = IESOSource(int(os.environ.get("LOOKBACK_HOURS", "48")))
    newest = None
    while True:
        metrics = Metrics(statsd, "producer")
        run_once(source, checkpoint, uri, producer, "live", metrics)
        say("cycle_done", **metrics.summary())
        statsd.incr("producer.cycles")
        if metrics.newest_published:
            newest = max(newest or metrics.newest_published, metrics.newest_published)
        wait_until = time.monotonic() + int(os.environ.get("POLL_SECONDS", "600"))  # IESO's listing is 1.6 MB: be polite
        while time.monotonic() < wait_until:
            if newest:  # keeps arriving while we wait, so a stalled producer shows as a growing age
                statsd.gauge("producer.data_age_s", (datetime.now(timezone.utc) - newest).total_seconds())
            time.sleep(15)


if __name__ == "__main__":
    main()
