import json
import os
import time
from datetime import datetime, timezone

from jsonschema import ValidationError

from producer.run import VALIDATOR, Metrics, say

TOPICS = ["ieso.demand.realtime", "ieso.demand.predispatch", "ieso.demand.ici"]


def decode(raw):
    """The event, or None if it is broken. One bad message must never block a topic."""
    try:
        event = json.loads(raw)
        VALIDATOR.validate(event)
        return event
    except (ValueError, ValidationError):
        return None


def _flush(batch, write, consumer, metrics):
    started = time.monotonic()
    write(batch)  # raises on failure, so the commit below never happens and the batch is read again
    consumer.commit(asynchronous=False)  # only after BigQuery has the data
    metrics.counts["batches"] += 1
    metrics.counts["events_written"] += len(batch)
    now = datetime.now(timezone.utc)
    for event in batch:
        if event["reason"] == "live":  # backfilled events are old by definition, so they say nothing about latency
            published = datetime.strptime(event["published_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            metrics.latencies.append((now - published).total_seconds())
    say("batch_written", events=len(batch), seconds=round(time.monotonic() - started, 1), **metrics.summary())  # running totals, with latency p50/p95 for live events


def run_loop(consumer, write, batch_size, flush_seconds, metrics, exit_when_idle=0):
    """Collect events, write them as a batch of batch_size or after flush_seconds, then commit.

    exit_when_idle > 0 stops after that many quiet seconds (used for replays).
    """
    batch, first_at, last_message_at = [], None, time.monotonic()
    while True:
        message = consumer.poll(1.0)
        now = time.monotonic()
        if message is not None:
            if message.error():
                raise RuntimeError(message.error())
            last_message_at = now
            event = decode(message.value())
            if event is None:
                metrics.counts["events_invalid"] += 1
            else:
                batch.append(event)
                first_at = first_at or now

        idle = exit_when_idle > 0 and message is None and now - last_message_at >= exit_when_idle
        if batch and (len(batch) >= batch_size or now - first_at >= flush_seconds or idle):
            _flush(batch, write, consumer, metrics)
            batch, first_at = [], None
        if idle:
            return


def main():
    from confluent_kafka import Consumer
    from google.cloud import bigquery

    from consumer.bq import write_batch

    client = bigquery.Client(project=os.environ["GCP_PROJECT"])
    dataset = os.environ.get("BQ_DATASET", "grid")
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": os.environ.get("GROUP_ID", "bq-sink"),
        "enable.auto.commit": False,  # we commit ourselves, after the write
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(TOPICS)

    metrics, started = Metrics(), time.monotonic()
    run_loop(consumer, lambda events: write_batch(client, dataset, events),
             int(os.environ.get("BATCH_SIZE", "500")), float(os.environ.get("FLUSH_SECONDS", "60")), metrics,
             float(os.environ.get("EXIT_WHEN_IDLE_SECONDS", "0")))
    say("consumer_done", seconds=round(time.monotonic() - started), **metrics.summary())


if __name__ == "__main__":
    main()
