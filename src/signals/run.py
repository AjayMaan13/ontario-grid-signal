"""Live signal evaluator: follows ieso.demand.ici, runs the rule on every new hour, writes flags to Kafka and BigQuery.

It keeps no state of its own. It never commits offsets, so every start replays the topic from the beginning and rebuilds
the demand history from Kafka itself. Only recent live hours produce flags, so a replay never re-announces old ones.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

from consumer.run import decode
from producer.run import say
from signals.live import evaluate, load_params
from signals.series import Series

FORMAT = "%Y-%m-%dT%H:%M:%SZ"
RECENT = timedelta(hours=3)


def parse(text: str) -> datetime:
    return datetime.strptime(text, FORMAT).replace(tzinfo=timezone.utc)


def handle(points: dict, event: dict, params: dict, now: datetime) -> list[dict]:
    """Add an ICIDemand reading to the history. Return signal rows, but only for a recent live hour."""
    start = parse(event["interval_start"])
    points[start] = event["value_mw"]
    if event["reason"] != "live" or now - parse(event["ingested_at"]) > RECENT:
        return []
    return evaluate(Series(list(points.items())), start, params, now)


def main():
    from confluent_kafka import Consumer, Producer
    from google.cloud import bigquery

    from signals.sink import write_signals

    params = load_params()
    client = bigquery.Client(project=os.environ["GCP_PROJECT"])
    dataset = os.environ.get("BQ_DATASET", "grid")
    producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "enable.idempotence": True, "acks": "all"})
    consumer = Consumer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "group.id": "signal-eval", "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    consumer.subscribe(["ieso.demand.ici"])

    points, flagged = {}, 0
    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(message.error())
        event = decode(message.value())
        if event is None or event["report"] != "ICIDemand":
            continue
        rows = handle(points, event, params, datetime.now(timezone.utc))
        if not rows:
            continue
        write_signals(client, dataset, rows)
        for row in rows:
            producer.produce("grid.signals", key=row["variant"], value=json.dumps(row).encode())
        producer.flush(30)
        flagged += sum(row["risk"] for row in rows)
        say("signals_written", rows=len(rows), at_risk=sum(row["risk"] for row in rows), flagged_total=flagged, history_hours=len(points))


if __name__ == "__main__":
    main()
