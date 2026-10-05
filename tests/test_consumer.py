import json

import pytest

from consumer.bq import COLUMNS, to_row
from consumer.run import run_loop
from producer.run import Metrics

from test_events import realtime_events


class Message:
    def __init__(self, raw):
        self.raw = raw

    def value(self):
        return self.raw

    def error(self):
        return None


class FakeConsumer:
    """Stands in for Kafka: hands out messages in order and remembers how far it was told to commit."""

    def __init__(self, events, committed=0, log=None):
        self.messages = [Message(e if isinstance(e, bytes) else json.dumps(e).encode()) for e in events]
        self.position = self.committed = committed
        self.log = log if log is not None else []

    def poll(self, timeout):
        if self.position >= len(self.messages):
            return None
        self.position += 1
        return self.messages[self.position - 1]

    def commit(self, asynchronous):
        self.committed = self.position
        self.log.append("commit")


def twelve_events():
    return realtime_events()  # one real hour: 12 five-minute readings


def run(consumer, write, batch_size=5, flush_seconds=60, metrics=None):
    metrics = metrics or Metrics()
    run_loop(consumer, write, batch_size, flush_seconds, metrics, exit_when_idle=0.01)
    return metrics


def test_events_are_written_in_batches_and_the_rest_when_the_topic_goes_quiet():
    written = []
    consumer = FakeConsumer(twelve_events())
    metrics = run(consumer, lambda batch: written.append(len(batch)))
    assert written == [5, 5, 2]
    assert consumer.committed == 12
    assert metrics.counts["events_written"] == 12


def test_offsets_are_committed_only_after_each_write():
    log = []
    run(FakeConsumer(twelve_events(), log=log), lambda batch: log.append(f"write {len(batch)}"))
    assert log == ["write 5", "commit", "write 5", "commit", "write 2", "commit"]


def test_a_failed_write_commits_nothing_and_a_restart_reads_the_same_events_again():
    events = twelve_events()
    crashed = FakeConsumer(events)

    def broken_write(batch):
        raise RuntimeError("BigQuery is down")

    with pytest.raises(RuntimeError):
        run(crashed, broken_write)
    assert crashed.committed == 0  # nothing was acknowledged

    table = {}  # an idempotent sink: writing the same event twice leaves one row
    restarted = FakeConsumer(events, committed=crashed.committed)
    run(restarted, lambda batch: table.update({(e["interval_start"], e["version"]): e for e in batch}))
    assert len(table) == 12  # same result as if the crash had never happened


def test_a_broken_message_is_counted_and_skipped_without_stopping_the_topic():
    written = []
    consumer = FakeConsumer(twelve_events()[:3] + [b"not json", json.dumps({"junk": 1}).encode()] + twelve_events()[3:5])
    metrics = run(consumer, lambda batch: written.extend(batch))
    assert len(written) == 5
    assert metrics.counts["events_invalid"] == 2


def test_live_events_record_how_long_after_publishing_they_reached_bigquery():
    live = [{**e, "reason": "live"} for e in twelve_events()[:3]]
    metrics = run(FakeConsumer(live), lambda batch: None)
    assert len(metrics.latencies) == 3


def test_backfilled_events_do_not_count_towards_latency():
    metrics = run(FakeConsumer(twelve_events()[:3]), lambda batch: None)
    assert metrics.latencies == []


def test_a_row_for_bigquery_has_exactly_the_table_columns():
    from datetime import datetime, timezone

    row = to_row(twelve_events()[0], datetime(2026, 9, 28, 13, tzinfo=timezone.utc))
    assert list(row) == COLUMNS
    assert row["loaded_at"] == "2026-09-28T13:00:00+00:00"
