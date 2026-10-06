import socket
from datetime import datetime, timedelta, timezone

from observability.statsd import Statsd
from producer.run import Metrics
from test_events import realtime_events
from test_consumer import FakeConsumer, run


class RecordingStatsd:
    """Stands in for the Datadog client in tests: remembers (kind, name, value, tags)."""

    def __init__(self):
        self.calls = []

    def incr(self, name, value=1, tags=()):
        self.calls.append(("c", name, value, list(tags)))

    def gauge(self, name, value, tags=()):
        self.calls.append(("g", name, value, list(tags)))

    def histogram(self, name, value, tags=()):
        self.calls.append(("h", name, value, list(tags)))

    def named(self, name):
        return [c for c in self.calls if c[1] == name]


# ---- the client ----

def test_the_client_sends_dogstatsd_packets_with_tags():
    server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server.bind(("127.0.0.1", 0))
    server.settimeout(2)
    client = Statsd(host="127.0.0.1", port=server.getsockname()[1], tags=["service:consumer"])
    client.incr("consumer.events_written", 500)
    client.gauge("consumer.data_age_s", 12.5, ["topic:ieso.demand.ici"])
    client.histogram("consumer.batch_seconds", 4.2)
    assert [server.recv(200).decode() for _ in range(3)] == [
        "grid.consumer.events_written:500|c|#service:consumer",
        "grid.consumer.data_age_s:12.5|g|#service:consumer,topic:ieso.demand.ici",
        "grid.consumer.batch_seconds:4.2|h|#service:consumer",
    ]


def test_with_no_agent_configured_it_does_nothing_and_never_fails(monkeypatch):
    monkeypatch.delenv("DD_AGENT_HOST", raising=False)
    client = Statsd()
    client.incr("x")
    client.gauge("x", 1)
    Statsd(host="").histogram("x", 1)


# ---- Metrics reports what the code already counted ----

def test_each_increase_of_a_counter_is_reported_as_the_difference():
    recording = RecordingStatsd()
    metrics = Metrics(recording, "producer")
    metrics.counts["files_failed"] += 1
    metrics.counts["files_failed"] += 2
    assert [c[2] for c in recording.named("producer.files_failed")] == [1, 2] and metrics.counts["files_failed"] == 3


def test_latency_is_kept_for_the_summary_and_reported_as_a_histogram():
    recording = RecordingStatsd()
    metrics = Metrics(recording, "consumer")
    metrics.observe_latency(630.0)
    assert metrics.latencies == [630.0] and recording.named("consumer.latency_s") == [("h", "consumer.latency_s", 630.0, [])]


def test_the_heartbeat_reports_how_old_the_newest_data_is_and_not_more_often_than_asked():
    recording = RecordingStatsd()
    metrics = Metrics(recording, "consumer")
    metrics.see_published(datetime.now(timezone.utc) - timedelta(minutes=95))
    metrics.heartbeat()
    metrics.heartbeat()  # too soon: nothing new
    age = recording.named("consumer.data_age_s")
    assert len(age) == 1 and 94 * 60 < age[0][2] < 96 * 60  # a freshness alert at 90 minutes would fire on this


def test_the_heartbeat_reports_the_lag_of_every_partition_the_consumer_owns():
    class Partition:
        def __init__(self, partition, offset=0):
            self.topic, self.partition, self.offset = "ieso.demand.ici", partition, offset

    class LaggingConsumer:
        def assignment(self):
            return [Partition(0), Partition(1)]

        def get_watermark_offsets(self, partition, cached):
            return 0, {0: 100, 1: 40}[partition.partition]

        def position(self, partitions):
            return [Partition(partitions[0].partition, offset={0: 70, 1: -1001}[partitions[0].partition])]  # -1001: no position yet

    recording = RecordingStatsd()
    Metrics(recording, "consumer").heartbeat(LaggingConsumer())
    lags = {c[3][1]: c[2] for c in recording.named("consumer.lag")}
    assert lags == {"partition:0": 30, "partition:1": 40}  # 100-70, and with no position yet the whole 40
    assert recording.named("consumer.lag_total")[0][2] == 70


def test_a_broken_consumer_never_stops_the_heartbeat_or_the_pipeline():
    class Broken:
        def assignment(self):
            raise RuntimeError("broker gone")

    Metrics(RecordingStatsd(), "consumer").heartbeat(Broken())


# ---- the consumer loop reports ----

def test_the_consumer_reports_events_written_batch_time_and_remembers_the_newest_data():
    recording = RecordingStatsd()
    metrics = Metrics(recording, "consumer")
    run(FakeConsumer(realtime_events()), lambda batch: None, batch_size=5, metrics=metrics)
    assert sum(c[2] for c in recording.named("consumer.events_written")) == 12
    assert len(recording.named("consumer.batch_seconds")) == 3
    assert metrics.newest_published == datetime(2026, 9, 28, 12, 54, 48, tzinfo=timezone.utc)
