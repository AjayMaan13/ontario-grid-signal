"""Runs the real producer against a real Kafka in a throwaway Docker container.

Skipped automatically when Docker is not running.
"""
import json
import time

import pytest

pytest.importorskip("confluent_kafka")
pytest.importorskip("testcontainers.kafka")

from confluent_kafka import Consumer, Producer  # noqa: E402
from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402
from testcontainers.kafka import KafkaContainer  # noqa: E402

from producer import store  # noqa: E402
from producer.run import VALIDATOR, Metrics, run_once  # noqa: E402
from producer.sources import DirectorySource  # noqa: E402
from test_producer import make_archive  # noqa: E402

EXPECTED = {"ieso.demand.realtime": 12, "ieso.demand.predispatch": 24, "ieso.demand.ici": 8760}


def docker_running():
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not docker_running(), reason="Docker is not running")


@pytest.fixture(scope="module")
def bootstrap():
    with KafkaContainer() as kafka:
        servers = kafka.get_bootstrap_server()
        topics = [NewTopic(name, num_partitions=6, replication_factor=1) for name in EXPECTED]  # same as production
        admin = AdminClient({"bootstrap.servers": servers})  # keep a reference, or it is destroyed mid-request
        for future in admin.create_topics(topics).values():
            future.result()
        yield servers


def read_all(servers, topic, expected):
    consumer = Consumer({"bootstrap.servers": servers, "group.id": f"test-{time.time_ns()}", "auto.offset.reset": "earliest"})
    consumer.subscribe([topic])
    messages, deadline = [], time.time() + 60
    while len(messages) < expected and time.time() < deadline:
        message = consumer.poll(1.0)
        if message and not message.error():
            messages.append(message)
    for _ in range(3):  # keep listening briefly, to catch any extra messages
        message = consumer.poll(1.0)
        if message and not message.error():
            messages.append(message)
    consumer.close()
    return messages


def test_producer_sends_keyed_valid_events_and_a_second_run_adds_nothing(bootstrap, tmp_path):
    archive, checkpoint = make_archive(tmp_path), str(tmp_path / "cp.json")

    def run_producer():  # every call is a fresh "process": new Kafka producer, checkpoint read from disk
        producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
        run_once(DirectorySource(archive), store.load(checkpoint), checkpoint, producer, "backfill", Metrics())

    run_producer()
    for topic, count in EXPECTED.items():
        messages = read_all(bootstrap, topic, count)
        assert len(messages) == count
        assert len({m.key() for m in messages}) == 1  # one series, one key
        assert len({m.partition() for m in messages}) == 1  # the same key always lands in the same partition
        for message in messages:
            VALIDATOR.validate(json.loads(message.value()))  # payload matches the schema

    run_producer()  # restart
    for topic, count in EXPECTED.items():
        assert len(read_all(bootstrap, topic, count)) == count  # not one extra message
