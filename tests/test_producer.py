import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from producer import store
from producer.checkpoint import Checkpoint
from producer.run import Metrics, run_once
from producer.sources import DirectorySource, SourceFile, is_wanted, parse_listing, sort_key

FIXTURES = Path(__file__).parent / "fixtures"


class FakeProducer:
    """Stands in for Kafka: remembers what was sent, and can be told to fail delivery."""

    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail

    def produce(self, topic, key, value, on_delivery):
        self.sent.append((topic, key, json.loads(value)))

    def flush(self, timeout):
        return 1 if self.fail else 0  # number of events still undelivered


def make_archive(tmp_path):
    """A folder laid out like the raw bucket, holding one real file per report."""
    for report, fixture, name in [
        ("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v12.xml", "PUB_RealtimeTotals_2026092808_v12.xml"),
        ("PredispTotals", "PUB_PredispTotals_20260927_v14.xml", "PUB_PredispTotals_20260927_v14.xml"),
        ("ICIDemand", "PUB_ICIDemand_2022.csv", "PUB_ICIDemand_2022_v8647.csv"),
    ]:
        (tmp_path / "archive" / report).mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURES / fixture, tmp_path / "archive" / report / name)
    return tmp_path / "archive"


def run(archive, checkpoint_path, producer, reason="backfill"):
    metrics = Metrics()
    uri = str(checkpoint_path)
    run_once(DirectorySource(archive), store.load(uri), uri, producer, reason, metrics)
    return metrics.counts


# ---- the producer loop ----

def test_first_run_sends_every_interval_to_the_right_topic(tmp_path):
    producer = FakeProducer()
    counts = run(make_archive(tmp_path), tmp_path / "cp.json", producer)
    by_topic = {}
    for topic, key, event in producer.sent:
        by_topic[topic] = by_topic.get(topic, 0) + 1
    assert by_topic == {"ieso.demand.realtime": 12, "ieso.demand.predispatch": 24, "ieso.demand.ici": 8760}
    assert counts["events_produced"] == 8796
    assert counts["files_failed"] == 0


def test_running_twice_sends_nothing_the_second_time(tmp_path):
    archive = make_archive(tmp_path)
    run(archive, tmp_path / "cp.json", FakeProducer())
    second = FakeProducer()
    counts = run(archive, tmp_path / "cp.json", second)  # a restart: new process, same checkpoint file
    assert second.sent == []
    assert counts["files_skipped"] == 3


def test_a_newer_version_with_the_same_values_sends_nothing(tmp_path):
    archive = make_archive(tmp_path)
    run(archive, tmp_path / "cp.json", FakeProducer())
    shutil.copy(archive / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v12.xml", archive / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v13.xml")
    producer = FakeProducer()
    counts = run(archive, tmp_path / "cp.json", producer)
    assert producer.sent == []
    assert counts["events_deduplicated"] == 12


def test_a_restated_value_is_sent_as_one_new_event(tmp_path):
    archive = make_archive(tmp_path)
    run(archive, tmp_path / "cp.json", FakeProducer())
    text = (archive / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v12.xml").read_text().replace("16087.9", "16000.0")
    (archive / "RealtimeTotals/PUB_RealtimeTotals_2026092808_v13.xml").write_text(text)
    producer = FakeProducer()
    run(archive, tmp_path / "cp.json", producer)
    assert [(e["version"], e["value_mw"]) for _, _, e in producer.sent] == [(13, 16000.0)]


def test_a_bad_file_is_counted_and_does_not_stop_the_others(tmp_path):
    archive = make_archive(tmp_path)
    (archive / "RealtimeTotals/PUB_RealtimeTotals_2026092809_v1.xml").write_text("<broken")
    producer = FakeProducer()
    counts = run(archive, tmp_path / "cp.json", producer)
    assert counts["files_failed"] == 1
    assert len(producer.sent) == 8796  # everything else still went out


def test_if_kafka_does_not_confirm_nothing_is_remembered_and_nothing_is_lost(tmp_path):
    archive = make_archive(tmp_path)
    with pytest.raises(RuntimeError, match="delivery failed"):
        run(archive, tmp_path / "cp.json", FakeProducer(fail=True))
    assert not (tmp_path / "cp.json").exists()  # a crash before delivery left no checkpoint behind
    retry = FakeProducer()
    run(archive, tmp_path / "cp.json", retry)  # the restart sends everything: at-least-once, no loss
    assert len(retry.sent) == 8796


def test_events_are_sent_with_the_series_and_day_key_so_versions_of_an_interval_stay_in_order(tmp_path):
    producer = FakeProducer()
    run(make_archive(tmp_path), tmp_path / "cp.json", producer)
    assert {key for topic, key, _ in producer.sent if topic == "ieso.demand.realtime"} == {"RealtimeTotals|ONTARIO|2026-09-28"}
    assert len({key for topic, key, _ in producer.sent if topic == "ieso.demand.ici"}) == 365  # one key per day of the 2022 base period


def test_live_runs_record_how_long_after_publishing_each_event_left(tmp_path):
    metrics = Metrics()
    uri = str(tmp_path / "cp.json")
    run_once(DirectorySource(make_archive(tmp_path)), store.load(uri), uri, FakeProducer(), "live", metrics)
    assert "latency_p95_s" in metrics.summary()


def test_checkpoint_file_round_trips(tmp_path):
    checkpoint = Checkpoint()
    checkpoint.mark_file_done("ICIDemand", "PUB_ICIDemand_2022_v8647.csv")
    store.save(str(tmp_path / "cp.json"), checkpoint)
    assert store.load(str(tmp_path / "cp.json")).is_file_done("ICIDemand", "PUB_ICIDemand_2022_v8647.csv")


# ---- sources ----

def test_listing_gives_file_names_with_publish_times_converted_from_est_to_utc():
    files = dict(parse_listing((FIXTURES / "ICIPeakTracker_listing.html").read_text(encoding="latin-1")))
    assert files["PUB_ICIPeakTracker_2022_v8501.xml"] == datetime(2024, 10, 23, 18, 4, tzinfo=timezone.utc)  # 13:04 EST


def test_only_versioned_files_of_the_right_format_are_wanted():
    assert is_wanted("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v12.xml")
    assert not is_wanted("RealtimeTotals", "PUB_RealtimeTotals_2026092808_v12.csv")  # CSV has no Ontario demand
    assert not is_wanted("RealtimeTotals", "PUB_RealtimeTotals.xml")  # base file: content keeps changing


def test_versions_sort_numerically_not_alphabetically():
    now = datetime.now(timezone.utc)
    names = ["PUB_ICIDemand_2026_v10.csv", "PUB_ICIDemand_2026_v9.csv", "PUB_ICIDemand_2025_v8260.csv"]
    ordered = sorted((SourceFile(n, now, n) for n in names), key=sort_key)
    assert [f.name for f in ordered] == ["PUB_ICIDemand_2025_v8260.csv", "PUB_ICIDemand_2026_v9.csv", "PUB_ICIDemand_2026_v10.csv"]
