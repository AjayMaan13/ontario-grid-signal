import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate

from parsers.ici_demand import parse_ici_demand
from parsers.predisp_totals import parse_predisp_totals_xml
from parsers.realtime_totals import parse_realtime_totals
from producer.checkpoint import Checkpoint
from producer.events import SCHEMA_VERSION, key_for, to_events, topic_for

ROOT = Path(__file__).parent.parent
FIXTURES = ROOT / "tests" / "fixtures"
SCHEMA = json.loads((ROOT / "schemas" / "demand_event_v1.json").read_text())
PUBLISHED = datetime(2026, 9, 28, 12, 54, 48, tzinfo=timezone.utc)
INGESTED = datetime(2026, 9, 28, 12, 56, tzinfo=timezone.utc)


def realtime_events(filename="PUB_RealtimeTotals_2026092808_v12.xml"):
    rows = parse_realtime_totals((FIXTURES / "PUB_RealtimeTotals_2026092808_v12.xml").read_text())
    return to_events("RealtimeTotals", filename, rows, PUBLISHED, "gs://raw/" + filename, "backfill", INGESTED)


def predisp_events():
    rows = parse_predisp_totals_xml((FIXTURES / "PUB_PredispTotals_20260927_v14.xml").read_text())
    return to_events("PredispTotals", "PUB_PredispTotals_20260927_v14.xml", rows, PUBLISHED, "gs://raw/x", "live", INGESTED)


def ici_events():
    rows = parse_ici_demand((FIXTURES / "PUB_ICIDemand_2022.csv").read_text())
    return to_events("ICIDemand", "PUB_ICIDemand_2022_v8647.csv", rows, PUBLISHED, "gs://raw/x", "backfill", INGESTED)


# ---- event builder ----

def test_realtime_file_becomes_one_event_per_interval():
    events = realtime_events()
    assert len(events) == 12
    assert events[0]["interval_start"] == "2026-09-28T12:00:00Z"
    assert events[0]["value_mw"] == 16087.9  # Ontario demand, not total load
    assert events[0]["version"] == 12
    assert events[0]["published_at"] == "2026-09-28T12:54:48Z"
    assert topic_for(events[0]) == "ieso.demand.realtime"
    assert key_for(events[0]) == "RealtimeTotals|ONTARIO"


def test_predispatch_events_say_they_are_total_load_not_demand():
    events = predisp_events()
    assert len(events) == 24
    assert events[0]["measure"] == "total_load"
    assert topic_for(events[0]) == "ieso.demand.predispatch"


def test_ici_file_becomes_one_event_per_hour():
    assert len(ici_events()) == 8760


def test_unversioned_file_is_refused():
    with pytest.raises(ValueError, match="not a versioned file"):
        realtime_events("PUB_RealtimeTotals.xml")


def test_hash_ignores_ingestion_time_so_it_is_the_same_on_every_read():
    assert realtime_events()[0]["content_sha256"] == realtime_events()[5]["content_sha256"]


# ---- contract: every event matches the versioned JSON Schema ----

@pytest.mark.parametrize("make_events", [realtime_events, predisp_events, ici_events])
def test_every_event_matches_the_schema(make_events):
    for event in make_events():
        validate(event, SCHEMA)


def test_schema_file_and_code_agree_on_the_version():
    assert SCHEMA["properties"]["schema_version"]["const"] == SCHEMA_VERSION


def test_schema_rejects_a_bad_event():
    event = realtime_events()[0]
    event["value_mw"] = "16087.9"  # a string instead of a number
    with pytest.raises(ValidationError):
        validate(event, SCHEMA)


# ---- checkpoint ----

def send(checkpoint, events):
    """What the producer does: keep the new ones, then mark them as delivered."""
    new = [e for e in events if checkpoint.is_new(e)]
    for e in new:
        checkpoint.mark(e)
    return new


def with_value(event, value, version):
    return {**event, "value_mw": value, "version": version}


def test_running_the_same_file_twice_sends_nothing_the_second_time():
    checkpoint = Checkpoint()
    assert len(send(checkpoint, realtime_events())) == 12
    assert send(checkpoint, realtime_events()) == []


def test_a_newer_version_with_the_same_value_is_not_resent():
    checkpoint = Checkpoint()
    first = realtime_events()[0]
    send(checkpoint, [first])
    assert send(checkpoint, [with_value(first, first["value_mw"], 13)]) == []


def test_a_newer_version_with_a_changed_value_is_sent():
    checkpoint = Checkpoint()
    first = realtime_events()[0]
    send(checkpoint, [first])
    assert len(send(checkpoint, [with_value(first, 16000.0, 13)])) == 1


def test_same_version_but_different_value_is_sent_and_flagged(caplog):
    checkpoint = Checkpoint()
    first = realtime_events()[0]
    send(checkpoint, [first])
    with caplog.at_level(logging.WARNING):
        assert len(send(checkpoint, [with_value(first, 16000.0, 12)])) == 1
    assert "same version, different value" in caplog.text


def test_an_old_version_arriving_late_does_not_overwrite_a_newer_one():
    checkpoint = Checkpoint()
    first = realtime_events()[0]
    send(checkpoint, [with_value(first, 16000.0, 13)])
    send(checkpoint, [with_value(first, 15000.0, 11)])  # late, older
    assert checkpoint.seen[Checkpoint._id(first)] == [13, 16000.0]


def test_an_event_that_was_never_marked_is_sent_again_after_a_crash():
    checkpoint = Checkpoint()
    events = realtime_events()
    assert len([e for e in events if checkpoint.is_new(e)]) == 12  # produced, but crashed before mark()
    assert len([e for e in events if checkpoint.is_new(e)]) == 12  # restart: sent again, not lost


def test_checkpoint_survives_being_saved_and_loaded():
    checkpoint = Checkpoint()
    send(checkpoint, realtime_events())
    restored = Checkpoint.from_json(checkpoint.to_json())
    assert send(restored, realtime_events()) == []
