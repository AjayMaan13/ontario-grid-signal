"""Runs the real MERGE statements and quality checks on real BigQuery, in a throwaway dataset.

Opt in with: make bq-test   (needs `gcloud auth application-default login`)
"""
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("google.cloud.bigquery")
from google.cloud import bigquery  # noqa: E402

from consumer import quality  # noqa: E402
from consumer.bq import SCHEMA, write_batch  # noqa: E402

pytestmark = pytest.mark.skipif(not os.environ.get("RUN_BQ_TESTS"), reason="set RUN_BQ_TESTS=1 (make bq-test) to run against real BigQuery")
PROJECT, LOCATION = "ontario-grid-signal", "northamerica-northeast2"
TABLES = ("raw_demand_versions", "current_demand")


@pytest.fixture(scope="module")
def client():
    return bigquery.Client(project=PROJECT)


@pytest.fixture(scope="module")
def dataset(client):
    name = f"test_{uuid.uuid4().hex[:12]}"
    new = bigquery.Dataset(f"{PROJECT}.{name}")
    new.location = LOCATION
    client.create_dataset(new)
    for table in TABLES:  # same partitioning and clustering as infra/main/bigquery.tf
        t = bigquery.Table(f"{PROJECT}.{name}.{table}", schema=SCHEMA)
        t.time_partitioning = bigquery.TimePartitioning(field="interval_start")
        t.clustering_fields = ["report", "zone"]
        client.create_table(t)
    yield name
    client.delete_dataset(f"{PROJECT}.{name}", delete_contents=True, not_found_ok=True)


@pytest.fixture(autouse=True)
def empty_tables(client, dataset):
    for table in TABLES:
        client.query(f"TRUNCATE TABLE `{PROJECT}.{dataset}.{table}`").result()


def event(start="2026-09-28T12:00:00Z", version=1, value=16000.0, report="RealtimeTotals", ingested="2026-09-28T12:56:00Z"):
    return {"schema_version": 1, "report": report, "measure": "total_load" if report == "PredispTotals" else "ontario_demand",
            "interval_start": start, "zone": "ONTARIO", "version": version, "value_mw": value, "published_at": "2026-09-28T12:54:48Z",
            "ingested_at": ingested, "source_uri": "gs://x", "content_sha256": "0" * 64, "reason": "backfill"}


def rows(client, dataset, table):
    return [dict(r) for r in client.query(f"SELECT * FROM `{PROJECT}.{dataset}.{table}` ORDER BY interval_start, version").result()]


# ---- the two MERGE statements ----

def test_a_first_write_fills_both_tables(client, dataset):
    write_batch(client, dataset, [event(), event(start="2026-09-28T12:05:00Z")])
    assert len(rows(client, dataset, "raw_demand_versions")) == 2
    assert len(rows(client, dataset, "current_demand")) == 2


def test_writing_the_same_batch_again_changes_nothing(client, dataset):
    batch = [event(), event(start="2026-09-28T12:05:00Z")]
    write_batch(client, dataset, batch)
    before = quality.summary(client, dataset)
    write_batch(client, dataset, batch)  # a crash before the commit means exactly this happens
    assert quality.summary(client, dataset) == before


def test_duplicates_inside_one_batch_become_one_row(client, dataset):
    write_batch(client, dataset, [event(), event(), event()])
    assert len(rows(client, dataset, "raw_demand_versions")) == 1
    assert len(rows(client, dataset, "current_demand")) == 1


def test_a_newer_version_with_a_new_value_updates_current_and_raw_keeps_both(client, dataset):
    write_batch(client, dataset, [event(version=1, value=16000.0)])
    write_batch(client, dataset, [event(version=2, value=16100.0)])
    assert [(r["version"], r["value_mw"]) for r in rows(client, dataset, "current_demand")] == [(2, 16100.0)]
    assert [r["version"] for r in rows(client, dataset, "raw_demand_versions")] == [1, 2]


def test_an_older_version_arriving_late_does_not_overwrite_current(client, dataset):
    write_batch(client, dataset, [event(version=2, value=16100.0)])
    write_batch(client, dataset, [event(version=1, value=16000.0)])  # backfill overlapping live
    assert [(r["version"], r["value_mw"]) for r in rows(client, dataset, "current_demand")] == [(2, 16100.0)]
    assert [r["version"] for r in rows(client, dataset, "raw_demand_versions")] == [1, 2]  # but history keeps it


def test_the_same_version_with_a_different_value_goes_to_whichever_was_seen_last(client, dataset):
    write_batch(client, dataset, [event(version=2, value=16000.0, ingested="2026-09-28T12:56:00Z")])
    write_batch(client, dataset, [event(version=2, value=16050.0, ingested="2026-09-28T13:10:00Z")])
    assert [r["value_mw"] for r in rows(client, dataset, "current_demand")] == [16050.0]


def test_the_newest_version_wins_inside_one_batch(client, dataset):
    write_batch(client, dataset, [event(version=1, value=16000.0), event(version=3, value=16300.0), event(version=2, value=16200.0)])
    assert [(r["version"], r["value_mw"]) for r in rows(client, dataset, "current_demand")] == [(3, 16300.0)]


# ---- data quality: green on clean data, red when it is corrupted ----

DAY_START = datetime(2026, 9, 20, 5, tzinfo=timezone.utc)  # midnight EST


def seed(client, dataset):
    """An ICIDemand reading (the historical range) and three complete EST days of five-minute readings."""
    realtime = [event(start=(DAY_START + timedelta(minutes=5 * i)).strftime("%Y-%m-%dT%H:%M:%SZ"), value=15000.0 + i % 50) for i in range(288 * 3)]
    write_batch(client, dataset, [event(report="ICIDemand", value=15000.0)] + realtime)


def test_quality_checks_pass_on_clean_data(client, dataset):
    seed(client, dataset)
    assert quality.run_all(client, dataset) == {"duplicate_keys": 0, "current_matches_raw": 0, "values_in_range": 0, "realtime_gaps": 0}


MIDDLE = "TIMESTAMP '2026-09-21 12:00:00+00'"
CURRENT = "`{p}.{d}.current_demand`"


@pytest.mark.parametrize("check, damage", [
    ("duplicate_keys", f"INSERT INTO {CURRENT} SELECT * FROM {CURRENT} WHERE interval_start = {MIDDLE} AND report = 'RealtimeTotals'"),
    ("current_matches_raw", f"UPDATE {CURRENT} SET version = 0 WHERE interval_start = {MIDDLE} AND report = 'RealtimeTotals'"),
    ("values_in_range", f"UPDATE {CURRENT} SET value_mw = 999999 WHERE interval_start = {MIDDLE} AND report = 'RealtimeTotals'"),
    ("realtime_gaps", f"DELETE FROM {CURRENT} WHERE interval_start = {MIDDLE} AND report = 'RealtimeTotals'"),
])
def test_quality_checks_go_red_when_the_data_is_corrupted(client, dataset, check, damage):
    seed(client, dataset)
    client.query(damage.format(p=PROJECT, d=dataset)).result()
    assert quality.run_all(client, dataset)[check] > 0
