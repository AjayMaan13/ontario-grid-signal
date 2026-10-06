"""Nightly: find IESO files the live path missed or IESO re-issued, send them through Kafka, confirm they landed.

Every task works from the run's data interval, never from "now", so re-running any past day gives that day's answer.
The task bodies live in src/reconcile/steps.py; imports are inside the tasks to keep DAG parsing fast.
"""
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import PokeReturnValue, dag, task

LOOKBACK = timedelta(days=30)  # IESO keeps hourly files for about a month


@dag(
    dag_id="reconcile_ieso_revisions",
    schedule="0 8 * * *",  # 08:00 UTC, every day
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    catchup=False,  # chosen on purpose: old days are run explicitly with a backfill
    max_active_runs=1,
    default_args={"owner": "ajay", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ieso", "reconciliation"],
)
def reconcile_ieso_revisions():
    @task
    def find_changes(data_interval_start=None, data_interval_end=None):
        """Compare IESO's file list for the 30 days before this interval with what the producer already processed."""
        from producer import store
        from producer.sources import IESOSource
        from reconcile.compare import summarize, to_dict
        from reconcile.steps import detect

        since, until = data_interval_end - LOOKBACK, data_interval_end
        known = store.load(os.environ["CHECKPOINT_URI"]).files
        handled = store.load(os.environ["HANDLED_URI"]).files
        changes, listed = detect(IESOSource(), known, handled, since, until)
        return {"interval_start": data_interval_start.isoformat(), "interval_end": data_interval_end.isoformat(),
                "listed": listed, "summary": summarize(changes), "changes": [to_dict(c) for c in changes]}

    @task
    def fetch_and_archive(found):
        """Make sure each changed file is in the raw bucket (usually the hourly archiver already put it there)."""
        from google.cloud import storage

        from reconcile.steps import archive

        bucket = storage.Client().bucket(os.environ["RAW_BUCKET"])
        return {"changes": found["changes"], "copied": archive(found["changes"], bucket)}

    @task
    def republish(archived):
        """Send the changed files to Kafka as events tagged reason=reconciliation."""
        from confluent_kafka import Producer

        from producer.sources import GCSSource
        from producer import store
        from reconcile.steps import republish as send

        bucket = os.environ["RAW_BUCKET"]
        producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"], "enable.idempotence": True, "acks": "all"})
        checkpoint = store.load(os.environ["CHECKPOINT_URI"])  # read only: never saved here
        return send(archived["changes"], GCSSource(bucket), producer, checkpoint, lambda report, name: f"gs://{bucket}/{report}/{name}")

    @task.sensor(poke_interval=30, timeout=900, mode="reschedule")
    def verify_landed(result):
        """Wait until BigQuery's current_demand reflects every event that was sent."""
        from google.cloud import bigquery

        from reconcile.steps import landed_count

        # A sensor only hands a value to the next task when it is wrapped in PokeReturnValue(xcom_value=...).
        if not result["events"]:
            return PokeReturnValue(is_done=True, xcom_value=True)  # nothing new was sent, so there is nothing to wait for
        client = bigquery.Client(project=os.environ["GCP_PROJECT"])
        done = landed_count(client, os.environ.get("BQ_DATASET", "grid"), result["events"]) == len(result["events"])
        return PokeReturnValue(is_done=done, xcom_value=done)

    @task(trigger_rule="all_done")  # record the run even when the check above timed out
    def record_run_summary(found, result, landed, data_interval_start=None, data_interval_end=None):
        from google.cloud import bigquery

        from producer import store
        from reconcile.compare import from_dict
        from reconcile.steps import record_run

        found, result = found or {}, result or {}
        # If an earlier task failed there is no "found", but the run must still leave a row: use the run's own interval.
        start, end = found.get("interval_start") or data_interval_start.isoformat(), found.get("interval_end") or data_interval_end.isoformat()
        changes = found.get("changes", [])
        by_kind = {kind: sum(c["kind"] == kind for c in changes) for kind in ("new_group", "new_version")}
        ok = bool(landed) and not result.get("failed")
        if landed:  # only files whose events are confirmed in BigQuery count as handled
            handled = store.load(os.environ["HANDLED_URI"])
            for change in map(from_dict, result.get("processed", [])):
                handled.mark_file_done(change.report, change.name)
            store.save(os.environ["HANDLED_URI"], handled)
        client = bigquery.Client(project=os.environ["GCP_PROJECT"])
        record_run(client, os.environ.get("BQ_DATASET", "grid"), {
            "run_key": start, "data_interval_start": start, "data_interval_end": end,
            "listed_files": sum(found.get("listed", {}).values()),
            "new_group_files": by_kind["new_group"], "new_version_files": by_kind["new_version"],
            "files_processed": len(result.get("processed", [])), "files_failed": len(result.get("failed", [])),
            "files_with_new_values": result.get("files_with_new_values", 0),
            "events_sent": result.get("events_sent", 0), "events_landed": result.get("events_sent", 0) if landed else 0,
            "status": "ok" if ok else "needs_attention", "by_report": str(found.get("summary", {})),
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        })

    found = find_changes()
    archived = fetch_and_archive(found)
    result = republish(archived)
    landed = verify_landed(result)
    record_run_summary(found, result, landed)


reconcile_ieso_revisions()
