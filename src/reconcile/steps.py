"""What each task of the nightly DAG does. The DAG file only wires these together."""
import json
import re
from email.utils import format_datetime
from pathlib import Path

from google.cloud import bigquery

from producer.run import PARSERS, Metrics, process_file
from producer.sources import BASE, SourceFile, _fetch
from reconcile.compare import find_changes, from_dict

EVENT_CAP = 2000  # how many sent events are passed on for verification (Airflow's XCom is for small data)
RUN_SCHEMA = [bigquery.SchemaField.from_api_repr(f)
              for f in json.loads((Path(__file__).resolve().parents[2] / "sql" / "reconciliation_runs_schema.json").read_text())]


# ---- 1. find what is missing ----

def detect(source, known_files, handled_files, since, until):
    """(changes, how many files IESO lists per report) for the window [since, until)."""
    done = set(known_files) | set(handled_files)  # entries look like "RealtimeTotals/PUB_..._v12.xml"
    changes, listed = [], {}
    for report in PARSERS:
        files = source.list_files(report, since, until)
        listed[report] = len(files)
        names = {entry.split("/", 1)[1] for entry in done if entry.startswith(report + "/")}
        changes += find_changes(report, files, names)
    return changes, listed


# ---- 2. make sure the raw bucket holds them ----

def archive(changes, bucket, fetch=_fetch):
    """Copy any changed file that is not in the raw bucket yet. The hourly archiver has usually done it already."""
    copied = 0
    for change in (from_dict(c) for c in changes):
        blob = bucket.blob(f"{change.report}/{change.name}")
        if blob.exists():
            continue
        body = fetch(f"{BASE}/{change.report}/{change.name}")
        blob.metadata = {"ieso_last_modified": format_datetime(change.published_at, usegmt=True)}
        blob.upload_from_string(body)
        copied += 1
    return copied


# ---- 3. send them through Kafka with the producer's own code ----

class _Recorder:
    """Wraps the Kafka producer and remembers every event it sends."""

    def __init__(self, producer):
        self.producer, self.events = producer, []

    def produce(self, topic, key, value, on_delivery):
        self.events.append(json.loads(value))
        self.producer.produce(topic, key=key, value=value, on_delivery=on_delivery)

    def flush(self, timeout):
        return self.producer.flush(timeout)


def republish(changes, source, producer, checkpoint, uri_for):
    """Send the changed files as events with reason=reconciliation.

    checkpoint is the producer's checkpoint, loaded for reading only: it is used to skip values Kafka already has
    and is never saved here, so this job and the live producer never write the same file.
    """
    recorder, metrics, processed, failed = _Recorder(producer), Metrics(), [], []
    for data in changes:
        change = from_dict(data)
        file = SourceFile(change.name, change.published_at, uri_for(change.report, change.name))
        (processed if process_file(source, change.report, file, checkpoint, recorder, "reconciliation", metrics) else failed).append(data)
    return {
        "processed": processed,
        "failed": failed,
        "events_sent": len(recorder.events),
        "files_with_new_values": len({e["source_uri"] for e in recorder.events}),
        "events": [{k: e[k] for k in ("report", "interval_start", "version", "value_mw")} for e in recorder.events[:EVENT_CAP]],
    }


# ---- 4. check BigQuery has them ----

def landed_count(client, dataset, events) -> int:
    """How many of the sent events are now reflected in current_demand (its version is at least the sent one)."""
    if not events:
        return 0
    structs = []
    for e in events:  # these values come from our own schema-checked events; they are validated again before going into SQL
        if e["report"] not in PARSERS or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", e["interval_start"]) or not isinstance(e["version"], int):
            raise ValueError(f"unexpected event, refusing to build SQL: {e}")
        structs.append(f"STRUCT('{e['report']}' AS report, TIMESTAMP '{e['interval_start']}' AS interval_start, {e['version']} AS version)")
    lo, hi = min(e["interval_start"] for e in events), max(e["interval_start"] for e in events)
    sql = f"""
        WITH k AS (SELECT * FROM UNNEST([{', '.join(structs)}]))
        SELECT COUNT(*) FROM k JOIN `{client.project}.{dataset}.current_demand` c
          ON c.report = k.report AND c.interval_start = k.interval_start AND c.zone = 'ONTARIO'
        WHERE c.interval_start BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}' AND c.version >= k.version"""
    return list(client.query(sql).result())[0][0]


# ---- 5. write the run summary ----

def record_run(client, dataset, row: dict):
    """One row per run. Running the same interval again replaces its row."""
    table = f"{client.project}.{dataset}.reconciliation_runs"
    client.query(f"DELETE FROM `{table}` WHERE run_key = @key",
                 job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("key", "STRING", row["run_key"])])).result()
    client.load_table_from_json([row], table, job_config=bigquery.LoadJobConfig(schema=RUN_SCHEMA, write_disposition="WRITE_APPEND")).result()
