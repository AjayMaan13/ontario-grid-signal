import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

SCHEMA = [bigquery.SchemaField.from_api_repr(f) for f in json.loads((Path(__file__).resolve().parents[2] / "sql" / "demand_schema.json").read_text())]
COLUMNS = [field.name for field in SCHEMA]

# Every version of every interval, once. Re-sending an event changes nothing.
RAW_SQL = """
MERGE `{raw}` T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY report, interval_start, zone, version ORDER BY ingested_at) AS rn
    FROM `{staging}`)
  WHERE rn = 1
) S
ON T.report = S.report AND T.interval_start = S.interval_start AND T.zone = S.zone AND T.version = S.version
   AND T.interval_start BETWEEN @lo AND @hi
WHEN NOT MATCHED THEN INSERT ROW
"""

# Latest value per interval. A newer version wins; an older one arriving late never overwrites it.
# Same version but a different value (a silent restatement) is won by whichever was seen last.
CURRENT_SQL = """
MERGE `{current}` T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY report, interval_start, zone ORDER BY version DESC, ingested_at DESC) AS rn
    FROM `{staging}`)
  WHERE rn = 1
) S
ON T.report = S.report AND T.interval_start = S.interval_start AND T.zone = S.zone
   AND T.interval_start BETWEEN @lo AND @hi
WHEN MATCHED AND (S.version > T.version OR (S.version = T.version AND S.ingested_at > T.ingested_at)) THEN UPDATE SET
  measure = S.measure, version = S.version, value_mw = S.value_mw, published_at = S.published_at,
  ingested_at = S.ingested_at, loaded_at = S.loaded_at, source_uri = S.source_uri,
  content_sha256 = S.content_sha256, reason = S.reason
WHEN NOT MATCHED THEN INSERT ROW
"""
# The BETWEEN @lo AND @hi lines let BigQuery read only the partitions this batch touches.


def to_row(event: dict, loaded_at: datetime) -> dict:
    return {name: loaded_at.isoformat() if name == "loaded_at" else event[name] for name in COLUMNS}


def _utc(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def write_batch(client, dataset: str, events: list[dict]):
    """Load the batch into a staging table, MERGE it into both tables, drop the staging table.

    Raises if anything fails. The caller must not commit its Kafka offsets in that case.
    """
    if not events:
        return
    prefix = f"{client.project}.{dataset}"
    staging = f"{prefix}._staging_{uuid.uuid4().hex}"
    rows = [to_row(e, datetime.now(timezone.utc)) for e in events]
    starts = [e["interval_start"] for e in events]  # ISO strings sort in time order
    params = [bigquery.ScalarQueryParameter("lo", "TIMESTAMP", _utc(min(starts))), bigquery.ScalarQueryParameter("hi", "TIMESTAMP", _utc(max(starts)))]
    try:
        client.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_EMPTY")).result()
        for sql in (RAW_SQL, CURRENT_SQL):
            query = sql.format(raw=f"{prefix}.raw_demand_versions", current=f"{prefix}.current_demand", staging=staging)
            client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
