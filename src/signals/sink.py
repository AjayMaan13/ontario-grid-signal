import json
import uuid
from pathlib import Path

from google.cloud import bigquery

SCHEMA = [bigquery.SchemaField.from_api_repr(f) for f in json.loads((Path(__file__).resolve().parents[2] / "sql" / "signals_schema.json").read_text())]

# Sending the same decision twice (a restart replays recent events) must change nothing.
MERGE = """
MERGE `{table}` T USING `{staging}` S
ON T.variant = S.variant AND T.candidate_hour = S.candidate_hour AND T.decided_at = S.decided_at
WHEN NOT MATCHED THEN INSERT ROW
"""


def write_signals(client, dataset: str, rows: list[dict]):
    """Load the rows into a staging table, MERGE them into `signals`, drop the staging table."""
    if not rows:
        return
    staging = f"{client.project}.{dataset}._staging_{uuid.uuid4().hex}"
    try:
        client.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_EMPTY")).result()
        client.query(MERGE.format(table=f"{client.project}.{dataset}.signals", staging=staging)).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
