"""python -m consumer.cli check | summary | reset | runs"""
import json
import os
import sys

from google.cloud import bigquery

from consumer import quality

client = bigquery.Client(project=os.environ.get("GCP_PROJECT", "ontario-grid-signal"))
dataset = os.environ.get("BQ_DATASET", "grid")
command = sys.argv[1] if len(sys.argv) > 1 else "check"

if command == "check":
    results = quality.run_all(client, dataset)
    print(json.dumps(results, indent=2))
    sys.exit(1 if any(results.values()) else 0)
elif command == "summary":
    print(json.dumps(quality.summary(client, dataset), indent=2))
elif command == "runs":  # what the nightly reconciliation DAG has found, one row per run
    sql = f"SELECT run_key, status, listed_files, new_group_files, new_version_files, files_processed, files_failed, files_with_new_values, events_sent, events_landed FROM `{client.project}.{dataset}.reconciliation_runs` ORDER BY run_key"
    for row in client.query(sql).result():
        print(dict(row))
elif command == "reset":  # empties both tables, for a clean replay
    for table in ("raw_demand_versions", "current_demand"):
        client.query(f"TRUNCATE TABLE `{client.project}.{dataset}.{table}`").result()
    print("both tables emptied")
else:
    sys.exit("usage: python -m consumer.cli check | summary | reset | runs")
