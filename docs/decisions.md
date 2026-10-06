# Decisions

Recommended options from the build guide, accepted on 28 Sep 2026.

## A. Tiny always-on raw archiver: yes (Accepted)
IESO prunes intermediate forecast versions (about 11 days for PredispTotals). A Cloud Scheduler job plus a small Cloud Run job copies raw files into GCS hourly, independent of the GKE cluster that is destroyed between sessions. Cost should stay in the free tier (to check in the pricing calculator).

## B. GKE Standard, zonal (Accepted)
Same management-fee coverage as Autopilot, but Standard forces us to deal with node pools, requests and limits, which is the Kubernetes depth we want.

## C. No schema registry (Accepted)
Kafka is still used; only the registry is skipped. JSON events validated against a JSON Schema in code and tests. A registry is another stateful service for little gain at this volume.

## D. BigQuery load path: load job into a staging table, then MERGE (Accepted)
The consumer collects a micro-batch (500 events or 60 seconds), loads it into a uniquely named staging table, runs one `MERGE` into `raw_demand_versions` (insert only if the version is new) and one into `current_demand` (newer version wins), drops the staging table, and only then commits its Kafka offsets. Load jobs and `MERGE` are built for batches and cost nothing at this volume. The Storage Write API is faster but needs much more code. Replaying or re-sending an event changes nothing, so at-least-once delivery is safe.

## E. Reconciliation compares IESO's listing with the producer's checkpoint (Accepted)
The nightly DAG lists IESO's files for the 30 days before its data interval and compares them with the files the producer has already processed (its checkpoint), plus a small list of files earlier reconciliations fixed. It does not compare against BigQuery rows: a re-issued file whose values are unchanged creates no new rows, so a row comparison would flag the same file every night. Files are classified `new_group` (the live path never saw the hour) or `new_version` (IESO re-issued an hour we hold). Events go to the normal demand topics with `reason=reconciliation`, so the existing consumer handles them. The DAG never writes the producer's checkpoint, so the two never overwrite each other.

## F. Airflow: LocalExecutor, DAGs and code from git-sync (Accepted)
Tasks run inside the scheduler pod (LocalExecutor): the simplest option, enough for one nightly DAG. The repo is public, so git-sync pulls the DAG files and `src/` straight from GitHub; the image holds only libraries, so a code change needs a `git push`, not an image build. The chart's default 100 GB scheduler log disk is turned off; only Postgres keeps a 5 GB disk, which `make airflow-down` removes.

## G. The live evaluator follows Kafka and runs the backtest's own function (Accepted)
`src/signals/run.py` consumes `ieso.demand.ici` and calls the same `decide()` the backtest uses, with the window and margins frozen in `backtest/results.json`; it never has parameters of its own. It keeps no state and never commits offsets, so each start replays the topic and rebuilds the demand history from Kafka; only recent `live` hours produce decisions, so a replay never re-announces old ones, and BigQuery inserts are idempotent on (variant, hour, decision time). Decisions are written to the `grid.signals` topic and a `signals` table, including off-season "outside the window" rows, so the table also shows the evaluator is alive (a freshness signal for Weekend 7). The `oracle` variant is never run live because it needs the hour's own demand.

## H. Kafka key is report|zone|delivery date (Accepted)
The key was `report|zone`. With a single zone, every event of a topic had the same key and landed in one partition: the other five were empty, so extra consumers (KEDA, Weekend 7) would have had nothing to share. The key now includes the EST delivery date. All versions of one interval still share a key, so they stay in order on one partition (the property that matters for the MERGE tie-break), while different days spread across partitions. Found by reading the lag table in Weekend 4; fixed before the load test.
