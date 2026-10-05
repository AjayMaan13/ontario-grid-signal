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
