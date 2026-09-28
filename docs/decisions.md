# Decisions

Recommended options from the build guide, accepted on 28 Sep 2026.

## A. Tiny always-on raw archiver: yes (Accepted)
IESO prunes intermediate forecast versions (about 11 days for PredispTotals). A Cloud Scheduler job plus a small Cloud Run job copies raw files into GCS hourly, independent of the GKE cluster that is destroyed between sessions. Cost should stay in the free tier (to check in the pricing calculator).

## B. GKE Standard, zonal (Accepted)
Same management-fee coverage as Autopilot, but Standard forces us to deal with node pools, requests and limits, which is the Kubernetes depth we want.

## C. No schema registry (Accepted)
Kafka is still used; only the registry is skipped. JSON events validated against a JSON Schema in code and tests. A registry is another stateful service for little gain at this volume.
