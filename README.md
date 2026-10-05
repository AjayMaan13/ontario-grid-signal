# Ontario Grid Signal

A learning project that watches Ontario's public electricity demand data and flags hours that
look like they could become one of the year's top-5 demand peaks.

## The problem, in 60 seconds

Ontario's Industrial Conservation Initiative (ICI) charges large ("Class A") electricity
customers based on their share of usage during the **five highest-demand hours of the year**
(May-April). If a factory curtails power during those hours, it saves money — but the peak
hours are only known for certain after the year ends. This project builds a live signal that
flags likely-peak hours as they happen, using IESO's public data, and honestly backtests it
against IESO's own record of past peaks.

Full plan: [Ontario-Grid-Signal-Build-Guide.md](Ontario-Grid-Signal-Build-Guide.md).

## Status

| Weekend | What | Status |
|---|---|---|
| 0 | Understand the data, parsers, schema design | Done — [docs/data-notes.md](docs/data-notes.md) |
| 1 | Terraform (GKE, IAM, BigQuery) + hourly raw-file archiver | Done |
| 2 | Kafka (Strimzi) on the cluster, topics declared in Git | Done |
| 3 | Producer: IESO and the archive into Kafka, safe to restart | Done |
| 4 | Consumer: Kafka into BigQuery (every version + latest), data-quality checks, replay from zero | Done |
| 5-8 | Reconciliation, peak-risk signal, monitoring, load test | Not started |

## Layout

```
docs/           data notes and the 3 infra decisions (docs/decisions.md)
sql/            BigQuery table design
src/parsers/    turn raw IESO files into typed records (see tests/test_parsers.py)
src/producer/   IESO or archive -> events -> Kafka, with a checkpoint so restarts send nothing twice
src/consumer/   Kafka -> BigQuery (staging table + MERGE), and the data-quality checks
schemas/        the JSON Schema every event must match
archiver/       hourly job that copies IESO files into GCS before IESO deletes them
infra/
  bootstrap/    Terraform state bucket, raw bucket, image registry (never destroyed)
  main/         the GKE cluster, network, service accounts, BigQuery dataset
  archiver/     the archiver's Cloud Run job + Cloud Scheduler trigger
k8s/kafka/      Kafka cluster and topics (Strimzi custom resources)
k8s/producer/   producer Deployment and backfill Job
k8s/consumer/   consumer Deployment and replay Job
tests/          pytest: parsers (golden files) and the archiver's listing parser
```

## Why these tools

Short version, one line each — the guide has the full table with the "why not X" answer for
each:

- **Terraform** — the GKE cluster is created and destroyed every session, so it must be
  reproducible from code, not click-ops.
- **A tiny always-on archiver (Cloud Run + Scheduler)** — IESO deletes old forecast versions
  after about a month; this collects them before they're gone, independent of the cluster.
- **Kafka (Strimzi)** — decouples ingestion from multiple consumers (BigQuery sink, signal
  evaluator) and allows replaying history by offset. Volume is tiny for this project; the
  reason is replay and fan-out, not throughput.
- **GKE Standard, not Autopilot** — forces dealing with nodes, requests and limits, which is
  the Kubernetes depth the project is meant to show.

## Running it

Needs `gcloud`, `terraform` and `kubectl`, authenticated against your own GCP project.

```bash
export GOOGLE_OAUTH_ACCESS_TOKEN=$(gcloud auth print-access-token)

make up         # create the GKE cluster
make kafka-up   # install Strimzi, the Kafka cluster, and the 5 topics
make kafka-test # produce + consume one message, to prove it works
make down       # destroy the cluster and check for billable leftovers
make test       # run the Python tests (no cloud needed)
```

The raw-file archiver runs on its own schedule (hourly), independent of `make up`/`make down` —
see [infra/archiver](infra/archiver) and Decision A in [docs/decisions.md](docs/decisions.md).

## Cost

The cluster bills by the hour while it's up (Spot `e2-medium` nodes). Run `make down` when
you stop working. The archiver, buckets and BigQuery dataset cost close to nothing and are
meant to stay on.
