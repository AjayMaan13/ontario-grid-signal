# The dataset for the tables in sql/ddl.sql. Tables are added in Weekend 4.
resource "google_bigquery_dataset" "grid" {
  dataset_id = "grid"
  location   = var.region

  # Safe to wipe on destroy: everything in here can be rebuilt by replaying
  # the raw files in the raw bucket. Without this, destroy fails once tables exist.
  delete_contents_on_destroy = true
}

# Both tables share one column list, kept in sql/demand_schema.json (the consumer loads with it too).
locals {
  demand_schema = file("${path.module}/../../sql/demand_schema.json")
}

# Append-only: every version of every interval.
resource "google_bigquery_table" "raw_demand_versions" {
  dataset_id          = google_bigquery_dataset.grid.dataset_id
  table_id            = "raw_demand_versions"
  schema              = local.demand_schema
  clustering          = ["report", "zone"]
  deletion_protection = false # the dataset is rebuilt on every make up

  time_partitioning {
    type  = "DAY"
    field = "interval_start"
  }
}

# Latest known value per interval, kept by MERGE.
resource "google_bigquery_table" "current_demand" {
  dataset_id          = google_bigquery_dataset.grid.dataset_id
  table_id            = "current_demand"
  schema              = local.demand_schema
  clustering          = ["report", "zone"]
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "interval_start"
  }
}

# One row per nightly reconciliation run: what it found, sent and confirmed.
resource "google_bigquery_table" "reconciliation_runs" {
  dataset_id          = google_bigquery_dataset.grid.dataset_id
  table_id            = "reconciliation_runs"
  schema              = file("${path.module}/../../sql/reconciliation_runs_schema.json")
  deletion_protection = false
}

# What the live evaluator decided, hour by hour (see src/signals).
resource "google_bigquery_table" "signals" {
  dataset_id          = google_bigquery_dataset.grid.dataset_id
  table_id            = "signals"
  schema              = file("${path.module}/../../sql/signals_schema.json")
  deletion_protection = false
}
