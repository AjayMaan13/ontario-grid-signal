# The dataset for the tables in sql/ddl.sql. Tables are added in Weekend 4.
resource "google_bigquery_dataset" "grid" {
  dataset_id = "grid"
  location   = var.region

  # Safe to wipe on destroy: everything in here can be rebuilt by replaying
  # the raw files in the raw bucket. Without this, destroy fails once tables exist.
  delete_contents_on_destroy = true
}
