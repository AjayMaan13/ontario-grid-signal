# A service account is a robot identity. Each part of the project gets its own,
# with only the permissions it needs (sa-archiver lives in infra/archiver). No key files: pods will act as these
# accounts through Workload Identity (wired up when the pods exist).

resource "google_service_account" "producer" {
  account_id = "sa-producer"
}

resource "google_service_account" "consumer" {
  account_id = "sa-consumer"
}

resource "google_service_account" "airflow" {
  account_id = "sa-airflow"
}

# The machines (nodes) themselves. Without this they run as the default account,
# which has project-wide Editor rights. Logs and metrics only.
resource "google_service_account" "nodes" {
  account_id = "sa-gke-nodes"
}

resource "google_project_iam_member" "nodes" {
  for_each = toset(["roles/logging.logWriter", "roles/monitoring.metricWriter", "roles/artifactregistry.reader"])
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${google_service_account.nodes.email}"
}

# producer: only reads raw files
resource "google_storage_bucket_iam_member" "producer" {
  bucket = data.google_storage_bucket.raw.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.producer.email}"
}

# airflow: fetches changed files into the raw bucket
resource "google_storage_bucket_iam_member" "airflow" {
  bucket = data.google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# consumer and airflow: edit tables in the grid dataset (and only that dataset)
resource "google_bigquery_dataset_iam_member" "consumer" {
  dataset_id = google_bigquery_dataset.grid.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.consumer.email}"
}

resource "google_bigquery_dataset_iam_member" "airflow" {
  dataset_id = google_bigquery_dataset.grid.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airflow.email}"
}

# Running any BigQuery query (like the MERGE) needs permission to start a job.
# This is a project-level role but grants no access to data by itself.
resource "google_project_iam_member" "bq_jobs" {
  for_each = {
    consumer = google_service_account.consumer.email
    airflow  = google_service_account.airflow.email
  }
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${each.value}"
}

# producer: saves its checkpoint (what it has already sent) in the state bucket
resource "google_storage_bucket_iam_member" "producer_checkpoint" {
  bucket = data.google_storage_bucket.state.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.producer.email}"
}

# Workload Identity: lets the Kubernetes service account "producer" in namespace "grid"
# act as sa-producer, with no key file.
resource "google_service_account_iam_member" "producer_workload_identity" {
  service_account_id = google_service_account.producer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[grid/producer]"
}

# Workload Identity for the consumer: the Kubernetes service account "consumer" in namespace
# "grid" acts as sa-consumer (which can edit the grid dataset and run BigQuery jobs).
resource "google_service_account_iam_member" "consumer_workload_identity" {
  service_account_id = google_service_account.consumer.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[grid/consumer]"
}

# airflow: reads the producer's checkpoint and keeps its own list of files it has already fixed
resource "google_storage_bucket_iam_member" "airflow_state" {
  bucket = data.google_storage_bucket.state.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.airflow.email}"
}

# Workload Identity: the Kubernetes service account "airflow" in namespace "airflow" acts as sa-airflow.
# With LocalExecutor the tasks run inside the scheduler pod, which uses this account.
resource "google_service_account_iam_member" "airflow_workload_identity" {
  service_account_id = google_service_account.airflow.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[airflow/airflow]"
}
