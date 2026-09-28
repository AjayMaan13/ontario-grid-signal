# Created in infra/bootstrap so it can never be destroyed. Only looked up here.
data "google_storage_bucket" "raw" {
  name = "${var.project_id}-raw"
}

# The job's identity: may write files into the raw bucket and nothing else.
resource "google_service_account" "archiver" {
  account_id = "sa-archiver"
}

resource "google_storage_bucket_iam_member" "archiver" {
  bucket = data.google_storage_bucket.raw.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.archiver.email}"
}

# The job itself: runs the container once, then stops. You pay only while it runs.
resource "google_cloud_run_v2_job" "archiver" {
  name                = "ieso-archiver"
  location            = var.region
  deletion_protection = false

  template {
    template {
      service_account = google_service_account.archiver.email
      timeout         = "3600s" # the first run copies about a month of files
      max_retries     = 1

      containers {
        image = var.image

        env {
          name  = "BUCKET"
          value = data.google_storage_bucket.raw.name
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }
}

# The clock: Cloud Scheduler starts the job. It needs its own identity to do so.
resource "google_service_account" "scheduler" {
  account_id = "sa-archiver-scheduler"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_can_run" {
  name     = google_cloud_run_v2_job.archiver.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "hourly" {
  name      = "ieso-archiver-hourly"
  region    = var.region
  schedule  = "20 * * * *" # 20 minutes past every hour
  time_zone = "Etc/UTC"

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.archiver.name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}
