# Bootstrap: creates the two buckets that must outlive every `make down`:
#   1. tfstate - Terraform's memory for infra/main
#   2. raw     - raw IESO files copied hourly by the archiver (the source of truth)
#   3. grid    - Artifact Registry repo that stores the archiver's container image
# Uses LOCAL state on purpose (it cannot store its own state in the bucket it creates).
# Apply when changed, otherwise leave alone. Nothing here is ever destroyed.

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

variable "project_id" {
  type        = string
  description = "GCP project ID (not the project name)"
}

variable "region" {
  type    = string
  default = "northamerica-northeast2" # Toronto
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "tf_state" {
  name                        = "${var.project_id}-tfstate" # bucket names are global, project id keeps it unique
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true # every state change is kept, so a bad apply can be rolled back
  }

  lifecycle {
    prevent_destroy = true
  }
}

output "state_bucket" {
  value = google_storage_bucket.tf_state.name
}

resource "google_storage_bucket" "raw" {
  name                        = "${var.project_id}-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle {
    prevent_destroy = true
  }
}

output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

resource "google_artifact_registry_repository" "grid" {
  repository_id = "grid"
  location      = var.region
  format        = "DOCKER"

  lifecycle {
    prevent_destroy = true
  }
}
