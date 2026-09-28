terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # Terraform's memory lives in the bucket made by infra/bootstrap.
  # Backend blocks cannot use variables, so the name is written out.
  backend "gcs" {
    bucket = "ontario-grid-signal-tfstate"
    prefix = "grid-signal/main"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}
