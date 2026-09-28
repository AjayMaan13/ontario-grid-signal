# The archiver: a small job that copies IESO files into the raw bucket every hour.
# It lives in its own config (not infra/main) so `make down` never touches it:
# IESO deletes old files, so collecting must not stop when the cluster is off.

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  backend "gcs" {
    bucket = "ontario-grid-signal-tfstate"
    prefix = "grid-signal/archiver"
  }
}

variable "project_id" {
  type    = string
  default = "ontario-grid-signal"
}

variable "region" {
  type    = string
  default = "northamerica-northeast2"
}

variable "image" {
  type    = string
  default = "northamerica-northeast2-docker.pkg.dev/ontario-grid-signal/grid/archiver:latest"
}

provider "google" {
  project = var.project_id
  region  = var.region
}
