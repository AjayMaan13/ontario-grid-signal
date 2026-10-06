# Datadog monitors and the dashboard, as code.
# Keys are NOT in any file: the provider reads DD_API_KEY and DD_APP_KEY from your terminal environment.
#   terraform apply -var="alert_email=you@example.com" -var="site=datadoghq.com"

terraform {
  required_version = ">= 1.5"

  required_providers {
    datadog = {
      source  = "DataDog/datadog"
      version = "~> 3.0"
    }
  }

  backend "gcs" {
    bucket = "ontario-grid-signal-tfstate"
    prefix = "grid-signal/datadog"
  }
}

variable "site" {
  type        = string
  default     = "datadoghq.com" # the same value as your DD_SITE
  description = "Your Datadog site, for example datadoghq.com, us3.datadoghq.com or datadoghq.eu"
}

variable "alert_email" {
  type        = string
  description = "Where alerts are sent"
}

# Thresholds are variables so a test can use a short one (for example 10 minutes) and then go back to the real one.
variable "freshness_minutes" {
  type        = number
  default     = 90
  description = "Alert when the newest data written to BigQuery is older than this"
}

variable "lag_events" {
  type        = number
  default     = 2000
  description = "Alert when the consumer is more than this many events behind for 5 minutes"
}

variable "failed_files" {
  type        = number
  default     = 3
  description = "Alert when more than this many files fail to parse in 15 minutes"
}

provider "datadog" {
  api_url = "https://api.${var.site}/"
}
