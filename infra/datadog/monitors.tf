# Three alerts, in the order that matters for a peak signal: stale data first, then lag, then errors.
# A silent pipeline means a missed peak, so "no data" counts as an alert too.

resource "datadog_monitor" "freshness" {
  name    = "Grid signal: demand data is stale"
  type    = "metric alert"
  message = <<-EOT
    The newest demand data written to BigQuery is older than ${var.freshness_minutes} minutes.
    If the consumer is running but this fires, the producer or IESO has stopped: check the producer logs.
    If it is "no data", the consumer itself is down. See docs/observability.md.
    @${var.alert_email}
  EOT

  query = "max(last_10m):max:grid.consumer.data_age_s{service:consumer} > ${var.freshness_minutes * 60}"

  monitor_thresholds {
    critical = var.freshness_minutes * 60
  }

  notify_no_data      = true
  no_data_timeframe   = 20
  require_full_window = false
  tags                = ["project:grid-signal", "sli:freshness"]
}

resource "datadog_monitor" "consumer_lag" {
  name    = "Grid signal: consumer is falling behind"
  type    = "metric alert"
  message = <<-EOT
    The BigQuery consumer has been more than ${var.lag_events} events behind for 5 minutes.
    Producer healthy but lag rising: the consumers cannot keep up (BigQuery write limits?). Check KEDA and the consumer logs.
    @${var.alert_email}
  EOT

  query = "min(last_5m):sum:grid.consumer.lag_total{service:consumer} > ${var.lag_events}"

  monitor_thresholds {
    critical = var.lag_events
  }

  notify_no_data      = false
  require_full_window = false
  tags                = ["project:grid-signal", "sli:lag"]
}

resource "datadog_monitor" "producer_errors" {
  name    = "Grid signal: producer is failing to parse files"
  type    = "metric alert"
  message = <<-EOT
    More than ${var.failed_files} IESO files failed to parse in 15 minutes. IESO may have changed a file format.
    Look for file_failed lines in the producer logs.
    @${var.alert_email}
  EOT

  query = "sum(last_15m):sum:grid.producer.files_failed{*}.as_count() > ${var.failed_files}"

  monitor_thresholds {
    critical = var.failed_files
  }

  notify_no_data      = false
  require_full_window = false
  tags                = ["project:grid-signal", "sli:errors"]
}

resource "datadog_dashboard_json" "pipeline" {
  dashboard = file("${path.module}/dashboard.json")
}
