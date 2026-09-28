output "cluster_name" {
  value = google_container_cluster.main.name
}

output "zone" {
  value = var.zone
}
