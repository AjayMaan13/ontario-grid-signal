# The Kubernetes cluster (the "control plane" is run by Google).
resource "google_container_cluster" "main" {
  name     = "grid-signal"
  location = var.zone # a zone, not a region, makes it a zonal cluster

  network    = google_compute_network.vpc.id
  subnetwork = google_compute_subnetwork.nodes.id

  # Use the pods/services ranges from network.tf (this makes it "VPC-native")
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  # Lets a pod act as a Google service account with no key files
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Google insists on creating a default node pool. We delete it and add our own below.
  remove_default_node_pool = true
  initial_node_count       = 1

  # Google applies Kubernetes upgrades on a safe schedule
  release_channel {
    channel = "REGULAR"
  }

  # No username/password style login to the cluster
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }

  resource_labels = {
    project = "grid-signal"
  }

  # Default is true, which makes `terraform destroy` fail.
  deletion_protection = false
}

# The machines that run our pods.
resource "google_container_node_pool" "main" {
  name       = "main"
  cluster    = google_container_cluster.main.id
  location   = var.zone
  node_count = var.node_count

  # Google repairs broken machines and upgrades them for us
  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type    = "e2-medium"
    spot            = true # much cheaper; Google can take them back at any time
    disk_size_gb    = 30
    disk_type       = "pd-balanced"
    service_account = google_service_account.nodes.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    workload_metadata_config {
      mode = "GKE_METADATA" # needed for Workload Identity
    }

    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
  }
}
