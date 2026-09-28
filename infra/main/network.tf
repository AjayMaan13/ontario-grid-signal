# The private network the cluster lives in.
resource "google_compute_network" "vpc" {
  name                    = "grid-signal"
  auto_create_subnetworks = false # we define the address ranges ourselves
}

# One subnet. Nodes get addresses from the main range; pods and services get
# their own "secondary" ranges. Sizes are fixed at creation, so they are generous.
resource "google_compute_subnetwork" "nodes" {
  name          = "grid-signal-nodes"
  network       = google_compute_network.vpc.id
  region        = var.region
  ip_cidr_range = "10.0.0.0/24" # 256 addresses for nodes

  # Machines with no public reach can still call Google APIs
  private_ip_google_access = true

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.4.0.0/16" # 65k addresses for pods
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.8.0.0/20" # 4k addresses for Kubernetes services
  }
}
