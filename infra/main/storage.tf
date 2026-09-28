# The raw bucket is created in infra/bootstrap so `terraform destroy` here can
# never delete it. This block only looks it up.
data "google_storage_bucket" "raw" {
  name = "${var.project_id}-raw"
}
