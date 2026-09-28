variable "project_id" {
  type    = string
  default = "ontario-grid-signal"
}

variable "region" {
  type    = string
  default = "northamerica-northeast2" # Toronto
}

variable "zone" {
  type    = string
  default = "northamerica-northeast2-a" # one zone = "zonal" cluster, cheaper than regional
}

variable "node_count" {
  type    = number
  default = 3 # 2 wasn't enough: GKE's own system pods (kube-dns, fluentbit, ...) already
  # use ~860m of each e2-medium's ~940m allocatable CPU, leaving no room for Kafka.
}
