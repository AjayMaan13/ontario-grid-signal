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
  default = 2
}
