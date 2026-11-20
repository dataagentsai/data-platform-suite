# The metadata store (ADR-010) on Lakebase, Databricks managed PostgreSQL: one autoscaling project per environment,
# named dp-meta-<env> unless overridden (the engine bundle's meta_instance and the first cloud's store use that name).
# Lakebase must be offered in the workspace's region (dbs/cloud/azure.yaml, metadata_store_region). If it is not, set
# enabled = false: the store then binds to a PostgreSQL flexible server behind the same procedures (NOT BUILT).

terraform {
  required_providers {
    databricks = { source = "databricks/databricks" }
  }
}

variable "env" { type = string }

variable "enabled" {
  type    = bool
  default = true
}

variable "project_name" {
  description = "null: dp-meta-<env>"
  type        = string
  default     = null
}

variable "capacity_cu" {
  description = "Compute units, fixed (min = max): a computes of this size must have min_cu == max_cu (DEPLOYED.md, 13 Nov)."
  type        = number
  default     = 1
}

locals {
  name = coalesce(var.project_name, "dp-meta-${var.env}")
}

resource "databricks_postgres_project" "meta" {
  count      = var.enabled ? 1 : 0
  project_id = local.name
  spec = {
    display_name = local.name
    pg_version   = 16
    default_endpoint_settings = {
      autoscaling_limit_min_cu = var.capacity_cu
      autoscaling_limit_max_cu = var.capacity_cu
    }
  }
}

output "project_name" { value = var.enabled ? local.name : null }
