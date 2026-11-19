# The metadata store (ADR-010) on Lakebase, Databricks managed PostgreSQL (engine layer), one instance per environment
# at the smallest capacity. If Lakebase is not available (the Free Edition probe), set enabled = false and bind the
# metadata store to a plain PostgreSQL adapter instead: same procedures, a binding change only.

terraform {
  required_providers {
    databricks = { source = "databricks/databricks" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }

variable "enabled" {
  type    = bool
  default = true
}

variable "capacity" {
  description = "Smallest is CU_1."
  type        = string
  default     = "CU_1"
}

resource "databricks_database_instance" "meta" {
  count    = var.enabled ? 1 : 0
  name     = "${var.name_prefix}-${var.env}-meta"
  capacity = var.capacity
}

output "instance_name" {
  value = var.enabled ? databricks_database_instance.meta[0].name : null
}

output "read_write_dns" {
  value = var.enabled ? databricks_database_instance.meta[0].read_write_dns : null
}
