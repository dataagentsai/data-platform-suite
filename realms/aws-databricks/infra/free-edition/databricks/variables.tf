variable "env" {
  description = "dev or prod"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env is dev or prod."
  }
}

variable "databricks_host" {
  description = "The Free Edition workspace URL (not a secret)."
  type        = string
}

variable "warehouse_id" {
  description = "Serverless SQL warehouse used to run CREATE CATALOG."
  type        = string
}

variable "catalog_layers" {
  description = "Layers that get a catalog <env>_<layer> (ADR-012)."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "ops"]
}

variable "schemas" {
  description = "layer => schemas. Bronze and silver: one per source of the tyre manifest; gold: one per domain."
  type        = map(list(string))
  default = {
    landing = ["deliveries"]
    bronze  = ["dealer_api", "erp", "lims", "mes", "partner", "press_stream"]
    silver  = ["dealer_api", "erp", "lims", "mes", "partner", "press_stream"]
    gold    = ["tyre"]
    ops     = ["meta", "access"]
  }
}

variable "force_destroy" {
  type    = bool
  default = false
}

variable "lakebase_create" {
  description = "true: Terraform makes the instance dp-<env>-meta. false: look up var.lakebase_instance (not owned)."
  type        = bool
  default     = false
}

variable "lakebase_instance" {
  description = "Existing Lakebase instance to look up when lakebase_create = false."
  type        = string
  default     = ""
}

variable "lakebase_capacity" {
  type    = string
  default = "CU_1"
}

variable "meta_database" {
  description = "PostgreSQL database for the metadata store inside the instance (made by meta_setup.py)."
  type        = string
  default     = "dp_meta"
}
