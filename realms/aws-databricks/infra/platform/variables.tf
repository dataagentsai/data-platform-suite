variable "env" {
  description = "dev or prod"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env is dev or prod."
  }
}

variable "name_prefix" {
  description = "Prefix of every AWS name. S3 bucket names are global, so make it unique to the company (e.g. acme-dp)."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name_prefix))
    error_message = "lower case letters, digits and hyphens, 3 to 21 characters (bucket names must stay under 63)."
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "azs" {
  type    = list(string)
  default = ["us-east-2a", "us-east-2b"]
}

variable "databricks_account_id" {
  description = "From the Databricks account console (top right menu). Not a secret."
  type        = string
}

variable "metastore_id" {
  description = "The regional Unity Catalog metastore, output of infra/account."
  type        = string
}

variable "cost_center" {
  type    = string
  default = "data-platform"
}

variable "extra_tags" {
  type    = map(string)
  default = {}
}

variable "layers" {
  description = "One bucket per layer (brief: landing, bronze, silver, gold, quarantine; ADR-012 adds ops)."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "quarantine", "ops"]
}

variable "catalog_layers" {
  description = "Layers that get a Unity Catalog catalog <env>_<layer> (ADR-012)."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "ops"]
}

variable "key_per_layer" {
  type    = bool
  default = false
}

variable "force_destroy" {
  description = "dev: true (destroy wipes data). prod: false."
  type        = bool
  default     = false
}

variable "expire_days" {
  description = "Object expiry per layer (landing is kept for replays; quarantine is short-lived)."
  type        = map(number)
  default     = { quarantine = 30 }
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/20"
}

variable "enable_nat" {
  description = "Only classic clusters need it; serverless does not. ~35 USD a month."
  type        = bool
  default     = false
}

variable "secret_names" {
  description = "Secrets Manager containers (values set by hand, never in code)."
  type        = list(string)
  default     = ["sources"]
}

variable "streams" {
  description = "Kinesis streams, name => shards. Empty: no stream (and no stream cost)."
  type        = map(number)
  default     = {}
}

variable "stream_retention_hours" {
  type    = number
  default = 24
}

variable "lakebase_enabled" {
  type    = bool
  default = true
}

variable "lakebase_capacity" {
  type    = string
  default = "CU_1"
}

variable "budget_usd" {
  type    = number
  default = 25
}

variable "budget_emails" {
  type    = list(string)
  default = []
}
