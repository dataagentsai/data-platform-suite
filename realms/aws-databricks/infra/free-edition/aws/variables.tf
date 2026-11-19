variable "env" {
  description = "dev or prod"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env is dev or prod."
  }
}

variable "name_prefix" {
  description = "Prefix of every AWS name; S3 names are global, so make it unique."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.name_prefix))
    error_message = "lower case letters, digits and hyphens, 3 to 21 characters."
  }
}

variable "aws_region" {
  type    = string
  default = "us-east-2"
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
  description = "Buckets <prefix>-<env>-<layer>. Free Edition keeps bronze/silver/gold in its own storage, so only the movers' layers."
  type        = list(string)
  default     = ["landing", "quarantine"]
}

variable "force_destroy" {
  type    = bool
  default = false
}

variable "expire_days" {
  type    = map(number)
  default = { quarantine = 30 }
}

variable "streams" {
  description = "Kinesis streams, name => shards. Default {}: declared, none created (a shard is ~11 USD a month)."
  type        = map(number)
  default     = {}
}

variable "stream_retention_hours" {
  type    = number
  default = 24
}

variable "secret_names" {
  type    = list(string)
  default = []
}

variable "alert_emails" {
  type    = list(string)
  default = []
}

variable "log_retention_days" {
  type    = number
  default = 7
}

variable "manage_ci_roles" {
  description = "Once per account: the GitHub OIDC provider and CI roles. true in exactly one environment (dev)."
  type        = bool
  default     = false
}

variable "github_repo" {
  type    = string
  default = "dataagentsai/data-platform-suite"
}
