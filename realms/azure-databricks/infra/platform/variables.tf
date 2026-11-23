variable "env" {
  description = "dev or prod"
  type        = string
  validation {
    condition     = contains(["dev", "prod"], var.env)
    error_message = "env is dev or prod."
  }
}

variable "subscription_id" {
  description = "The Azure subscription. Never written in a file: TF_VAR_subscription_id or ARM_SUBSCRIPTION_ID."
  type        = string
}

variable "name_prefix" {
  description = "Prefix of every Azure name. Storage account names (<prefix><env>, <prefix><env>pi) are global, 3 to 24 lowercase letters and digits, so the prefix is short, unique to the owner and has no hyphen."
  type        = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9]{1,13}$", var.name_prefix))
    error_message = "name_prefix: 2 to 14 characters, a lowercase letter first, then lowercase letters and digits only (the longest account name, <prefix>prodpi, must fit 24 characters; the key vault <prefix>-<env>-kv must fit too)."
  }
}

variable "location" {
  description = "Azure region (internal name, e.g. eastus2). Must offer Lakebase: README, 'Region'."
  type        = string
  default     = "eastus2"
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
  description = "One container per layer (the platform's own list: dataplatform.cloud_ports.LAYERS)."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "quarantine", "ops"]
}

variable "catalog_layers" {
  description = "Layers that get a Unity Catalog catalog <env>_<layer> (ADR-012)."
  type        = list(string)
  default     = ["landing", "bronze", "silver", "gold", "ops"]
}

variable "storage_replication" {
  type    = string
  default = "LRS"
}

variable "shared_access_key_enabled" {
  type    = bool
  default = true
}

variable "force_destroy" {
  description = "dev: true (destroy drops catalogs with their contents). prod: false."
  type        = bool
  default     = false
}

variable "key_vault_purge_protection" {
  type    = bool
  default = false
}

variable "lakebase_enabled" {
  type    = bool
  default = true
}

variable "lakebase_project_name" {
  description = "Defaults to dp-meta-<env>, the name the engine bundle and the Free Edition store use."
  type        = string
  default     = null
}

variable "budget_amount" {
  description = "Monthly budget per environment, in the subscription's BILLING currency (an account billed in rupees: rupees, not dollars)."
  type        = number
  default     = 1800
}

variable "budget_emails" {
  description = "Who is told at 80% actual and 100% forecast. Set in the untracked tfvars, never in a tracked file."
  type        = list(string)
  default     = []
}

variable "secrets_reader_object_ids" {
  description = "Entra object ids of identities that read Key Vault secrets."
  type        = list(string)
  default     = []
}

variable "data_factory_enabled" {
  description = "Create the managed copy service for the movers (ADR-033). Idle it costs nothing."
  type        = bool
  default     = true
}
