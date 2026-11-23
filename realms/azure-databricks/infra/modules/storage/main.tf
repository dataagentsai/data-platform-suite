# Storage for one environment (dbs/cloud/azure.yaml, decisions landing_layout and storage_naming):
#   * one Data Lake Storage Gen2 account named <prefix><env> (dataplatform.adapters.azure_blob.account_name), with one
#     container per layer, named exactly by the layer;
#   * a separate account <prefix><env>pi for personal data, whose landing container is purged by a lifecycle rule.
# Containers are made through the management plane (storage_account_id), so no account key is needed to apply.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" {
  description = "Prefix of the account name: lowercase letters and digits."
  type        = string
}
variable "env" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "tags" { type = map(string) }

variable "layers" {
  description = "One container per layer."
  type        = list(string)
}

variable "replication" {
  description = "LRS is cheapest; prod may choose ZRS or GRS."
  type        = string
  default     = "LRS"
}

variable "shared_access_key_enabled" {
  description = "Account keys. Unity Catalog reaches the account through the access connector and the adapters use Entra ID, so no key is needed; true only if a first apply cannot reach the data plane without one."
  type        = bool
  default     = true
}

variable "personal_purge_days" {
  description = "Lifecycle rules work in whole days, so 1 is the finest the platform can ask of the cloud. The 23-hour purge the spec asks for (personal_landing_max_hours) is carried by the platform's own purge; this is the backstop."
  type        = number
  default     = 1
}

variable "force_destroy" {
  description = "Present for symmetry with the first cloud: the resource group destroy removes the accounts either way."
  type        = bool
  default     = false
}

locals {
  account_name  = "${var.name_prefix}${var.env}"
  personal_name = "${var.name_prefix}${var.env}pi"
}

resource "azurerm_storage_account" "env" {
  name                            = local.account_name
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = var.replication
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = var.shared_access_key_enabled
  tags                            = var.tags
}

# The file drop zone: where partners and file transfers leave files, <system>/<dataset>/<name> (the FileDrop port).
# It is a source, not a layer, so it is not in the platform's layer list.
resource "azurerm_storage_container" "drop" {
  name                  = "drop"
  storage_account_id    = azurerm_storage_account.env.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "layer" {
  for_each              = toset(var.layers)
  name                  = each.key
  storage_account_id    = azurerm_storage_account.env.id
  container_access_type = "private"
}

resource "azurerm_storage_account" "personal" {
  name                            = local.personal_name
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_kind                    = "StorageV2"
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = var.shared_access_key_enabled
  tags                            = merge(var.tags, { data_class = "personal" })
}

resource "azurerm_storage_container" "personal_landing" {
  name                  = "landing"
  storage_account_id    = azurerm_storage_account.personal.id
  container_access_type = "private"
}

resource "azurerm_storage_management_policy" "personal_purge" {
  storage_account_id = azurerm_storage_account.personal.id
  rule {
    name    = "purge-personal-landing"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["landing/"]
    }
    actions {
      base_blob {
        delete_after_days_since_modification_greater_than = var.personal_purge_days
      }
    }
  }
}

output "account_name" { value = azurerm_storage_account.env.name }
output "account_id" { value = azurerm_storage_account.env.id }
output "personal_account_name" { value = azurerm_storage_account.personal.name }
output "containers" { value = { for l, c in azurerm_storage_container.layer : l => c.name } }
output "layer_urls" { value = { for l, c in azurerm_storage_container.layer : l => "abfss://${c.name}@${azurerm_storage_account.env.name}.dfs.core.windows.net/" } }
