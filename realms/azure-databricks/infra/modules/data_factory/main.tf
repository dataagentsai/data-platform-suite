# The managed copy service for the movers (ADR-033, binding `mover`): one factory per environment. It copies from a
# source into the landing container and writes the delivery's manifest last (the landing contract, DPC-0038). The
# factory's own identity is the only credential: it gets Storage Blob Data Contributor on the environment's storage
# account and signs in to it with no key. The pipelines are generated from the movers file (realms/azure-databricks/
# generate_adf.py) and deployed by `make adf-deploy`, so a mover is metadata, not a hand-built pipeline.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "storage_account_id" { type = string }
variable "storage_account_name" { type = string }
variable "tags" { type = map(string) }

variable "enabled" {
  description = "Create the factory. A factory costs nothing while idle; runs are billed per activity."
  type        = bool
  default     = true
}

resource "azurerm_data_factory" "env" {
  count               = var.enabled ? 1 : 0
  name                = "${var.name_prefix}-${var.env}-adf"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

# The factory writes landing and reads the file drop zone with its own identity.
resource "azurerm_role_assignment" "factory_blob" {
  count                = var.enabled ? 1 : 0
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.env[0].identity[0].principal_id
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "storage" {
  count                = var.enabled ? 1 : 0
  name                 = "ls_storage"
  data_factory_id      = azurerm_data_factory.env[0].id
  url                  = "https://${var.storage_account_name}.dfs.core.windows.net"
  use_managed_identity = true
}

output "name" { value = var.enabled ? azurerm_data_factory.env[0].name : "" }
output "id" { value = var.enabled ? azurerm_data_factory.env[0].id : "" }
output "principal_id" { value = var.enabled ? azurerm_data_factory.env[0].identity[0].principal_id : "" }
