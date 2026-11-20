# The Databricks access connector: a system-assigned managed identity that Unity Catalog uses to reach storage (no
# keys, no secrets). It is given Storage Blob Data Contributor on the environment's storage account only.

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
variable "tags" { type = map(string) }

resource "azurerm_databricks_access_connector" "env" {
  name                = "${var.name_prefix}-${var.env}-uc"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.env.identity[0].principal_id
}

output "id" { value = azurerm_databricks_access_connector.env.id }
output "principal_id" { value = azurerm_databricks_access_connector.env.identity[0].principal_id }
output "storage_role_assignment_id" { value = azurerm_role_assignment.storage.id }
