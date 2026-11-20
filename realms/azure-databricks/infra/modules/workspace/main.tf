# The Azure Databricks workspace for one environment. Premium SKU: Unity Catalog, workspace bindings and Lakebase need
# it. Public network access stays on (virtual network injection and Private Link are in the spec and NOT BUILT here);
# the engine job runs on serverless compute, which needs no network of ours.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_databricks_workspace" "env" {
  name                        = "${var.name_prefix}-${var.env}-dbx"
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "${var.name_prefix}-${var.env}-dbx-managed-rg"
  tags                        = var.tags
}

output "id" { value = azurerm_databricks_workspace.env.id }
output "name" { value = azurerm_databricks_workspace.env.name }
output "workspace_url" { value = "https://${azurerm_databricks_workspace.env.workspace_url}" }
output "workspace_id" { value = azurerm_databricks_workspace.env.workspace_id }
