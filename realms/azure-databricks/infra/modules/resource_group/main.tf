# One resource group per environment: everything the environment owns lives in it, so the budget, the role
# assignments and `terraform destroy` all have one boundary.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "location" { type = string }
variable "tags" { type = map(string) }

resource "azurerm_resource_group" "env" {
  name     = "${var.name_prefix}-${var.env}-rg"
  location = var.location
  tags     = var.tags
}

output "name" { value = azurerm_resource_group.env.name }
output "id" { value = azurerm_resource_group.env.id }
output "location" { value = azurerm_resource_group.env.location }
