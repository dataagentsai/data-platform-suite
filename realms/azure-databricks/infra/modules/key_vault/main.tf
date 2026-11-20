# One Key Vault per environment, role-based access only (no access policies). Names: 3 to 24 characters, letters,
# digits and hyphens, starting with a letter, no two hyphens in a row, not ending in a hyphen; global across Azure.
# Secret VALUES are put by hand (`az keyvault secret set`), never in code or state; the secret reference
# secret://a/b is held as a--b (dataplatform.adapters.secrets.key_vault_name).

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "location" { type = string }
variable "resource_group_name" { type = string }
variable "tenant_id" { type = string }
variable "tags" { type = map(string) }

variable "purge_protection" {
  description = "prod: true (a deleted vault cannot be purged for the retention period). dev: false so destroy and recreate work."
  type        = bool
  default     = false
}

variable "secrets_officer_object_ids" {
  description = "Entra object ids that may write secrets (the deploying identity is added by the root)."
  type        = list(string)
  default     = []
}

variable "secrets_reader_object_ids" {
  description = "Entra object ids that may read secrets (managed identities of the platform)."
  type        = list(string)
  default     = []
}

locals {
  name = "${var.name_prefix}-${var.env}-kv"
}

resource "azurerm_key_vault" "env" {
  name                       = local.name
  resource_group_name        = var.resource_group_name
  location                   = var.location
  tenant_id                  = var.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  soft_delete_retention_days = 7
  purge_protection_enabled   = var.purge_protection
  tags                       = var.tags
}

resource "azurerm_role_assignment" "officer" {
  for_each             = toset(var.secrets_officer_object_ids)
  scope                = azurerm_key_vault.env.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = each.key
}

resource "azurerm_role_assignment" "reader" {
  for_each             = toset(var.secrets_reader_object_ids)
  scope                = azurerm_key_vault.env.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = each.key
}

output "name" { value = azurerm_key_vault.env.name }
output "id" { value = azurerm_key_vault.env.id }
output "vault_uri" { value = azurerm_key_vault.env.vault_uri }
