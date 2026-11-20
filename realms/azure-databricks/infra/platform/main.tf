# One environment of the platform: Databricks on Azure (stack dbs/stacks/azure-databricks.yaml).
# The same root runs for dev and prod; only ../envs/<env>.tfvars and ../envs/<env>.backend.hcl differ, and each has
# its own state, so dev and prod are separate resource groups, storage accounts, key vaults, workspaces and catalogs
# in one subscription to start.
#
#   az login                                   (then: export TF_VAR_subscription_id=...; never written in a file)
#   terraform init -backend-config=../envs/dev.backend.hcl
#   terraform plan -var-file=../envs/dev.tfvars
#
# Order (decision infra_provisioner: azurerm first, databricks after): resource group, storage, access connector,
# Key Vault, workspace; then Unity Catalog and Lakebase inside the workspace. Every Azure resource carries the tags
# below, so the bill splits by environment and cost centre.

locals {
  tags = merge({
    project     = var.name_prefix
    env         = var.env
    cost_center = var.cost_center
    managed_by  = "terraform"
  }, var.extra_tags)
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id
  storage_use_azuread = true
}

# Workspace-level API of the workspace created below, authenticated as the signed-in Azure identity (az login).
provider "databricks" {
  host                        = module.workspace.workspace_url
  azure_workspace_resource_id = module.workspace.id
}

data "azurerm_client_config" "current" {}

module "resource_group" {
  source      = "../modules/resource_group"
  name_prefix = var.name_prefix
  env         = var.env
  location    = var.location
  tags        = local.tags
}

module "storage" {
  source                    = "../modules/storage"
  name_prefix               = var.name_prefix
  env                       = var.env
  location                  = var.location
  resource_group_name       = module.resource_group.name
  tags                      = local.tags
  layers                    = var.layers
  replication               = var.storage_replication
  shared_access_key_enabled = var.shared_access_key_enabled
  force_destroy             = var.force_destroy
}

module "access_connector" {
  source              = "../modules/access_connector"
  name_prefix         = var.name_prefix
  env                 = var.env
  location            = var.location
  resource_group_name = module.resource_group.name
  storage_account_id  = module.storage.account_id
  tags                = local.tags
}

module "key_vault" {
  source                     = "../modules/key_vault"
  name_prefix                = var.name_prefix
  env                        = var.env
  location                   = var.location
  resource_group_name        = module.resource_group.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  tags                       = local.tags
  purge_protection           = var.key_vault_purge_protection
  secrets_officer_object_ids = [data.azurerm_client_config.current.object_id]
  secrets_reader_object_ids  = var.secrets_reader_object_ids
}

module "workspace" {
  source              = "../modules/workspace"
  name_prefix         = var.name_prefix
  env                 = var.env
  location            = var.location
  resource_group_name = module.resource_group.name
  tags                = local.tags
}

module "unity_catalog" {
  source              = "../modules/unity_catalog"
  env                 = var.env
  account_name        = module.storage.account_name
  workspace_id        = module.workspace.workspace_id
  access_connector_id = module.access_connector.id
  layers              = var.layers
  catalog_layers      = var.catalog_layers
  force_destroy       = var.force_destroy
  depends_on          = [module.access_connector]
}

module "lakebase" {
  source       = "../modules/lakebase"
  env          = var.env
  enabled      = var.lakebase_enabled
  project_name = var.lakebase_project_name
  depends_on   = [module.unity_catalog]
}

module "budget" {
  source            = "../modules/budget"
  name_prefix       = var.name_prefix
  env               = var.env
  resource_group_id = module.resource_group.id
  limit_usd         = var.budget_usd
  emails            = var.budget_emails
}
