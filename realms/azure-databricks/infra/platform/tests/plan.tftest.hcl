# Plan against mocked providers: no Azure subscription, no Databricks workspace, no credentials, nothing created.
# Run once per environment (the Makefile does both):
#   terraform test -var-file=../envs/dev.tfvars.example
#   terraform test -var-file=../envs/prod.tfvars.example

variables {
  subscription_id = "00000000-0000-0000-0000-000000000000" # a mock; the real one comes from TF_VAR_subscription_id
}

mock_provider "azurerm" {
  mock_data "azurerm_client_config" {
    defaults = {
      tenant_id       = "00000000-0000-0000-0000-000000000001"
      object_id       = "00000000-0000-0000-0000-000000000002"
      subscription_id = "00000000-0000-0000-0000-000000000000"
    }
  }
  mock_resource "azurerm_resource_group" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mock-rg" }
  }
  mock_resource "azurerm_storage_account" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mock-rg/providers/Microsoft.Storage/storageAccounts/mock" }
  }
  mock_resource "azurerm_databricks_access_connector" {
    defaults = {
      id       = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mock-rg/providers/Microsoft.Databricks/accessConnectors/mock"
      identity = [{ type = "SystemAssigned", principal_id = "00000000-0000-0000-0000-000000000003", tenant_id = "00000000-0000-0000-0000-000000000001", identity_ids = [] }]
    }
  }
  mock_resource "azurerm_databricks_workspace" {
    defaults = {
      id            = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mock-rg/providers/Microsoft.Databricks/workspaces/mock"
      workspace_id  = "1234567890"
      workspace_url = "adb-1.1.azuredatabricks.net"
    }
  }
  mock_resource "azurerm_key_vault" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/mock-rg/providers/Microsoft.KeyVault/vaults/mock" }
  }
}

mock_provider "databricks" {}

run "plan" {
  command = plan

  assert {
    condition     = length(module.storage.containers) == length(var.layers) && alltrue([for l in var.layers : module.storage.containers[l] == l])
    error_message = "one container per layer, named exactly by the layer"
  }

  assert {
    condition     = module.storage.account_name == "${var.name_prefix}${var.env}" && length(module.storage.account_name) <= 24
    error_message = "the storage account is <prefix><env> (dataplatform.adapters.azure_blob.account_name)"
  }

  assert {
    condition     = module.storage.personal_account_name != module.storage.account_name
    error_message = "personal data lives in its own storage account"
  }

  assert {
    condition     = alltrue([for l in var.catalog_layers : module.unity_catalog.catalogs[l] == "${var.env}_${l}"])
    error_message = "catalogs must be <env>_<layer> (ADR-012)"
  }

  assert {
    condition     = alltrue([for c in module.unity_catalog.catalogs : startswith(c, "${var.env}_")])
    error_message = "an environment must not create another environment's catalogs"
  }

  assert {
    condition     = alltrue([for l in var.catalog_layers : contains(var.layers, l)])
    error_message = "every catalog needs a container behind it"
  }

  assert {
    condition     = module.resource_group.name == "${var.name_prefix}-${var.env}-rg" && module.key_vault.name == "${var.name_prefix}-${var.env}-kv"
    error_message = "resource group and key vault are per environment"
  }

  assert {
    condition     = (var.lakebase_enabled ? module.lakebase.project_name == "dp-meta-${var.env}" : module.lakebase.project_name == null)
    error_message = "one metadata store per environment, named as the engine bundle expects"
  }

  assert {
    condition     = module.budget.name == "${var.name_prefix}-${var.env}" && var.budget_usd > 0 && module.budget.notification_count == 2
    error_message = "a budget exists for every environment"
  }
}

# The alert exists once someone is named to hear it. The address is built in the test so that no email is written in
# any file of the repository.
run "budget_alerts_when_emails_given" {
  command = plan
  variables {
    budget_emails = [join("@", ["owner", "example.invalid"])]
  }

  assert {
    condition     = module.budget.notification_count == 2 && module.budget.email_count == 1
    error_message = "80% actual and 100% forecast notifications, to the address given"
  }
}

# Names the cloud would refuse are refused at plan time, before anything is asked of it.
run "refuses_uppercase_prefix" {
  command = plan
  variables {
    name_prefix = "Acme"
  }
  expect_failures = [var.name_prefix]
}

run "refuses_hyphen_in_prefix" {
  command = plan
  variables {
    name_prefix = "acme-dp"
  }
  expect_failures = [var.name_prefix]
}

run "refuses_long_prefix" {
  command = plan
  variables {
    name_prefix = "abcdefghijklmnopqrstuvwxy"
  }
  expect_failures = [var.name_prefix]
}

run "refuses_unknown_env" {
  command = plan
  variables {
    env = "staging"
  }
  expect_failures = [var.env]
}
