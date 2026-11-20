# Provider versions are pinned exactly, each at a release more than two weeks old on 19 Nov 2026
# (azurerm 5.12.0 of 29 Oct 2026, databricks 1.141.0 of 2 Nov 2026; dates from the registry's version list).
terraform {
  required_version = ">= 1.10"
  required_providers {
    azurerm    = { source = "hashicorp/azurerm", version = "= 5.12.0" }
    databricks = { source = "databricks/databricks", version = "= 1.141.0" }
  }
  # Remote state in an Azure storage account. Everything (account, container, key) comes from
  # ../envs/<env>.backend.hcl; the account is made once by `make bootstrap`. Authentication is Entra ID
  # (use_azuread_auth), never an account key.
  backend "azurerm" {}
}
