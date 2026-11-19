# One environment of the platform on Databricks Free Edition (stack variant "AWS account + Databricks Free Edition").
# The engine-layer half; the cloud-layer half is ../aws. They are separate roots so either can be applied alone.
#
#   make -C realms/aws-databricks free-databricks ENV=<env>   (local state: ~/.dataplatform/tfstate/free-edition/<env>.databricks.tfstate,
#                                                     outside the repo so a removed worktree cannot lose it)
#
# Free Edition has no account console: no workspace, metastore, storage credential or external location is made here
# (those stay in ../../platform for Databricks on AWS). Auth: DATABRICKS_CONFIG_PROFILE locally (profile "free"),
# DATABRICKS_HOST + OIDC in CI.

terraform {
  required_version = ">= 1.10"
  required_providers {
    databricks = { source = "databricks/databricks", version = ">= 1.85, < 2.0" }
  }
  backend "local" {}
}

provider "databricks" {
  host = var.databricks_host
}

module "catalogs" {
  source         = "../../modules/free_edition_catalogs"
  env            = var.env
  catalog_layers = var.catalog_layers
  schemas        = var.schemas
  warehouse_id   = var.warehouse_id
  force_destroy  = var.force_destroy
}

# The metadata store (ADR-010) on Lakebase. dev: the instance already exists (made by hand while probing Free
# Edition), so it is only looked up, never owned: a destroy here cannot delete it. prod: lakebase_create = true makes
# a new instance (billed while running).
module "lakebase" {
  source      = "../../modules/lakebase"
  name_prefix = "dp"
  env         = var.env
  enabled     = var.lakebase_create
  capacity    = var.lakebase_capacity
}

data "databricks_database_instance" "meta" {
  count      = var.lakebase_create ? 0 : 1
  name       = var.lakebase_instance
  depends_on = [module.lakebase]
}

locals {
  meta_instance = var.lakebase_create ? module.lakebase.instance_name : data.databricks_database_instance.meta[0].name
  meta_host     = var.lakebase_create ? module.lakebase.read_write_dns : data.databricks_database_instance.meta[0].read_write_dns
}
