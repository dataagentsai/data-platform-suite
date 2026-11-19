# One environment of the platform: Databricks on AWS (stack dbs/stacks/aws-databricks.yaml).
# The same root runs for dev and prod; only ../envs/<env>.tfvars and ../envs/<env>.backend.hcl differ.
#
#   terraform init -backend-config=../envs/dev.backend.hcl
#   terraform plan -var-file=../envs/dev.tfvars
#
# Order (decision infra_provisioner: aws provider first, databricks provider after): network and storage, the
# workspace, then Unity Catalog and Lakebase inside it. Every AWS resource carries the default tags below, and the
# workspace tags the compute it launches, so the bill splits by environment and cost centre.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 6.0" }
    databricks = { source = "databricks/databricks", version = ">= 1.85, < 2.0" }
  }
  backend "s3" {} # bucket, key and region from ../envs/<env>.backend.hcl; S3-native locking (use_lockfile)
}

locals {
  tags = merge({
    project     = var.name_prefix
    env         = var.env
    cost_center = var.cost_center
    managed_by  = "terraform"
  }, var.extra_tags)
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = local.tags
  }
}

# Account-level Databricks API. Auth comes from the environment: DATABRICKS_AUTH_TYPE=github-oidc and
# DATABRICKS_CLIENT_ID in CI (no stored secret), or a CLI profile locally.
provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

# Workspace-level API of the workspace created below.
provider "databricks" {
  alias = "workspace"
  host  = module.workspace.workspace_url
}

data "aws_caller_identity" "current" {}

module "storage" {
  source        = "../modules/storage"
  name_prefix   = var.name_prefix
  env           = var.env
  layers        = var.layers
  key_per_layer = var.key_per_layer
  force_destroy = var.force_destroy
  expire_days   = var.expire_days
}

module "network" {
  source      = "../modules/network"
  name_prefix = var.name_prefix
  env         = var.env
  cidr        = var.vpc_cidr
  azs         = var.azs
  enable_nat  = var.enable_nat
}

module "secrets" {
  source               = "../modules/secrets"
  name_prefix          = var.name_prefix
  env                  = var.env
  names                = var.secret_names
  kms_key_arn          = module.storage.buckets["ops"].kms_key_arn
  recovery_window_days = var.force_destroy ? 0 : 7
}

module "stream" {
  source          = "../modules/stream"
  name_prefix     = var.name_prefix
  env             = var.env
  streams         = var.streams
  retention_hours = var.stream_retention_hours
  kms_key_arn     = module.storage.buckets["landing"].kms_key_arn
}

module "workspace" {
  source                = "../modules/workspace"
  providers             = { databricks.account = databricks.account }
  name_prefix           = var.name_prefix
  env                   = var.env
  aws_region            = var.aws_region
  databricks_account_id = var.databricks_account_id
  vpc_id                = module.network.vpc_id
  subnet_ids            = module.network.subnet_ids
  security_group_id     = module.network.security_group_id
  force_destroy         = var.force_destroy
  tags                  = local.tags
}

module "unity_catalog" {
  source = "../modules/unity_catalog"
  providers = {
    databricks.account   = databricks.account
    databricks.workspace = databricks.workspace
  }
  name_prefix    = var.name_prefix
  env            = var.env
  aws_account_id = data.aws_caller_identity.current.account_id
  metastore_id   = var.metastore_id
  workspace_id   = module.workspace.workspace_id
  buckets        = module.storage.buckets
  catalog_layers = var.catalog_layers
  force_destroy  = var.force_destroy
}

module "lakebase" {
  source      = "../modules/lakebase"
  providers   = { databricks = databricks.workspace }
  name_prefix = var.name_prefix
  env         = var.env
  enabled     = var.lakebase_enabled
  capacity    = var.lakebase_capacity
  depends_on  = [module.unity_catalog]
}

module "budget" {
  source      = "../modules/budget"
  name_prefix = var.name_prefix
  env         = var.env
  limit_usd   = var.budget_usd
  emails      = var.budget_emails
}
