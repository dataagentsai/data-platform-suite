# One environment of the platform, cloud-layer half, for the stack variant "AWS account + Databricks Free Edition".
# The engine-layer half is ../databricks; they are separate roots so either can be applied alone.
#
#   make -C realms/aws-databricks free-aws ENV=<env>   (local state: ~/.dataplatform/tfstate/free-edition/<env>.aws.tfstate,
#                                                     outside the repo so a removed worktree cannot lose it)
#
# Cheapest shape of W2's cloud layer (target: under 2 USD a month idle): S3 buckets per layer the movers use
# (landing, which also holds the world's drops under drops/, and quarantine) with SSE-S3 instead of KMS; no VPC and
# no NAT (Free Edition compute is serverless in Databricks' account); Kinesis declared but empty by default; no
# Secrets Manager entry by default (0.40 USD each); an SNS alert topic and one short-retention log group. The GitHub
# OIDC provider and CI roles are once per account, so only the dev tfvars sets manage_ci_roles = true.
# The account's monthly budget already exists (made by hand) and is not managed here.

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
  backend "local" {}
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

module "storage" {
  source        = "../../modules/storage"
  name_prefix   = var.name_prefix
  env           = var.env
  layers        = var.layers
  key_per_layer = false
  sse           = "s3"
  force_destroy = var.force_destroy
  expire_days   = var.expire_days
}

module "stream" {
  source          = "../../modules/stream"
  name_prefix     = var.name_prefix
  env             = var.env
  streams         = var.streams # {} = no stream, no shard-hours
  retention_hours = var.stream_retention_hours
}

module "secrets" {
  source               = "../../modules/secrets"
  name_prefix          = var.name_prefix
  env                  = var.env
  names                = var.secret_names # [] = none (each costs 0.40 USD a month)
  recovery_window_days = var.force_destroy ? 0 : 7
}

module "alerts" {
  source             = "../../modules/alerts"
  name_prefix        = var.name_prefix
  env                = var.env
  emails             = var.alert_emails
  log_retention_days = var.log_retention_days
}

module "github_ci" {
  count       = var.manage_ci_roles ? 1 : 0
  source      = "../../modules/github_ci"
  name_prefix = var.name_prefix
  github_repo = var.github_repo
}
