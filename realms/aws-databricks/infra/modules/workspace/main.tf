# One Databricks workspace for one environment, in the customer-managed VPC (modules/network).
# AWS side: the workspace root bucket and the cross-account role Databricks assumes to launch classic compute.
# Databricks side (account-level provider): credentials, storage configuration, network, workspace.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.account]
    }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "aws_region" { type = string }
variable "databricks_account_id" { type = string }
variable "vpc_id" { type = string }
variable "subnet_ids" { type = list(string) }
variable "security_group_id" { type = string }

variable "force_destroy" {
  type    = bool
  default = false
}

variable "tags" {
  description = "Custom tags Databricks puts on the compute it launches, so the bill splits by environment."
  type        = map(string)
  default     = {}
}

locals {
  name = "${var.name_prefix}-${var.env}"
}

# --- cross-account role -----------------------------------------------------------------------------------------
data "databricks_aws_assume_role_policy" "this" {
  provider    = databricks.account
  external_id = var.databricks_account_id
}

data "databricks_aws_crossaccount_policy" "this" {
  provider    = databricks.account
  policy_type = "customer" # customer-managed VPC: Databricks may not create or change networks
}

resource "aws_iam_role" "cross_account" {
  name               = "${local.name}-databricks-cross-account"
  assume_role_policy = data.databricks_aws_assume_role_policy.this.json
}

resource "aws_iam_role_policy" "cross_account" {
  name   = "databricks-cross-account"
  role   = aws_iam_role.cross_account.id
  policy = data.databricks_aws_crossaccount_policy.this.json
}

# --- workspace root bucket (workspace system files only; data lives in the layer buckets) --------------------------
resource "aws_s3_bucket" "root" {
  bucket        = "${local.name}-workspace-root"
  force_destroy = var.force_destroy
  tags          = { layer = "workspace-root" }
}

resource "aws_s3_bucket_public_access_block" "root" {
  bucket                  = aws_s3_bucket.root.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "root" {
  bucket = aws_s3_bucket.root.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

data "databricks_aws_bucket_policy" "root" {
  provider = databricks.account
  bucket   = aws_s3_bucket.root.bucket
}

resource "aws_s3_bucket_policy" "root" {
  bucket     = aws_s3_bucket.root.id
  policy     = data.databricks_aws_bucket_policy.root.json
  depends_on = [aws_s3_bucket_public_access_block.root]
}

# --- Databricks account objects ---------------------------------------------------------------------------------
resource "databricks_mws_credentials" "this" {
  provider         = databricks.account
  credentials_name = "${local.name}-credentials"
  role_arn         = aws_iam_role.cross_account.arn
  depends_on       = [aws_iam_role_policy.cross_account]
}

resource "databricks_mws_storage_configurations" "this" {
  provider                   = databricks.account
  account_id                 = var.databricks_account_id
  storage_configuration_name = "${local.name}-storage"
  bucket_name                = aws_s3_bucket.root.bucket
}

resource "databricks_mws_networks" "this" {
  provider           = databricks.account
  account_id         = var.databricks_account_id
  network_name       = "${local.name}-network"
  vpc_id             = var.vpc_id
  subnet_ids         = var.subnet_ids
  security_group_ids = [var.security_group_id]
}

resource "databricks_mws_workspaces" "this" {
  provider                 = databricks.account
  account_id               = var.databricks_account_id
  workspace_name           = local.name
  aws_region               = var.aws_region
  credentials_id           = databricks_mws_credentials.this.credentials_id
  storage_configuration_id = databricks_mws_storage_configurations.this.storage_configuration_id
  network_id               = databricks_mws_networks.this.network_id
  custom_tags              = var.tags
}

output "workspace_id" { value = databricks_mws_workspaces.this.workspace_id }
output "workspace_url" { value = databricks_mws_workspaces.this.workspace_url }
