# Unity Catalog for one environment (ADR-012): the workspace joins the regional metastore; one IAM role and storage
# credential reach this environment's buckets; one external location per bucket; one catalog per layer, named
# <env>_<layer> and bound to this environment's workspace only, so dev compute can never write prod tables.
# The landing catalog holds the volume the lander reads deliveries through (dbs/cloud/aws.yaml landing_store).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.account, databricks.workspace]
    }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "aws_account_id" { type = string }
variable "metastore_id" { type = string }
variable "workspace_id" { type = string }

variable "buckets" {
  description = "layer => {name, arn, kms_key_arn}, from modules/storage"
  type        = map(object({ name = string, arn = string, kms_key_arn = string }))
}

variable "catalog_layers" {
  description = "Layers that get a catalog (ADR-012). Other buckets get an external location only."
  type        = list(string)
}

variable "landing_volume_prefix" {
  description = "Folder in the landing bucket the landing volume covers; the S3 landing adapter writes under it."
  type        = string
  default     = "files"
}

variable "force_destroy" {
  type    = bool
  default = false
}

locals {
  role_name = "${var.name_prefix}-${var.env}-unity-catalog"
  role_arn  = "arn:aws:iam::${var.aws_account_id}:role/${local.role_name}"
}

resource "databricks_metastore_assignment" "this" {
  provider     = databricks.account
  metastore_id = var.metastore_id
  workspace_id = var.workspace_id
}

# --- storage credential and its IAM role -------------------------------------------------------------------------
# The credential is created first with the role's ARN; the role then trusts Databricks with the credential's
# external id (the order the provider documents for a self-assuming Unity Catalog role).
resource "databricks_storage_credential" "this" {
  provider = databricks.workspace
  name     = "${var.env}_layers"
  comment  = "All ${var.env} layer buckets, managed by Terraform"
  aws_iam_role {
    role_arn = local.role_arn
  }
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  depends_on     = [databricks_metastore_assignment.this]
}

data "databricks_aws_unity_catalog_assume_role_policy" "this" {
  provider       = databricks.workspace
  aws_account_id = var.aws_account_id
  role_name      = local.role_name
  external_id    = databricks_storage_credential.this.aws_iam_role[0].external_id
}

resource "aws_iam_role" "unity_catalog" {
  name               = local.role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.this.json
}

resource "aws_iam_role_policy" "unity_catalog" {
  name = "layer-buckets"
  role = aws_iam_role.unity_catalog.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Buckets"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
        Resource = flatten([for b in values(var.buckets) : [b.arn, "${b.arn}/*"]])
      },
      {
        Sid      = "Keys"
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey*"]
        Resource = distinct([for b in values(var.buckets) : b.kms_key_arn])
      },
      {
        Sid      = "SelfAssume"
        Effect   = "Allow"
        Action   = ["sts:AssumeRole"]
        Resource = [local.role_arn]
      },
    ]
  })
}

# --- external locations, one per bucket, usable from this workspace only ----------------------------------------
resource "databricks_external_location" "layer" {
  for_each        = var.buckets
  provider        = databricks.workspace
  name            = "${var.env}_${each.key}"
  url             = "s3://${each.value.name}/"
  credential_name = databricks_storage_credential.this.id
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  force_destroy   = var.force_destroy
  depends_on      = [aws_iam_role_policy.unity_catalog]
}

# --- catalogs <env>_<layer> -------------------------------------------------------------------------------------
resource "databricks_catalog" "layer" {
  for_each       = toset(var.catalog_layers)
  provider       = databricks.workspace
  name           = "${var.env}_${each.key}"
  storage_root   = "s3://${var.buckets[each.key].name}/_managed/"
  isolation_mode = "ISOLATED"
  force_destroy  = var.force_destroy
  comment        = "${each.key} layer, ${var.env} (ADR-012)"
  properties     = { env = var.env, layer = each.key }
  depends_on     = [databricks_external_location.layer]
}

resource "databricks_workspace_binding" "catalog" {
  for_each       = databricks_catalog.layer
  provider       = databricks.workspace
  securable_name = each.value.name
  securable_type = "catalog"
  workspace_id   = var.workspace_id
}

resource "databricks_workspace_binding" "external_location" {
  for_each       = databricks_external_location.layer
  provider       = databricks.workspace
  securable_name = each.value.name
  securable_type = "external_location"
  workspace_id   = var.workspace_id
}

resource "databricks_workspace_binding" "credential" {
  provider       = databricks.workspace
  securable_name = databricks_storage_credential.this.name
  securable_type = "storage_credential"
  workspace_id   = var.workspace_id
}

# --- the landing volume the lander reads --------------------------------------------------------------------------
resource "databricks_schema" "deliveries" {
  provider      = databricks.workspace
  catalog_name  = databricks_catalog.layer["landing"].name
  name          = "deliveries"
  comment       = "Deliveries exactly as received; one folder per source, dataset and window"
  force_destroy = var.force_destroy
}

resource "databricks_volume" "files" {
  provider         = databricks.workspace
  name             = "files"
  catalog_name     = databricks_catalog.layer["landing"].name
  schema_name      = databricks_schema.deliveries.name
  volume_type      = "EXTERNAL"
  storage_location = "s3://${var.buckets["landing"].name}/${var.landing_volume_prefix}/"
  comment          = "Read by the lander; written by the movers (S3 landing adapter, prefix ${var.landing_volume_prefix}/)"
}

output "catalogs" { value = { for l, c in databricks_catalog.layer : l => c.name } }
output "landing_volume_path" {
  value = "/Volumes/${databricks_catalog.layer["landing"].name}/${databricks_schema.deliveries.name}/${databricks_volume.files.name}"
}
output "storage_role_arn" { value = aws_iam_role.unity_catalog.arn }
