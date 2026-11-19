# One S3 bucket per layer for one environment (ADR-012: one storage location and key per environment and layer).
# Bucket names follow <name_prefix>-<env>-<layer>; dataplatform.adapters.s3.bucket_name() uses the same rule.
# Every bucket: versioned (ORCH-05), KMS-encrypted (SEC-02), private, owner-enforced, tagged for cost.
# sse = "s3" (the Free Edition stack) swaps the customer-managed KMS keys for SSE-S3: still encrypted at rest, no key
# to pay for (~1 USD a key a month), but without a key policy separating who may decrypt.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "layers" { type = list(string) }

variable "key_per_layer" {
  description = "One customer-managed KMS key per layer (prod) or one per environment (dev, cheaper: each key costs ~1 USD a month)."
  type        = bool
}

variable "force_destroy" {
  description = "Allow destroy with objects inside (dev only)."
  type        = bool
  default     = false
}

variable "expire_days" {
  description = "Days after which current objects expire, per layer; layers not listed never expire."
  type        = map(number)
  default     = {}
}

variable "noncurrent_days" {
  description = "Days old versions are kept (recovery window)."
  type        = number
  default     = 30
}

variable "sse" {
  description = "kms: customer-managed KMS keys (SEC-02, the Databricks-on-AWS default). s3: SSE-S3, no key cost."
  type        = string
  default     = "kms"
  validation {
    condition     = contains(["kms", "s3"], var.sse)
    error_message = "sse is kms or s3."
  }
}

locals {
  key_names = var.sse != "kms" ? toset([]) : var.key_per_layer ? toset(var.layers) : toset(["all"])
  key_of    = { for l in var.layers : l => var.key_per_layer ? l : "all" }
}

resource "aws_kms_key" "this" {
  for_each                = local.key_names
  description             = "${var.name_prefix} ${var.env} ${each.key} data"
  enable_key_rotation     = true
  deletion_window_in_days = 7
  tags                    = { layer = each.key }
}

resource "aws_kms_alias" "this" {
  for_each      = local.key_names
  name          = "alias/${var.name_prefix}-${var.env}-${each.key}"
  target_key_id = aws_kms_key.this[each.key].key_id
}

resource "aws_s3_bucket" "layer" {
  for_each      = toset(var.layers)
  bucket        = "${var.name_prefix}-${var.env}-${each.key}"
  force_destroy = var.force_destroy
  tags          = { layer = each.key }
}

resource "aws_s3_bucket_versioning" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.sse == "kms" ? "aws:kms" : "AES256"
      kms_master_key_id = var.sse == "kms" ? aws_kms_key.this[local.key_of[each.key]].arn : null
    }
    bucket_key_enabled = var.sse == "kms" # fewer KMS calls, lower cost
  }
}

resource "aws_s3_bucket_public_access_block" "layer" {
  for_each                = aws_s3_bucket.layer
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "layer" {
  for_each = aws_s3_bucket.layer
  bucket   = each.value.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "layer" {
  for_each   = aws_s3_bucket.layer
  bucket     = each.value.id
  depends_on = [aws_s3_bucket_versioning.layer]

  rule {
    id     = "old-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_days
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }

  dynamic "rule" {
    for_each = contains(keys(var.expire_days), each.key) ? [var.expire_days[each.key]] : []
    content {
      id     = "expire"
      status = "Enabled"
      filter {}
      expiration {
        days = rule.value
      }
    }
  }
}

output "buckets" {
  description = "layer => {name, arn, kms_key_arn}; kms_key_arn is null with sse = s3"
  value = { for l, b in aws_s3_bucket.layer : l => {
    name        = b.bucket
    arn         = b.arn
    kms_key_arn = var.sse == "kms" ? aws_kms_key.this[local.key_of[l]].arn : null
  } }
}

output "kms_key_arns" {
  value = [for k in aws_kms_key.this : k.arn]
}
