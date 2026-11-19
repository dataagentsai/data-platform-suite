# Kinesis Data Streams for the stream_bus port (machines and sensors). Provisioned with the fewest shards, because
# on-demand mode has a higher floor price. One shard costs ~11 USD a month; retention past 24 hours costs extra.
# Stream names follow <name_prefix>-<env>-<stream>; dataplatform.adapters.kinesis.KinesisStream(name_prefix=...)
# uses the same rule.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }

variable "streams" {
  description = "stream name => shard count"
  type        = map(number)
  default     = {}
}

variable "retention_hours" {
  description = "Longer than the longest outage the platform promises to recover from (ORCH-06)."
  type        = number
  default     = 24
}

variable "kms_key_arn" {
  description = "Customer-managed key; null uses the AWS-managed alias/aws/kinesis key (no key fee)."
  type        = string
  default     = null
}

resource "aws_kinesis_stream" "this" {
  for_each         = var.streams
  name             = "${var.name_prefix}-${var.env}-${each.key}"
  shard_count      = each.value
  retention_period = var.retention_hours
  encryption_type  = "KMS"
  kms_key_id       = coalesce(var.kms_key_arn, "alias/aws/kinesis")

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

output "arns" {
  value = { for k, s in aws_kinesis_stream.this : k => s.arn }
}
