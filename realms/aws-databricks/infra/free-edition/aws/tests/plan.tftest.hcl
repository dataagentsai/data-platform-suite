# Plan against a mocked AWS provider (no account, nothing created). Run per environment:
#   terraform test -var-file=../../envs/free-edition/dev.aws.tfvars

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
}

run "plan" {
  command = plan

  assert {
    condition     = alltrue([for l, b in module.storage.buckets : b.name == "${var.name_prefix}-${var.env}-${l}"])
    error_message = "bucket names must follow <prefix>-<env>-<layer> (dataplatform.adapters.s3.bucket_name)"
  }

  assert {
    condition     = alltrue([for b in module.storage.buckets : b.kms_key_arn == null])
    error_message = "Free Edition stack: SSE-S3, no customer-managed KMS key to pay for"
  }

  assert {
    condition     = length(module.stream.arns) == length(var.streams)
    error_message = "a Kinesis stream only when declared"
  }

  assert {
    condition     = length(module.github_ci) == (var.manage_ci_roles ? 1 : 0)
    error_message = "the GitHub OIDC provider and CI roles exist once per account"
  }
}
