# Plan against mocked providers: no AWS or Databricks account, no credentials, nothing created.
# Run once per environment (the Makefile and CI do both):
#   terraform test -var-file=../envs/dev.tfvars
#   terraform test -var-file=../envs/prod.tfvars

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
  mock_data "aws_region" {
    defaults = { region = "us-east-2" }
  }
}

mock_provider "databricks" {
  alias = "account"
  mock_data "databricks_aws_assume_role_policy" {
    defaults = { json = "{}" }
  }
  mock_data "databricks_aws_crossaccount_policy" {
    defaults = { json = "{}" }
  }
  mock_data "databricks_aws_bucket_policy" {
    defaults = { json = "{}" }
  }
  mock_resource "databricks_mws_workspaces" {
    defaults = { workspace_id = 1234567890, workspace_url = "https://mock.cloud.databricks.com" }
  }
}

mock_provider "databricks" {
  alias = "workspace"
  mock_data "databricks_aws_unity_catalog_assume_role_policy" {
    defaults = { json = "{}" }
  }
}

run "plan" {
  command = plan

  assert {
    condition     = length(module.storage.buckets) == length(var.layers)
    error_message = "one bucket per layer"
  }

  assert {
    condition     = alltrue([for l, b in module.storage.buckets : b.name == "${var.name_prefix}-${var.env}-${l}"])
    error_message = "bucket names must follow <prefix>-<env>-<layer> (dataplatform.adapters.s3.bucket_name)"
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
    condition     = (var.lakebase_enabled ? module.lakebase.instance_name == "${var.name_prefix}-${var.env}-meta" : module.lakebase.instance_name == null)
    error_message = "one metadata store per environment"
  }

  assert {
    condition     = length(module.network.subnet_ids) == 2
    error_message = "Databricks needs subnets in two availability zones"
  }
}
