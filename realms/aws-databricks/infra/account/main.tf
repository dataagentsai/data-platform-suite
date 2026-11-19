# Once per AWS account and Databricks account (not per environment):
#   * GitHub Actions OIDC: CI gets short-lived AWS credentials by role, never stored keys.
#     - <prefix>-ci-plan    read-only, trusted by pull requests (terraform plan)
#     - <prefix>-ci-dev     deploys dev, trusted by the GitHub environment "dev"
#     - <prefix>-ci-prod    deploys prod, trusted by the GitHub environment "prod" (with required reviewers = release gate)
#   * Cost allocation tags switched on for billing (env, project, cost_center).
#   * The regional Unity Catalog metastore shared by the dev and prod workspaces. It has no root storage: every
#     catalog brings its own location (ADR-012), so dev and prod data never share a bucket.
#
#   terraform init -backend-config=../envs/account.backend.hcl
#   terraform apply -var-file=../envs/account.tfvars

terraform {
  required_version = ">= 1.10"
  required_providers {
    aws        = { source = "hashicorp/aws", version = "~> 6.0" }
    databricks = { source = "databricks/databricks", version = ">= 1.85, < 2.0" }
  }
  backend "s3" {}
}

variable "name_prefix" { type = string }

variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "github_repo" {
  description = "owner/name of the repository whose workflows may assume the CI roles"
  type        = string
  default     = "dataagentsai/data-platform-suite"
}

variable "databricks_account_id" { type = string }

variable "metastore_owner" {
  description = "Account group or service principal that owns the metastore (not a person)."
  type        = string
  default     = "platform-admins"
}

variable "activate_cost_tags" {
  description = "The tags appear in billing only after AWS has seen them on a resource (up to 24 hours after the first apply of an environment); run again with true then."
  type        = bool
  default     = false
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = { project = var.name_prefix, env = "shared", managed_by = "terraform" }
  }
}

provider "databricks" {
  alias      = "account"
  host       = "https://accounts.cloud.databricks.com"
  account_id = var.databricks_account_id
}

# --- GitHub OIDC (modules/github_ci) ---------------------------------------------------------------------------
module "github_ci" {
  source      = "../modules/github_ci"
  name_prefix = var.name_prefix
  github_repo = var.github_repo
}

# --- cost allocation tags ---------------------------------------------------------------------------------------
resource "aws_ce_cost_allocation_tag" "this" {
  for_each = var.activate_cost_tags ? toset(["env", "project", "cost_center"]) : toset([])
  tag_key  = each.key
  status   = "Active"
}

# --- Unity Catalog metastore for the region ----------------------------------------------------------------------
resource "databricks_metastore" "this" {
  provider      = databricks.account
  name          = "${var.name_prefix}-${var.aws_region}"
  region        = var.aws_region
  owner         = var.metastore_owner
  force_destroy = false
}

output "metastore_id" { value = databricks_metastore.this.id }
output "ci_role_subjects" {
  description = "Which GitHub token subject may assume each CI role."
  value       = module.github_ci.role_subjects
}

output "ci_role_arns" { value = module.github_ci.role_arns }
