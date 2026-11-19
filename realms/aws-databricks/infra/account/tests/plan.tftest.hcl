# Plan the once-per-account resources against mocked providers (no account needed).
#   terraform test -var-file=../envs/account.tfvars

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "123456789012" }
  }
}

mock_provider "databricks" {
  alias = "account"
}

run "plan" {
  command = plan

  assert {
    condition     = length(module.github_ci.role_arns) == 3
    error_message = "a plan role for pull requests and one deploy role per environment"
  }

  assert {
    condition     = output.ci_role_subjects["prod"] == "repo:${var.github_repo}:environment:prod"
    error_message = "only the prod GitHub environment (behind the release gate) may assume the prod deploy role"
  }
}
