# GitHub Actions OIDC for one AWS account: CI gets short-lived AWS credentials by role, never stored keys.
#   <prefix>-ci-plan    read-only, trusted by pull requests (terraform plan)
#   <prefix>-ci-<env>   deploys <env>, trusted by the GitHub environment of that name (prod: required reviewers =
#                       the release gate)
# Used by infra/account (Databricks on AWS) and infra/free-edition/account (AWS account + Databricks Free Edition).

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }

variable "github_repo" {
  description = "owner/name of the repository whose workflows may assume the CI roles"
  type        = string
}

variable "environments" {
  description = "GitHub environments that get a deploy role"
  type        = list(string)
  default     = ["dev", "prod"]
}

data "aws_caller_identity" "current" {}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  oidc = aws_iam_openid_connect_provider.github.arn
  ci_roles = merge(
    { plan = "repo:${var.github_repo}:pull_request" },
    { for e in var.environments : e => "repo:${var.github_repo}:environment:${e}" },
  )
}

resource "aws_iam_role" "ci" {
  for_each             = local.ci_roles
  name                 = "${var.name_prefix}-ci-${each.key}"
  max_session_duration = 3600
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = local.oidc }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          "token.actions.githubusercontent.com:sub" = each.value
        }
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "plan_read_only" {
  role       = aws_iam_role.ci["plan"].name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}

# Deploy roles: power user for the services, plus IAM only on this project's own roles and policies.
resource "aws_iam_role_policy_attachment" "deploy_power_user" {
  for_each   = toset(var.environments)
  role       = aws_iam_role.ci[each.key].name
  policy_arn = "arn:aws:iam::aws:policy/PowerUserAccess"
}

resource "aws_iam_role_policy" "deploy_iam" {
  for_each = toset(var.environments)
  name     = "own-roles-only"
  role     = aws_iam_role.ci[each.key].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["iam:*Role*", "iam:*RolePolicy*", "iam:PassRole", "iam:GetOpenIDConnectProvider"]
      Resource = [
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.name_prefix}-${each.key}-*",
        "arn:aws:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/*",
      ]
    }]
  })
}

output "role_subjects" {
  description = "Which GitHub token subject may assume each CI role."
  value       = local.ci_roles
}

output "role_arns" { value = { for k, r in aws_iam_role.ci : k => r.arn } }
output "oidc_provider_arn" { value = local.oidc }
