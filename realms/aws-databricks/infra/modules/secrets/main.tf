# Secrets Manager entries for one environment (REL-05). Terraform creates the empty containers only; values are put
# by hand or by the source owner (aws secretsmanager put-secret-value), never written in code or state.
# Databricks reads them through a secret scope or the job's role; metadata stores only the secret name.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "names" { type = list(string) }
variable "kms_key_arn" {
  description = "Customer-managed key; null uses the AWS-managed aws/secretsmanager key."
  type        = string
  default     = null
}

variable "recovery_window_days" {
  description = "0 deletes at once (dev); 7 to 30 keeps a deleted secret recoverable (prod)."
  type        = number
  default     = 7
}

resource "aws_secretsmanager_secret" "this" {
  for_each                = toset(var.names)
  name                    = "${var.name_prefix}/${var.env}/${each.key}"
  description             = "${each.key} credentials for ${var.env}; value set outside Terraform"
  kms_key_id              = var.kms_key_arn
  recovery_window_in_days = var.recovery_window_days
}

output "arns" {
  value = { for k, s in aws_secretsmanager_secret.this : k => s.arn }
}
