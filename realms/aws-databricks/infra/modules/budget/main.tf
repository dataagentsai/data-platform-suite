# A monthly AWS budget per environment, filtered by the env cost-allocation tag (COST-02). The tag is switched on
# for billing once per account in infra/account.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "limit_usd" { type = number }

variable "emails" {
  description = "Who hears at 80% forecast and 100% actual. Empty: the budget exists, nobody is told."
  type        = list(string)
  default     = []
}

resource "aws_budgets_budget" "env" {
  name         = "${var.name_prefix}-${var.env}"
  budget_type  = "COST"
  limit_amount = tostring(var.limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:env$${var.env}"]
  }

  dynamic "notification" {
    for_each = length(var.emails) > 0 ? [["FORECASTED", 80], ["ACTUAL", 100]] : []
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value[1]
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value[0]
      subscriber_email_addresses = var.emails
    }
  }
}
