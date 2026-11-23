# A monthly budget on the environment's resource group (cost allocation by resource group plus tags). A budget alerts;
# it does not stop spending. Emails come from a variable, never from a file in the repository.

terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }
variable "resource_group_id" { type = string }

variable "limit" {
  description = "Monthly amount in the subscription's billing currency."
  type        = number
}

variable "emails" {
  description = "Who hears at 80% actual and 100% forecast. Empty: the subscription owners (a role, not an address) are told."
  type        = list(string)
  default     = []
}

# Azure wants the first day of a month; the budget is created for the current month and runs on (ignore_changes).
locals {
  start = formatdate("YYYY-MM-01'T'00:00:00'Z'", timestamp())
}

resource "azurerm_consumption_budget_resource_group" "env" {
  name              = "${var.name_prefix}-${var.env}"
  resource_group_id = var.resource_group_id
  amount            = var.limit
  time_grain        = "Monthly"

  time_period {
    start_date = local.start
  }

  dynamic "notification" {
    for_each = [{ op = "GreaterThan", threshold = 80, kind = "Actual" }, { op = "GreaterThan", threshold = 100, kind = "Forecasted" }]
    content {
      enabled        = true
      operator       = notification.value.op
      threshold      = notification.value.threshold
      threshold_type = notification.value.kind
      contact_emails = var.emails
      contact_roles  = ["Owner"] # Azure needs someone named: the subscription owners hear it even with no email
    }
  }

  lifecycle {
    ignore_changes = [time_period]
  }
}

output "name" { value = azurerm_consumption_budget_resource_group.env.name }
output "notification_count" { value = 2 }
output "email_count" { value = length(var.emails) }
