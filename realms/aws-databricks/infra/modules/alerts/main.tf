# Alerts and logs for one environment, at idle cost ~0: an SNS topic with e-mail subscribers (the first 1,000 e-mails a
# month are free) and one CloudWatch log group with short retention (pay per GB ingested; nothing while idle).
# E-mail subscriptions stay "pending confirmation" until the recipient clicks the link AWS sends.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws" }
  }
}

variable "name_prefix" { type = string }
variable "env" { type = string }

variable "emails" {
  type    = list(string)
  default = []
}

variable "log_retention_days" {
  type    = number
  default = 7
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-${var.env}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.key
}

resource "aws_cloudwatch_log_group" "platform" {
  name              = "/${var.name_prefix}/${var.env}/platform"
  retention_in_days = var.log_retention_days
}

output "topic_arn" { value = aws_sns_topic.alerts.arn }
output "log_group" { value = aws_cloudwatch_log_group.platform.name }
