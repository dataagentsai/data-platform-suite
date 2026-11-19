output "buckets" { value = { for l, b in module.storage.buckets : l => b.name } }
output "streams" { value = module.stream.arns }
output "secrets" { value = module.secrets.arns }
output "alerts_topic_arn" { value = module.alerts.topic_arn }
output "log_group" { value = module.alerts.log_group }
output "ci_role_arns" { value = var.manage_ci_roles ? module.github_ci[0].role_arns : {} }
