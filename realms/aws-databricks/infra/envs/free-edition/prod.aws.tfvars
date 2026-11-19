# prod, cloud-layer half of the Free Edition stack: declared, not deployed. Same shape as dev, safer settings.
env                = "prod"
name_prefix        = "dataagents-dp"
aws_region         = "us-east-2"
force_destroy      = false
expire_days        = { quarantine = 30 } # landing kept for replays (DUP-02)
streams            = {}
secret_names       = []
alert_emails       = ["alerts@example.com"]
log_retention_days = 14
manage_ci_roles    = false
