# dev, cloud-layer half of the Free Edition stack. NOT APPLIED: the account's organization SCP denies S3, SNS,
# CloudWatch Logs, Kinesis, Secrets Manager and IAM OIDC listing (DEPLOYED.md). Plan/apply once it is lifted.
env                = "dev"
name_prefix        = "dataagents-dp" # buckets dataagents-dp-dev-landing / -quarantine
aws_region         = "us-east-2"
force_destroy      = true
expire_days        = { landing = 30, quarantine = 7 }
streams            = {} # Kinesis off; { press_reading = 2 } for a streaming demo (~0.78 USD a day)
secret_names       = []
alert_emails       = ["alerts@example.com"]
log_retention_days = 3
manage_ci_roles    = true # the account's one GitHub OIDC provider and the ci-plan/dev/prod roles live in dev's state
