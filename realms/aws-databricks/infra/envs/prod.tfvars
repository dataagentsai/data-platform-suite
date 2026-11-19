# prod: same shape as dev, safer settings.
env                   = "prod"
name_prefix           = "changeme-dp"
databricks_account_id = "00000000-0000-0000-0000-000000000000"
metastore_id          = "00000000-0000-0000-0000-000000000000"

# streams: real time is 7.2 press readings a second, far under one shard
vpc_cidr               = "10.30.0.0/20"
key_per_layer          = true # one KMS key per layer bucket (dbs/cloud/aws.yaml key_manager)
force_destroy          = false
expire_days            = { quarantine = 30 } # landing kept for replays (DUP-02)
streams                = { press_reading = 1 }
stream_retention_hours = 168 # 7 days (dbs/cloud/aws.yaml stream_bus retention_days)
lakebase_enabled       = true
budget_usd             = 75
