# dev: cheapest settings. Replace the changeme and zero values when the accounts exist (README, "When the accounts arrive").
env                   = "dev"
name_prefix           = "changeme-dp" # e.g. acme-dp; globally unique for S3 (README step 4)
databricks_account_id = "00000000-0000-0000-0000-000000000000"
metastore_id          = "00000000-0000-0000-0000-000000000000"

key_per_layer          = false # one KMS key for the environment
force_destroy          = true
expire_days            = { landing = 30, quarantine = 7 }
streams                = { press_reading = 2 } # the 7-day world run in ~40 min (README, Kinesis sizing)
stream_retention_hours = 24
lakebase_enabled       = true # set false if Lakebase is unavailable; the metadata store then binds to plain PostgreSQL
budget_usd             = 25
