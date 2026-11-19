# dev on Databricks Free Edition (realms/aws-databricks/DEPLOYED.md). Not secrets: a workspace URL and a warehouse id.
env               = "dev"
databricks_host   = "https://dbc-00000000-0000.cloud.databricks.com"
warehouse_id      = "62a5ea0c0e6e5701" # Serverless Starter Warehouse, 2X-Small
force_destroy     = true               # destroy drops dev_* catalogs with their contents
lakebase_create   = false              # dp-meta-dev exists already; looked up, not owned
lakebase_instance = "dp-meta-dev"
