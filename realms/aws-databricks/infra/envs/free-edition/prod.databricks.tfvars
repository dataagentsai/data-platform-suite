# prod on Databricks Free Edition: declared, NOT deployed (DEPLOYED.md). Same shape as dev, safer settings.
# Same workspace (Free Edition has one), separate catalogs prod_<layer> and its own Lakebase instance.
env               = "prod"
databricks_host   = "https://dbc-00000000-0000.cloud.databricks.com"
warehouse_id      = "62a5ea0c0e6e5701"
force_destroy     = false
lakebase_create   = true # makes dp-prod-meta (CU_1, billed while running) -- the reason prod is not deployed yet
lakebase_capacity = "CU_1"
