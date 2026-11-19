# Plan against a mocked Databricks provider (no workspace, nothing created). Run per environment:
#   terraform test -var-file=../../envs/free-edition/dev.databricks.tfvars

mock_provider "databricks" {
  mock_data "databricks_database_instance" {
    defaults = { name = "dp-meta-dev", read_write_dns = "mock.database.cloud.databricks.com" }
  }
}

run "plan" {
  command = plan

  assert {
    condition     = alltrue([for l in var.catalog_layers : module.catalogs.catalogs[l] == "${var.env}_${l}"])
    error_message = "catalogs must be <env>_<layer> (ADR-012), the names the generated bundle uses"
  }

  assert {
    condition     = module.catalogs.landing_volume_path == "/Volumes/${var.env}_landing/deliveries/files"
    error_message = "the landing volume must be where generate_bundle.volume() points"
  }

  assert {
    condition     = contains(module.catalogs.schemas, "silver.erp") && contains(module.catalogs.schemas, "bronze.press_stream")
    error_message = "one schema per manifest source in bronze and silver (ADR-012)"
  }
}
