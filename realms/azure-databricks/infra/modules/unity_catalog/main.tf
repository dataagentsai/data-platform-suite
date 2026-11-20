# Unity Catalog for one environment (ADR-012), the Azure twin of the first cloud's module with the same names:
# a storage credential on the access connector's managed identity; one external location per layer container; one
# catalog per layer named <env>_<layer>, each bound to this environment's workspace only, so dev compute cannot reach
# prod tables; and the landing volume the lander reads deliveries through.
#
# The metastore is NOT made here. Azure Databricks attaches a workspace to the region's metastore on its own
# (assumption, see README "Unverified"); the metastore has no root storage, so every catalog brings its own location.

terraform {
  required_providers {
    databricks = { source = "databricks/databricks" }
  }
}

variable "env" { type = string }
variable "account_name" { type = string }
variable "workspace_id" { type = string }
variable "access_connector_id" { type = string }

variable "layers" {
  description = "Layers that have a container, so an external location."
  type        = list(string)
}

variable "catalog_layers" {
  description = "Layers that get a catalog <env>_<layer> (ADR-012). Must be a subset of layers."
  type        = list(string)
}

variable "landing_volume_prefix" {
  type    = string
  default = "files"
}

variable "force_destroy" {
  type    = bool
  default = false
}

locals {
  url = { for l in var.layers : l => "abfss://${l}@${var.account_name}.dfs.core.windows.net/" }
}

resource "databricks_storage_credential" "this" {
  name = "${var.env}_layers"
  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
  comment        = "All ${var.env} layer containers, managed by Terraform"
  isolation_mode = "ISOLATION_MODE_ISOLATED"
  force_destroy  = var.force_destroy
}

resource "databricks_external_location" "layer" {
  for_each        = local.url
  name            = "${var.env}_${each.key}"
  url             = each.value
  credential_name = databricks_storage_credential.this.id
  isolation_mode  = "ISOLATION_MODE_ISOLATED"
  force_destroy   = var.force_destroy
}

resource "databricks_catalog" "layer" {
  for_each       = toset(var.catalog_layers)
  name           = "${var.env}_${each.key}"
  storage_root   = "${local.url[each.key]}_managed/"
  isolation_mode = "ISOLATED"
  force_destroy  = var.force_destroy
  comment        = "${each.key} layer, ${var.env} (ADR-012)"
  properties     = { env = var.env, layer = each.key }
  depends_on     = [databricks_external_location.layer]
}

resource "databricks_workspace_binding" "catalog" {
  for_each       = databricks_catalog.layer
  securable_name = each.value.name
  securable_type = "catalog"
  workspace_id   = var.workspace_id
}

resource "databricks_workspace_binding" "external_location" {
  for_each       = databricks_external_location.layer
  securable_name = each.value.name
  securable_type = "external_location"
  workspace_id   = var.workspace_id
}

resource "databricks_workspace_binding" "credential" {
  securable_name = databricks_storage_credential.this.name
  securable_type = "storage_credential"
  workspace_id   = var.workspace_id
}

resource "databricks_schema" "deliveries" {
  catalog_name  = databricks_catalog.layer["landing"].name
  name          = "deliveries"
  comment       = "Deliveries exactly as received; one folder per source, dataset and window"
  force_destroy = var.force_destroy
}

resource "databricks_volume" "files" {
  name             = "files"
  catalog_name     = databricks_catalog.layer["landing"].name
  schema_name      = databricks_schema.deliveries.name
  volume_type      = "EXTERNAL"
  storage_location = "${local.url["landing"]}${var.landing_volume_prefix}/"
  comment          = "Read by the lander; written by the movers (Azure landing adapter, prefix ${var.landing_volume_prefix}/)"
  depends_on       = [databricks_external_location.layer]
}

output "catalogs" { value = { for l, c in databricks_catalog.layer : l => c.name } }
output "external_locations" { value = { for l, e in databricks_external_location.layer : l => e.url } }
output "landing_volume_path" {
  value = "/Volumes/${databricks_catalog.layer["landing"].name}/${databricks_schema.deliveries.name}/${databricks_volume.files.name}"
}
