# Unity Catalog for one environment on Databricks Free Edition (ADR-012 names, Free Edition storage).
#
# Same names as modules/unity_catalog (catalogs <env>_<layer>, landing volume <env>_landing.deliveries.files), so the
# generated bundle and the tasks need no second layout. What differs: Free Edition has no account console, so there is
# no metastore assignment, no storage credential, no external location and no workspace binding; every catalog lives
# in the workspace's default storage, not in the environment's S3 buckets. The S3 landing bucket is reached by the
# movers (boto3), not by Unity Catalog.
#
# Catalogs are made with SQL (scripts/uc_sql.sh) because the REST create needs a storage root Free Edition does not
# have; schemas and the volume then use the provider as usual.

terraform {
  required_providers {
    databricks = { source = "databricks/databricks" }
  }
}

variable "env" { type = string }

variable "catalog_layers" {
  type    = list(string)
  default = ["landing", "bronze", "silver", "gold", "ops"]
}

variable "schemas" {
  description = "layer => schemas in that layer's catalog (ADR-012: one per source in bronze and silver, per domain in gold)"
  type        = map(list(string))
}

variable "warehouse_id" {
  description = "SQL warehouse that runs CREATE CATALOG (Free Edition: the serverless starter warehouse)"
  type        = string
}

variable "force_destroy" {
  description = "dev: destroy drops the catalogs with everything in them. prod: false (destroy refuses a non-empty catalog)."
  type        = bool
  default     = false
}

locals {
  script   = abspath("${path.module}/../../scripts/uc_sql.sh")
  catalogs = { for l in var.catalog_layers : l => "${var.env}_${l}" }
  schema_rows = merge([for layer, names in var.schemas : {
    for s in names : "${layer}.${s}" => { catalog = local.catalogs[layer], name = s, layer = layer }
  }]...)
}

resource "terraform_data" "catalog" {
  for_each = local.catalogs
  input = {
    name    = each.value
    layer   = each.key
    wh      = var.warehouse_id
    script  = local.script
    cascade = var.force_destroy ? " CASCADE" : ""
  }

  provisioner "local-exec" {
    command = "bash '${self.input.script}' '${self.input.wh}' \"CREATE CATALOG IF NOT EXISTS ${self.input.name} COMMENT '${self.input.layer} layer, ${var.env} (ADR-012), managed by Terraform'\""
  }

  provisioner "local-exec" {
    when    = destroy
    command = "bash '${self.input.script}' '${self.input.wh}' 'DROP CATALOG IF EXISTS ${self.input.name}${self.input.cascade}'"
  }
}

resource "databricks_schema" "this" {
  for_each      = local.schema_rows
  catalog_name  = terraform_data.catalog[each.value.layer].output.name
  name          = each.value.name
  comment       = "${each.value.layer} layer, ${each.value.name}, ${var.env}"
  force_destroy = var.force_destroy
}

# The landing volume the lander reads (managed: Free Edition storage; the S3 landing bucket is the movers' side).
resource "databricks_volume" "files" {
  name         = "files"
  catalog_name = databricks_schema.this["landing.deliveries"].catalog_name
  schema_name  = databricks_schema.this["landing.deliveries"].name
  volume_type  = "MANAGED"
  comment      = "Deliveries exactly as received; one folder per source, dataset and window"
}

# Outputs from the names, not the resources, so they are known at plan time (the mocked plan test checks them).
output "catalogs" {
  value      = local.catalogs
  depends_on = [terraform_data.catalog]
}

output "schemas" { value = sort(keys(local.schema_rows)) }

output "landing_volume_path" {
  value      = "/Volumes/${local.catalogs["landing"]}/deliveries/files"
  depends_on = [databricks_volume.files]
}
