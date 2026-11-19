output "catalogs" { value = module.catalogs.catalogs }
output "schemas" { value = module.catalogs.schemas }
output "landing_volume_path" { value = module.catalogs.landing_volume_path }
output "metadata_store" {
  description = "Where DP_META_DSN points (the password is a short-lived OAuth token, never stored)."
  value = {
    instance = local.meta_instance
    host     = local.meta_host
    database = var.meta_database
  }
}
