output "workspace_url" { value = module.workspace.workspace_url }
output "workspace_id" { value = module.workspace.workspace_id }
output "buckets" { value = { for l, b in module.storage.buckets : l => b.name } }
output "catalogs" { value = module.unity_catalog.catalogs }
output "landing_volume_path" { value = module.unity_catalog.landing_volume_path }
output "metadata_store" { value = module.lakebase.instance_name }
output "secrets" { value = module.secrets.arns }
output "streams" { value = module.stream.arns }
