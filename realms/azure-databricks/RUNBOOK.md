# Runbook: from `az login` to the first 3-day run, and back to nothing

For the owner. Every step marked **[creates cost]** makes billable resources. Run everything from the repository root
of the worktree you deploy from. Nothing here has been run yet.

## 0. The one thing only you can do (the only blocker found)

The subscription is a **Free Trial and is Disabled** (a read-only `az rest` call on 19 Nov 2026 returned state
`Disabled`, offer `FreeTrial_2014-09-01`). Azure creates nothing in a disabled subscription. In the Azure portal open
Subscriptions, choose the subscription, and **upgrade to pay-as-you-go** (the free credit that remains is kept). This
is the only blocker I found: the code, names and providers are otherwise ready. Check afterwards:

```
az rest --method get --url "https://management.azure.com/subscriptions?api-version=2022-12-01" --query "value[].state" -o tsv
```

It must say `Enabled`.

## 1. Sign in and keep the subscription id out of files

```
az login
az account set --subscription "<name or id of the upgraded subscription>"
export TF_VAR_subscription_id=$(az account show --query id -o tsv)
export ARM_SUBSCRIPTION_ID=$TF_VAR_subscription_id
```

## 2. Register the resource providers (one time; free)

State on 19 Nov 2026 (read-only `az provider show`): Microsoft.Databricks **NotRegistered**, Microsoft.Storage
**NotRegistered**, Microsoft.KeyVault Registered, Microsoft.ManagedIdentity Registered, Microsoft.Authorization
Registered, Microsoft.Consumption Registered, Microsoft.CostManagement Registered, Microsoft.Compute Registered,
Microsoft.Network Registered. Register the two that are not (the others are listed so you can re-check after the
upgrade; registration is per subscription and a disabled one could not be changed):

```
for ns in Microsoft.Databricks Microsoft.Storage Microsoft.KeyVault Microsoft.ManagedIdentity Microsoft.Authorization Microsoft.Consumption Microsoft.CostManagement; do az provider register --namespace $ns; done
az provider show -n Microsoft.Databricks --query registrationState -o tsv     # repeat until Registered
```

## 3. Settings

```
cd infra/envs && cp dev.tfvars.example dev.tfvars && cp dev.backend.hcl.example dev.backend.hcl && cd ../..
```

Edit `dev.tfvars` (never committed): `name_prefix` (short, lowercase letters and digits, unique to you: storage account
and Key Vault names are global), `location` (keep `eastus2` unless you checked another region offers Lakebase),
`budget_emails = ["you@..."]`. Edit `dev.backend.hcl`: the state resource group and a globally unique state account
name (3 to 24 lowercase letters and digits). Do the same for prod only when prod is wanted.

## 4. State storage [creates cost: a few cents a month]

```
make -C realms/azure-databricks bootstrap
```

## 5. Plan, then deploy dev [creates cost]

```
make -C realms/azure-databricks plan ENV=dev        # read-only; read it: about 30 resources
make -C realms/azure-databricks infra ENV=dev       # asks for a yes
```

Expected failures on a first apply, and the answer: a name already taken (change `name_prefix`); a role assignment not
yet visible when the storage credential is validated (run `infra` again); Lakebase not offered in the region (set
`lakebase_enabled = false` and see "not built" in the README, or choose another region).

## 6. Databricks administrator (only if step 5 stops at Unity Catalog)

Terraform signs in to the new workspace as you. Creating a storage credential and catalogs needs you to be a
metastore administrator. If the error says permission denied or no metastore: sign in once to
`https://accounts.azuredatabricks.net` as a Global Administrator of the tenant (this makes you an account admin),
open Catalog, and confirm a metastore exists for the region and this workspace is attached (if no metastore exists,
create one with no storage root), then run `infra` again.

## 7. Deploy and run the engine job [creates cost]

```
make -C realms/azure-databricks bundle ENV=dev      # copies the tyre catalog slice, then databricks bundle deploy -t azure-dev
make -C realms/azure-databricks run ENV=dev         # the job dp-dev-engine-days: 3 compressed days, Iceberg tables in dev_<layer>
```

(`make bundle` and `run` take the workspace from `terraform output workspace_url`; set `DATABRICKS_HOST` to override.)
The run copies `ops.html` and `report.json` to `/Volumes/dev_ops/reports/runs/<run id>/` in the workspace. The proof
passes if the job ends successfully and the report shows the world's known answers met; the first run on a new cloud
may fail in the engine's Lakebase connection (the adapter has only been run on Free Edition): copy the failing task's
output and fix that in the engine, not here.

Set the Key Vault secret values you need by hand: `az keyvault secret set --vault-name <prefix>-dev-kv --name <a--b> --value ...`
(a reference `secret://a/b` is stored as `a--b`).

## 8. Tear everything down [stops the cost]

```
make -C realms/azure-databricks destroy ENV=dev     # bundle destroy, then terraform destroy (asks for a yes)
```

Then remove what Terraform does not own: the state account (`az group delete --name <state resource group>`); a
soft-deleted Key Vault keeps its name for 7 days (`az keyvault purge --name <prefix>-dev-kv`, not possible when purge
protection is on, which is the prod default); check that the Lakebase project is gone in the workspace's Lakebase page
(the project is purged on destroy only if the provider asks; if the workspace is gone it goes with it). Last, confirm
in Cost Management that spend has stopped. Deleting the resource group `<prefix>-dev-rg` by hand also works if
Terraform cannot (`az group delete`), because everything the environment owns lives in it (the workspace's managed
resource group is removed with the workspace).
