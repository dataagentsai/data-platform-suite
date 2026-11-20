# Databricks on Azure realm (second cloud, ADR-036, tasks AZ-5 and AZ-11)

The cloud layer `dbs/cloud/azure.yaml` under the Databricks engine layer, composed by the stack
`dbs/stacks/azure-databricks.yaml`. **Status: written and checked offline only.** Nothing here has been applied. The
owner's Azure subscription is a Free Trial that is currently **Disabled** (read back with a read-only `az rest` call on
19 Nov 2026), and every resource below costs money once it exists. The decision record ADR-036 stays `proposed` until
the 3-day compressed run has passed on a real subscription.

## What it deploys, per environment (dev and prod are two separate runs of the same code)

| Piece | Azure resource | Module |
|---|---|---|
| Boundary | one resource group `<prefix>-<env>-rg` | `resource_group` |
| Data | Data Lake Storage Gen2 account `<prefix><env>` with containers `landing`, `bronze`, `silver`, `gold`, `quarantine`, `ops` | `storage` |
| Personal data | a separate account `<prefix><env>pi`, container `landing`, lifecycle purge after 1 day | `storage` |
| Secrets | Key Vault `<prefix>-<env>-kv`, role-based access, deployer is Secrets Officer | `key_vault` |
| Engine | Azure Databricks workspace `<prefix>-<env>-dbx`, premium SKU | `workspace` |
| Identity to storage | Databricks access connector (managed identity) with Storage Blob Data Contributor on the environment's account | `access_connector` |
| Governance | Unity Catalog storage credential `<env>_layers`, one external location per layer, catalogs `<env>_<layer>` (landing, bronze, silver, gold, ops), each bound to this environment's workspace only, landing volume `<env>_landing.deliveries.files` | `unity_catalog` |
| Metadata store | a Lakebase (managed PostgreSQL) project `dp-meta-<env>`, fixed 1 compute unit | `lakebase` |
| Cost | a monthly budget on the resource group (default 20 USD) with alerts at 80% actual and 100% forecast; tags `project`, `env`, `cost_center`, `managed_by` on everything | `budget` |

Names follow the first cloud's module (ADR-012 `<env>_<layer>`), so the engine job needs no second layout. The Python
adapters agree with the Terraform names: the tests check `account_name` and `key_vault_name` from
`dataplatform.adapters` against the same rules.

Dev and prod share one subscription to start (the stack file says so). Each has its own state file, resource group,
accounts, vault, workspace, catalogs and bundle target, so moving prod to its own subscription later is a new backend
and provider setting, not a redesign.

## Run it offline

```
make -C realms/azure-databricks check          # tests + bundle up to date + terraform fmt / validate / mocked plan
make -C realms/azure-databricks test           # the table-driven checks (naming, layers, secrets, budget, isolation, pins, bundle)
make -C realms/azure-databricks tf-validate    # terraform init -backend=false, validate, terraform test on both environments
make -C realms/azure-databricks generate       # rewrite bundle/databricks.yml from the engine's bundle
```

`terraform init -backend=false` needs the network once, to download the two providers (pinned exactly: azurerm 5.12.0
of 29 Oct 2026 and databricks 1.141.0 of 2 Nov 2026, both more than two weeks old on 19 Nov 2026). `terraform test`
plans against mocked providers; it never contacts Azure.

## The bundle

`generate_bundle.py` reads the engine's `realms/databricks/engine/databricks.yml` (not edited) and writes
`bundle/databricks.yml`: the same jobs (`dp-<env>-engine-days` and the probe), paths re-pointed at the engine folder,
targets `azure-dev` and `azure-prod`. Differences from the Free Edition target: no host in the file (the workspace is
chosen by `DATABRICKS_HOST`, the Terraform output `workspace_url`), `mode: production` with a per-identity root path,
metadata store `dp-meta-<env>`, cost tags on the jobs. Compute is serverless, as the engine job already is (an
environment with `environment_version: "3"` and no cluster spec); Unity Catalog is on by default in a premium
workspace. `databricks bundle validate` needs a live login, so it is not part of the offline checks.

## What it costs (estimates, not quotes)

Basis: public list prices recalled from memory, not re-read from the price page on the day; the owner's currency and
agreement change them. Treat every figure as an order of magnitude.

| Item | Idle, per environment per day | One 3-day proof run |
|---|---|---|
| Workspace, resource group, access connector, Unity Catalog, role assignments | 0 | 0 |
| Storage (two empty LRS accounts; a proof run writes well under 10 GB) | under 0.01 USD | under 0.05 USD |
| Key Vault (standard, a few operations) | under 0.01 USD | under 0.01 USD |
| Lakebase project, 1 compute unit | storage only if it scales to zero when idle (under 0.05 USD); **unverified**: whether an endpoint scales to zero by default on Azure | a few hours of one compute unit: roughly 1 to 2 USD (assumed) |
| Serverless job compute | 0 (nothing runs) | assumed 10 to 20 serverless DBU for the run at about 0.4 to 0.5 USD each: roughly 4 to 10 USD |
| **Total** | **about 0.1 USD a day (about 3 USD a month) with nothing running** | **about 5 to 12 USD** |

The proof-run duration is not known (no run exists on this cloud); the range assumes one to two hours of job time. Two
environments double the idle figure. The default budget (20 USD a month per environment) covers one proof run and a
month of idle. A budget only alerts; destroying the environment is what stops the cost.

## Region

Default `eastus2`. Lakebase is generally available on Azure since 2 Mar 2026 in `eastus2`, `westeurope` and `westus`
plus eleven more (Australia East, Brazil South, Canada Central, Central India, South Central US, Southeast Asia, East
US, Central US, North Europe, UK South, West US 2), and since Aug 2026 in North Central US, France Central, Germany
West Central and East Asia. **How this was checked:** a web search of Microsoft's release notes and the Azure
Databricks documentation on 19 Nov 2026, plus a read-only `az provider show` that lists the regions where the
workspace resource type exists (38, including all of the above). Neither proves Lakebase is offered on the owner's
subscription or tier: that is **unverified** until a project is created (AZ-7).

## Unverified until an account runs it

- That a first workspace in a region gets the region's Unity Catalog metastore attached automatically, and that the
  deploying identity can create a storage credential and catalogs on it. If not, an account administrator must be
  made once (RUNBOOK, step 6) and, if the region has no metastore, one created in the account console.
- That the databricks provider's `databricks_postgres_project` creates a Lakebase project on Azure, and that the
  engine's Lakebase adapter connects to a project (it connected on Free Edition; see `DEPLOYED.md` of the first-cloud
  realm).
- That the access connector's role assignment has propagated before the storage credential is validated (the first
  apply may need a second run).
- That a Key Vault name, storage account name or workspace name is free (names are global; a clash shows at apply).
- Prices above.

## What is NOT built

| Item | Where it is tracked |
|---|---|
| Event Hubs stream adapter and namespace (accepted gaps ORCH-06, DUP-04) | AZ-4 |
| Data Factory mover (copy activities, change data capture) and the open-source log reader | AZ-6 |
| Virtual network injection, Private Link, customer-managed keys (specified, not built; public network access is on) | `dbs/cloud/azure.yaml` network_boundary, key_manager |
| Key Vault backed Databricks secret scope (the engine job mints its token key per run and needs none) | |
| Log Analytics workspace, Event Grid topics, Purview | |
| A PostgreSQL flexible-server fallback for the metadata store when Lakebase is not offered | AZ-7 |
| The 23-hour purge as a lifecycle rule (Azure lifecycle works in whole days: the backstop is 1 day; the platform's own purge carries 23 hours) | |
| GitHub Actions workflow for Azure | |
| Per-environment subscriptions and a management group | |
