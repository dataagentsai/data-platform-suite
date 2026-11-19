# Databricks on AWS realm (Goal 1b, task DE-112)

The cloud layer (`dbs/cloud/aws.yaml`) under the Databricks engine layer (`dbs/engine/databricks.yaml`), composed by
the stack `dbs/stacks/aws-databricks.yaml` (ADR-014). The Databricks-on-AWS path is written and checked offline only.
A cheaper variant on a free-plan AWS account + Databricks Free Edition is deployed for dev (Databricks half only; the
AWS half is blocked by the account's SCP): see `DEPLOYED.md`.

| Part | Where | Checked by |
|---|---|---|
| Terraform, cloud layer: S3 buckets per environment and layer, KMS, IAM roles, Secrets Manager, VPC, Kinesis, budget | `infra/modules/{storage,network,secrets,stream,budget}` | `terraform validate` + plan on mocked providers (`infra/platform/tests`) |
| Terraform, engine layer: workspace, metastore assignment, storage credential, external locations, catalogs `<env>_<layer>`, landing volume, Lakebase | `infra/modules/{workspace,unity_catalog,lakebase}` | same |
| Once per account: GitHub OIDC roles, cost allocation tags, the regional metastore | `infra/account` | same (`infra/account/tests`) |
| Movers: S3 landing store, S3 file drop, S3 object store, Kinesis event stream | `src/dataplatform/adapters/{s3,kinesis}.py` | the shared tables in `tests/dataplatform/test_source_ports.py` (rows `s3`, `kinesis`) and `test_cloud_adapters.py`, on moto |
| The tyre world writing to S3 and Kinesis | `scripts/run_world.py --binding aws` | run against moto (2 days, scale 0.02) |
| Asset bundle generated from the compiled manifests (M4) | `generate_bundle.py` -> `bundle/` | `tests/aws_databricks/test_bundle.py`, `databricks bundle validate` |
| CI/CD | `.github/workflows/aws-databricks.yml` | runs on every push |

## Environments: one AWS account, two workspaces

**Choice:** dev and prod live in **one AWS account**, each with its **own Databricks workspace**, its own buckets,
KMS keys, IAM roles, VPC, Kinesis streams, Lakebase instance and catalogs. Both workspaces share the one Unity Catalog
metastore of the region (Databricks allows one per region), and every catalog, external location and storage
credential is bound to its own environment's workspace only, so dev compute cannot read or write prod tables.

**Why:** free tier first. One account means one free tier, one bill, one OIDC setup and no AWS Organizations to run;
a Databricks workspace has no fixed fee, so a second one costs nothing until it runs something. The stack file says
the same ("one AWS account to start"). The separation that matters (data, keys, roles, compute, catalogs) is already
per environment, so moving prod to its own account later (`dbs/cloud/aws.yaml` says one account per environment under
Organizations) is a new state backend and provider role for prod, not a redesign. Free Edition stays available as a
sandbox (`realms/databricks`).

## Run it offline

```
make -C realms/aws-databricks test          # table-driven: slice 1, the world, source-port contracts incl. S3 + Kinesis (moto), bundle
make -C realms/aws-databricks generate      # rewrite bundle/ from realms/databricks/compiled/*.json
make -C realms/aws-databricks tf-validate   # needs terraform >= 1.10 (CI runs it on every push)
```

## When the accounts arrive

Done by hand, once (everything else is Terraform or CI):

1. **AWS account.** Create it; sign in locally with an admin identity (`aws configure sso`, then `aws sso login`).
   Pick the region: `us-east-2` is the default (decided 4 Nov, beside the Free Edition workspace); for another region change `aws_region` and `azs` in `infra/envs/*.tfvars`
   and `region` in `infra/envs/*.backend.hcl`.
2. **Databricks account on AWS** (databricks.com sign-up or AWS Marketplace; the premium trial is fine). Do **not**
   create a workspace in the quickstart; Terraform makes both. Note the **account id** (account console, top right).
3. In the Databricks **account console**:
   - create a group `platform-admins` and add yourself;
   - create a service principal `dp-ci`, make it account admin and a member of `platform-admins`, note its
     **application (client) id**;
   - add a **federation policy** to `dp-ci` for GitHub Actions: issuer `https://token.actions.githubusercontent.com`,
     audience your Databricks account id, subjects `repo:dataagentsai/data-platform-suite:environment:dev`,
     `...:environment:prod` and `...:pull_request`. No client secret is ever created.
4. **Fill the placeholders:** `changeme-dp` (a company-unique prefix, e.g. `acme-dp`; S3 names are global) and the
   zero ids in `infra/envs/*.tfvars` and `infra/envs/*.backend.hcl`.
5. Log in to the Databricks account locally: `databricks auth login --host https://accounts.cloud.databricks.com --account-id <id>`.
6. `make -C realms/aws-databricks bootstrap` (the state bucket), then `make -C realms/aws-databricks account`.
   Copy the output `metastore_id` into `infra/envs/dev.tfvars` and `prod.tfvars`; note `ci_role_arns`.
7. **Lakebase probe:** if Lakebase is not offered in your region or tier, set `lakebase_enabled = false`; the
   metadata store then binds to a plain PostgreSQL adapter (a binding change only).
8. `make -C realms/aws-databricks deploy ENV=dev`. Then put the source secrets' values
   (`aws secretsmanager put-secret-value --secret-id <prefix>/dev/sources --secret-string file://...`).
9. In each new workspace (account console, Workspaces, Permissions): give `dp-ci` the **admin** role.
10. **GitHub** (repository settings):
    - environments `dev` and `prod`; on `prod` add required reviewers and allow only the `main` branch
      (this is the release gate);
    - repository variables: `AWS_REGION`, `AWS_PLAN_ROLE_ARN` (= `ci_role_arns.plan`), `DATABRICKS_CLIENT_ID`
      (the `dp-ci` application id), and `DEPLOY_ENABLED = true` last;
    - environment variables, per environment: `AWS_DEPLOY_ROLE_ARN` (= `ci_role_arns.dev` / `.prod`) and
      `DATABRICKS_HOST` (Terraform output `workspace_url` of that environment).
11. A day after the first deploy, run `make account` again with `activate_cost_tags = true` in
    `infra/envs/account.tfvars`, so the `env`, `project` and `cost_center` tags show up in billing.

## Stack variant: AWS account + Databricks Free Edition (deployed for dev)

The accounts that exist today are a free-plan AWS account and a Databricks **Free Edition** workspace (no account
console, so none of steps 2–9 apply). `infra/free-edition/{aws,databricks}` deploy the same environment shape onto
them; `DEPLOYED.md` lists what exists, what it costs, how to tear it down, and why the AWS half is still blocked.

```
make -C realms/aws-databricks free-databricks ENV=dev   # catalogs dev_<layer>, schemas, landing volume (profile free)
make -C realms/aws-databricks free-meta ENV=dev         # Lakebase dp-meta-dev: database dp_meta, schema, tyre manifest
make -C realms/aws-databricks free-bundle-validate      # generated bundle, target free-dev
AWS_PROFILE=dp-dev make -C realms/aws-databricks free-aws ENV=dev   # S3, SNS, logs, GitHub OIDC (SCP-blocked today)
```

### GitHub settings for the Free Edition variant

Set these by hand once `free-aws` has been applied (the role ARNs are its output `ci_role_arns`; account id masked
here):

| Where | Name | Value |
|---|---|---|
| Repository variable | `AWS_REGION` | `us-east-2` |
| Repository variable | `AWS_PLAN_ROLE_ARN` | `arn:aws:iam::<account-id>:role/dataagents-dp-ci-plan` |
| Environment `dev`, variable | `AWS_DEPLOY_ROLE_ARN` | `arn:aws:iam::<account-id>:role/dataagents-dp-ci-dev` |
| Environment `prod`, variable | `AWS_DEPLOY_ROLE_ARN` | `arn:aws:iam::<account-id>:role/dataagents-dp-ci-prod` |
| Environments `dev` and `prod`, variable | `DATABRICKS_HOST` | `https://dbc-00000000-0000.cloud.databricks.com` |
| Repository variable | `DATABRICKS_CLIENT_ID` | application id of a workspace service principal `dp-ci` (Settings → Identity and access) |
| Environment `prod` | protection rules | required reviewers; deployments from `main` only |
| Repository variable | `DEPLOY_ENABLED` | **leave unset**: the deploy jobs still call `make infra`/`bundle` for Databricks on AWS |

Free Edition has no account console, so the GitHub OIDC federation policy W2 puts on `dp-ci` (step 3) may not be
available; if it is not, CI needs an OAuth secret for `dp-ci` as the repository **secret** `DATABRICKS_CLIENT_SECRET`
and `DATABRICKS_AUTH_TYPE=oauth-m2m` instead of `github-oidc` in the workflow. Neither was set here.
With `AWS_PLAN_ROLE_ARN` set, the `plan-real` job starts running `infra/platform` against the account, which is the
Databricks-on-AWS root; point it at `infra/free-edition/aws` first if that job is wanted for this variant.

From then on (Databricks on AWS): a pull request runs the checks and a real `terraform plan`; a merge to main deploys dev, runs the
integration test, waits for the prod reviewers, then deploys prod with the same commit.

## What it costs when idle (approximate)

| Item | dev | prod |
|---|---|---|
| KMS keys (1 USD each a month) | 1 | 6 |
| Kinesis, provisioned shards (0.015 USD a shard-hour) | ~22 (2 shards) | ~11 (1 shard) + ~15 for 7-day retention |
| Secrets Manager (0.40 USD a secret) | 0.40 | 0.40 |
| S3, VPC, gateway endpoint, workspace | ~0 | ~0 |
| NAT gateway (off by default; only classic clusters need it) | 0 | 0 |
| Lakebase CU_1 and serverless compute | billed by Databricks while running | same |

Set `streams = {}` until the press readings are needed to save the Kinesis shards.

### Kinesis sizing for the tyre world

A world day is 622,080 press readings (48 presses, one every 10 seconds); each record, with the arrival time carried
inside it, is about 213 bytes. A 7-day run is 4.35 million records, about 0.93 GB.

| | 1 shard | 2 shards (dev default) | 4 shards |
|---|---|---|---|
| Write limit (1,000 records a second per shard binds before 1 MB a second) | ~73 min for 7 days | ~36 min | ~18 min |
| Shard-hours if the stream lives one day / one week | 24 / 168 | 48 / 336 | 96 / 672 |
| Cost, one day / one week (plus 0.06 USD of PUT units for 4.35M records) | 0.42 / 2.58 USD | 0.78 / 5.10 USD | 1.50 / 10.14 USD |

The world writes from one process, about 8,700 PutRecords calls of 500 records; at 0.15 to 0.3 seconds a call that is
22 to 44 minutes whatever the shard count, so 2 shards is the match. On-demand mode would cost about 6.80 USD a week.
Dev keeps readings 24 hours, so the reader must catch up within a day. Prices are us-east-1 list prices; check your
region. Run: `python scripts/run_world.py --days 7 --db sqlite --binding aws --aws-prefix <prefix> --env dev`
(database and dealer interface stay local until their cloud adapters exist). Budgets alert at 25 USD (dev) and
75 USD (prod) once `budget_emails` is set.

## Known gaps (to close before the first real run)

- **Landing on S3.** The V1 jobs read landing through a Unity Catalog volume. Here that volume must be an external
  volume over the S3 landing bucket (`<env>_landing.deliveries.files`), so the client writes to S3 and the platform
  reads through the volume.
- **Token key.** The V1 jobs read it from a secret scope (`token-key`). Here it should come from Secrets Manager,
  encrypted with the KMS key Terraform makes.
- **Lakebase.** Lakebase is now autoscaling (a project with branches and endpoints, not an instance); the
  `lakebase` module predates that and needs checking on account day. The plain PostgreSQL adapter is the fallback.

Closed since this list was first written: the generator emits the V1 shape from the manifest (one platform job per
domain with three tasks, and a proof job that `make integration` runs); the metadata store on Lakebase is built and proven; `S3Landing` and
`FileLanding` both refuse a second write to a delivery, with a contract test on each.
