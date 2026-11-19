# Databricks on AWS. Everything offline works now; targets marked [account] need the AWS and Databricks accounts.
#
#   make check                       offline: tests (moto), bundle up to date, terraform fmt/validate/mock plan
#   make bootstrap                   [account] once: the Terraform state bucket
#   make account                     [account] once: GitHub OIDC roles, cost tags, Unity Catalog metastore
#   make deploy ENV=dev              [account] infrastructure, then jobs and pipelines
#   make integration ENV=dev         [account] feed one world day through S3 and run every generated job
#
# Stack variant "AWS account + Databricks Free Edition" (DEPLOYED.md); local state in ~/.dataplatform/tfstate:
#   make free-databricks ENV=dev     catalogs <env>_<layer>, schemas, landing volume (profile $(FREE_PROFILE))
#   make free-meta ENV=dev           metadata store on Lakebase: database, schema, tyre manifest, read-back check
#   make free-aws ENV=dev            S3, SNS, log group, GitHub OIDC roles (blocked today by the account's SCP)
#   make free-bundle-validate        the generated bundle against the Free Edition workspace
#
# Auth: AWS from the environment (aws sso login / AWS_PROFILE locally, OIDC role in CI); Databricks from
# DATABRICKS_HOST + a CLI profile locally, github-oidc in CI. Nothing here reads or writes a key.

ENV ?= dev
REPO := $(abspath ../..)
INFRA := infra
TF ?= terraform
PY := uv run --no-project --python 3.12
AUTO_APPROVE ?=
APPROVE := $(if $(AUTO_APPROVE),-auto-approve,)
STATE_BUCKET = $(shell sed -n 's/^bucket *= *"\(.*\)"/\1/p' $(INFRA)/envs/account.backend.hcl)
REGION = $(shell sed -n 's/^region *= *"\(.*\)"/\1/p' $(INFRA)/envs/account.backend.hcl)

FREE_PROFILE ?= free
FREE_STATE ?= $(HOME)/.dataplatform/tfstate/free-edition
FREE_META_INSTANCE ?= dp-meta-$(ENV)
TYRE_MANIFEST ?= $(HOME)/data-platform-catalog/slices/02-tyre/manifest

.PHONY: free-databricks free-aws free-meta free-bundle-validate free-destroy-databricks
.PHONY: catalog-copy
.PHONY: check test generate bundle-check tf-fmt tf-validate bootstrap account plan infra bundle deploy integration destroy

check: test bundle-check tf-validate

test:
	cd $(REPO) && PYTHONPATH=src $(PY) --with pytest --with pyyaml --with boto3 --with 'moto[s3,kinesis]' \
		pytest -q -p no:cacheprovider -o addopts="" tests/dataplatform tests/aws_databricks

generate:
	cd $(REPO) && $(PY) --with pyyaml python realms/aws-databricks/generate_bundle.py

bundle-check:
	cd $(REPO) && $(PY) --with pyyaml python realms/aws-databricks/generate_bundle.py --check

tf-fmt:
	$(TF) fmt -recursive $(INFRA)

tf-validate:
	$(TF) fmt -check -recursive $(INFRA)
	for root in platform account free-edition/aws free-edition/databricks; do \
		$(TF) -chdir=$(INFRA)/$$root init -backend=false -input=false >/dev/null && \
		$(TF) -chdir=$(INFRA)/$$root validate || exit 1; \
	done
	for env in dev prod; do $(TF) -chdir=$(INFRA)/platform test -var-file=../envs/$$env.tfvars || exit 1; done
	$(TF) -chdir=$(INFRA)/account test -var-file=../envs/account.tfvars
	for env in dev prod; do for half in aws databricks; do \
		$(TF) -chdir=$(INFRA)/free-edition/$$half test -var-file=../../envs/free-edition/$$env.$$half.tfvars || exit 1; \
	done; done

# ---------------------------------------------------------------------------- [account]
bootstrap:
	aws s3api head-bucket --bucket $(STATE_BUCKET) 2>/dev/null || \
		aws s3api create-bucket --bucket $(STATE_BUCKET) --region $(REGION) \
			--create-bucket-configuration LocationConstraint=$(REGION)
	aws s3api put-bucket-versioning --bucket $(STATE_BUCKET) --versioning-configuration Status=Enabled
	aws s3api put-public-access-block --bucket $(STATE_BUCKET) --public-access-block-configuration \
		BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

account:
	$(TF) -chdir=$(INFRA)/account init -input=false -backend-config=../envs/account.backend.hcl
	$(TF) -chdir=$(INFRA)/account apply -input=false -var-file=../envs/account.tfvars $(APPROVE)

plan:
	$(TF) -chdir=$(INFRA)/platform init -input=false -reconfigure -backend-config=../envs/$(ENV).backend.hcl
	$(TF) -chdir=$(INFRA)/platform plan -input=false -var-file=../envs/$(ENV).tfvars

infra:
	$(TF) -chdir=$(INFRA)/platform init -input=false -reconfigure -backend-config=../envs/$(ENV).backend.hcl
	$(TF) -chdir=$(INFRA)/platform apply -input=false -var-file=../envs/$(ENV).tfvars $(APPROVE)

# The tasks read the spec catalog slice from beside the bundle; it is copied in before every deploy (git-ignored).
catalog-copy:
	mkdir -p bundle/_catalog/slices
	rsync -a --delete $(dir $(TYRE_MANIFEST)) bundle/_catalog/slices/$(notdir $(patsubst %/,%,$(dir $(TYRE_MANIFEST))))/

bundle: bundle-check catalog-copy
	cd bundle && DATABRICKS_HOST=$${DATABRICKS_HOST:-$$($(TF) -chdir=../$(INFRA)/platform output -raw workspace_url)} \
		databricks bundle deploy -t $(ENV)

deploy: infra bundle

integration:
	cd $(REPO) && $(PY) --with boto3 --with pyyaml python realms/aws-databricks/integration.py --env $(ENV)

destroy:
	@test "$(ENV)" = dev || { echo "destroy is for dev only"; exit 1; }
	cd bundle && databricks bundle destroy -t dev --auto-approve
	$(TF) -chdir=$(INFRA)/platform destroy -input=false -var-file=../envs/dev.tfvars

# ---------------------------------------------------------------------------- Free Edition stack variant
free-databricks:
	mkdir -p $(FREE_STATE)
	$(TF) -chdir=$(INFRA)/free-edition/databricks init -input=false -reconfigure -backend-config=path=$(FREE_STATE)/$(ENV).databricks.tfstate
	DATABRICKS_CONFIG_PROFILE=$(FREE_PROFILE) $(TF) -chdir=$(INFRA)/free-edition/databricks apply -input=false \
		-var-file=../../envs/free-edition/$(ENV).databricks.tfvars $(APPROVE)

free-aws:
	mkdir -p $(FREE_STATE)
	$(TF) -chdir=$(INFRA)/free-edition/aws init -input=false -reconfigure -backend-config=path=$(FREE_STATE)/$(ENV).aws.tfstate
	$(TF) -chdir=$(INFRA)/free-edition/aws apply -input=false -var-file=../../envs/free-edition/$(ENV).aws.tfvars $(APPROVE)

free-meta:
	cd $(REPO) && DATABRICKS_CONFIG_PROFILE=$(FREE_PROFILE) $(PY) --with 'psycopg[binary]' --with pyyaml \
		python realms/aws-databricks/meta_setup.py --instance $(FREE_META_INSTANCE) --manifest $(TYRE_MANIFEST)

free-bundle-validate: bundle-check
	cd bundle && databricks bundle validate -t free-dev -p $(FREE_PROFILE)

free-destroy-databricks:
	@test "$(ENV)" = dev || { echo "destroy is for dev only"; exit 1; }
	$(TF) -chdir=$(INFRA)/free-edition/databricks init -input=false -reconfigure -backend-config=path=$(FREE_STATE)/dev.databricks.tfstate
	DATABRICKS_CONFIG_PROFILE=$(FREE_PROFILE) $(TF) -chdir=$(INFRA)/free-edition/databricks destroy -input=false \
		-var-file=../../envs/free-edition/dev.databricks.tfvars
