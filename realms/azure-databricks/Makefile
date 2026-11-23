# Databricks on Azure. Everything offline works now; targets marked [account] need an enabled Azure subscription and
# create billable resources. Nothing offline reads or writes a key or a subscription id.
#
#   make check                       offline: tests, bundle up to date, terraform fmt / validate / mock plan
#   make test | tf-validate | generate
#   make bootstrap                   [account] once: the Terraform state storage account
#   make plan ENV=dev                [account] read-only plan against the subscription
#   make infra ENV=dev               [account] apply the environment's infrastructure
#   make bundle ENV=dev              [account] deploy the engine job (bundle target azure-<env>)
#   make run ENV=dev                 [account] run job dp-<env>-engine-days (3 compressed days)
#   make destroy ENV=dev             [account] bundle destroy, then terraform destroy
#
# Auth: `az login` (Terraform and the bundle both use the signed-in identity). The subscription id is read from
# TF_VAR_subscription_id / ARM_SUBSCRIPTION_ID, never from a file. Settings live in infra/envs/<env>.tfvars and
# <env>.backend.hcl, copied from the .example files (git-ignored).

ENV ?= dev
REPO := $(abspath ../..)
INFRA := infra
TF ?= terraform
PY := uv run --no-project --python 3.12
AUTO_APPROVE ?=
APPROVE := $(if $(AUTO_APPROVE),-auto-approve,)
TARGET := azure-$(ENV)
ENGINE := $(REPO)/realms/databricks/engine
SLICE ?= 03-clinical
SLICES_DIR ?= $(HOME)/data-platform-catalog/slices
STATE_RG ?= $(shell sed -n 's/^resource_group_name *= *"\(.*\)"/\1/p' $(INFRA)/envs/dev.backend.hcl)
STATE_ACCOUNT ?= $(shell sed -n 's/^storage_account_name *= *"\(.*\)"/\1/p' $(INFRA)/envs/dev.backend.hcl)
STATE_CONTAINER ?= $(shell sed -n 's/^container_name *= *"\(.*\)"/\1/p' $(INFRA)/envs/dev.backend.hcl)
STATE_LOCATION ?= eastus2

.PHONY: adf-generate adf-deploy check test generate bundle-check tf-fmt tf-validate bootstrap plan infra catalog-copy bundle-validate bundle run deploy destroy

check: test bundle-check tf-validate

test:
	cd $(REPO) && PYTHONPATH=src $(PY) --with pytest --with pyyaml --with python-hcl2 \
		pytest -q -p no:cacheprovider -o addopts="" realms/azure-databricks/tests

generate:
	cd $(REPO) && $(PY) --with pyyaml python realms/azure-databricks/generate_bundle.py

bundle-check:
	cd $(REPO) && $(PY) --with pyyaml python realms/azure-databricks/generate_bundle.py --check

tf-fmt:
	$(TF) fmt -recursive $(INFRA)

# Offline: no backend, no credentials. `init` needs the network once, to download the two providers.
tf-validate:
	$(TF) fmt -check -recursive $(INFRA)
	$(TF) -chdir=$(INFRA)/platform init -backend=false -input=false >/dev/null
	$(TF) -chdir=$(INFRA)/platform validate
	for env in dev prod; do $(TF) -chdir=$(INFRA)/platform test -var-file=../envs/$$env.tfvars.example || exit 1; done

# ---------------------------------------------------------------------------- [account]: these create or change Azure resources
bootstrap:
	@echo "creating the Terraform state account $(STATE_ACCOUNT) in resource group $(STATE_RG)"
	az group create --name $(STATE_RG) --location $(STATE_LOCATION) --output none
	az storage account show --name $(STATE_ACCOUNT) --resource-group $(STATE_RG) --output none 2>/dev/null || \
		az storage account create --name $(STATE_ACCOUNT) --resource-group $(STATE_RG) --location $(STATE_LOCATION) \
			--sku Standard_LRS --kind StorageV2 --allow-blob-public-access false --min-tls-version TLS1_2 --output none
	az storage container create --name $(STATE_CONTAINER) --account-name $(STATE_ACCOUNT) --auth-mode login --output none

plan:
	$(TF) -chdir=$(INFRA)/platform init -input=false -reconfigure -backend-config=../envs/$(ENV).backend.hcl
	$(TF) -chdir=$(INFRA)/platform plan -input=false -var-file=../envs/$(ENV).tfvars

infra:
	$(TF) -chdir=$(INFRA)/platform init -input=false -reconfigure -backend-config=../envs/$(ENV).backend.hcl
	$(TF) -chdir=$(INFRA)/platform apply -input=false -var-file=../envs/$(ENV).tfvars $(APPROVE)

# The engine's tasks read the spec catalog slice from beside the engine bundle (a git-ignored copy; the engine's own
# convention). It is copied in before every deploy.
catalog-copy:
	mkdir -p $(ENGINE)/_catalog/slices
	rsync -a --delete $(SLICES_DIR)/$(SLICE) $(ENGINE)/_catalog/slices/

bundle-validate: bundle-check
	cd bundle && DATABRICKS_HOST=$${DATABRICKS_HOST:-$$($(TF) -chdir=../$(INFRA)/platform output -raw workspace_url)} \
		databricks bundle validate -t $(TARGET)

bundle: bundle-check catalog-copy
	cd bundle && DATABRICKS_HOST=$${DATABRICKS_HOST:-$$($(TF) -chdir=../$(INFRA)/platform output -raw workspace_url)} \
		databricks bundle deploy -t $(TARGET)

run:
	cd bundle && DATABRICKS_HOST=$${DATABRICKS_HOST:-$$($(TF) -chdir=../$(INFRA)/platform output -raw workspace_url)} \
		databricks bundle run engine_days -t $(TARGET) --params slice=$(SLICE),days=3

deploy: infra bundle

destroy:
	cd bundle && DATABRICKS_HOST=$${DATABRICKS_HOST:-$$($(TF) -chdir=../$(INFRA)/platform output -raw workspace_url)} \
		databricks bundle destroy -t $(TARGET) --auto-approve
	$(TF) -chdir=$(INFRA)/platform destroy -input=false -var-file=../envs/$(ENV).tfvars

# The managed copy service for the movers (ADR-033). adf/movers.json is the input; the generated objects hold the
# storage account name, so they are not committed. The factory itself is made by `make infra`.
ACCOUNT ?= $(shell $(TF) -chdir=$(INFRA)/platform output -raw storage_account 2>/dev/null)
adf-generate:
	python3 generate_adf.py --account $(ACCOUNT)

adf-deploy: adf-generate
	python3 deploy_adf.py deploy --factory $(shell $(TF) -chdir=$(INFRA)/platform output -raw data_factory) --resource-group $(shell $(TF) -chdir=$(INFRA)/platform output -raw resource_group)
