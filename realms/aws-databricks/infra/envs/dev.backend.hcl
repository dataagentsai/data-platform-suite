# Terraform state for dev. The bucket is made once by `make bootstrap` (realms/aws-databricks/Makefile).
bucket       = "changeme-dp-tfstate"
key          = "platform/dev.tfstate"
region       = "us-east-2"
use_lockfile = true
encrypt      = true
