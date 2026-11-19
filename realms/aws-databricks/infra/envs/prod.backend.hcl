# Terraform state for prod. The bucket is made once by `make bootstrap` (realms/aws-databricks/Makefile).
bucket       = "changeme-dp-tfstate"
key          = "platform/prod.tfstate"
region       = "us-east-2"
use_lockfile = true
encrypt      = true
