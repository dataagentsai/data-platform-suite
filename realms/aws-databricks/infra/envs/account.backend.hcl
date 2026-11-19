# Terraform state for the once-per-account resources.
bucket       = "changeme-dp-tfstate"
key          = "account.tfstate"
region       = "us-east-2"
use_lockfile = true
encrypt      = true
