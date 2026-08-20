terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Remote state — recommended once you deploy for real.
  # Create the bucket first, then uncomment and run: terraform init -migrate-state
  #
  # backend "s3" {
  #   bucket       = "packetshard-tfstate-<your-unique-suffix>"
  #   key          = "packetshard/terraform.tfstate"
  #   region       = "eu-central-1"
  #   use_lockfile = true
  # }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
    }
  }
}
