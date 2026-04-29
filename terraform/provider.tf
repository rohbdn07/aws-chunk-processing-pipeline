terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.98.0"
    }
  }
}

provider "aws" {
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  region     = var.aws_region

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3            = var.localstack_endpoint
    sfn           = var.localstack_endpoint
    iam           = var.localstack_endpoint
    lambda        = var.localstack_endpoint
    cloudwatch    = var.localstack_endpoint
    cloudwatchlog = var.localstack_endpoint
    sns           = var.localstack_endpoint
    sqs           = var.localstack_endpoint
  }
}
