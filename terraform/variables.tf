variable "s3_bucket_name" {
  type        = string
  default     = "report-store"
  description = "Name of the S3 bucket used by the workflow."
}

variable "aws_region" {
  type        = string
  default     = "eu-west-1"
  description = "AWS region used by the LocalStack-backed provider."
}

variable "localstack_endpoint" {
  type        = string
  default     = "http://host.docker.internal:4566"
  description = "Shared LocalStack endpoint used for AWS service emulation. Use host.docker.internal for Lambda to access LocalStack."
}

variable "aws_access_key" {
  type        = string
  default     = "key"
  description = "Static access key used for LocalStack development."
}

variable "aws_secret_key" {
  type        = string
  default     = "secret"
  description = "Static secret key used for LocalStack development."
}
