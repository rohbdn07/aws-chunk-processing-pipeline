locals {
  splitter_zip_path = "${path.module}/../build/splitter_function.zip"
  sender_zip_path   = "${path.module}/../build/sender_function.zip"
}

resource "aws_s3_bucket" "s3_bucket" {
  bucket = var.s3_bucket_name
}

resource "aws_iam_role" "lambda_exec" {
  name = "lambda_exec_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Sid    = "",
        Principal = {
          Service = "lambda.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "sns_publish_policy" {
  name        = "Lambda_SNS_Publish"
  description = "Allow lambda to publish to SNS"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = "sns:Publish",
        Resource = aws_sns_topic.splitter_status.arn
      }
    ]
  })
}

# S3 policy for Lambda to read and write objects
resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "Lambda_S3_Access"
  description = "Allow lambda to read and write to S3"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = "arn:aws:s3:::${var.s3_bucket_name}/*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:ListBucket"
        ],
        Resource = "arn:aws:s3:::${var.s3_bucket_name}"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_attachment" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}

resource "aws_iam_role_policy" "lambda_sqs_policy" {
  name = "lambda-sqs-access"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Action = [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      Resource = aws_sqs_queue.my_queue.arn
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_sns_attachment" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.sns_publish_policy.arn
}

resource "aws_cloudwatch_log_group" "lambda_log_group_1" {
  name              = "/aws/lambda/splitter_function"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "lambda_log_group_2" {
  name              = "/aws/lambda/sender_function"
  retention_in_days = 7
}

resource "aws_lambda_function" "lambda_function_L1" {
  function_name    = "splitter_function"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = "nodejs22.x"
  handler          = "index.handler"
  filename         = local.splitter_zip_path
  timeout          = 900
  memory_size      = 4096
  source_code_hash = filebase64sha256(local.splitter_zip_path)

  environment {
    variables = {
      STAGE               = "dev"
      DEBUG               = "true"
      BUCKET_NAME         = var.s3_bucket_name
      LOCALSTACK_ENDPOINT = var.localstack_endpoint
      SNS_TOPIC_ARN       = aws_sns_topic.splitter_status.arn
      SQS_URL             = aws_sqs_queue.my_queue.url
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda_log_group_1, aws_sns_topic.splitter_status]
}

resource "aws_lambda_function" "lambda_function_L2" {
  function_name    = "sender_function"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = "nodejs22.x"
  handler          = "index.handler"
  filename         = local.sender_zip_path
  timeout          = 900
  memory_size      = 2048
  source_code_hash = filebase64sha256(local.sender_zip_path)
  depends_on       = [aws_cloudwatch_log_group.lambda_log_group_2]

  environment {
    variables = {
      STAGE               = "dev"
      DEBUG               = "true"
      BUCKET_NAME         = var.s3_bucket_name
      LOCALSTACK_ENDPOINT = var.localstack_endpoint
    }
  }
}

resource "aws_iam_role" "step_function_role" {
  name = "StepFunctionRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Principal = {
        Service = "states.amazonaws.com"
      },
      Effect = "Allow",
    }]
  })
}

resource "aws_iam_role_policy" "step_function_policy" {
  name = "step_function_policy"
  role = aws_iam_role.step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = ["lambda:InvokeFunction"],
        Resource = [
          aws_lambda_function.lambda_function_L1.arn,
          aws_lambda_function.lambda_function_L2.arn,
        ]
      }
    ]
  })
}


resource "aws_sfn_state_machine" "sfn_state_machine" {
  name     = "my-state-machine"
  role_arn = aws_iam_role.step_function_role.arn

  definition = jsonencode({
    Comment = "Split and send report by chunks. Workflow Summary: the workflow splits a report into chunks and processes each chunk in concurrently using Map. Each iteration processes one item from the keysToChunks array. The workflow ends after all chunks have been processed successfully.",
    StartAt = "SplitLargeReport",
    States = {
      SplitLargeReport = {
        Type     = "Task",
        Resource = aws_lambda_function.lambda_function_L1.arn,
        Next     = "SendReportByChunks"
      },
      SendReportByChunks = {
        Type           = "Map",
        ItemsPath      = "$.keysToChunks",
        MaxConcurrency = 10,
        Iterator = {
          StartAt = "ProcessChunk",
          States = {
            ProcessChunk = {
              Type     = "Task",
              Resource = aws_lambda_function.lambda_function_L2.arn,
              End      = true
            }
          }
        },
        End = true
      }
    }
  })
}

resource "aws_sns_topic" "splitter_status" {
  name = "splitter_status"
}

resource "aws_sqs_queue" "my_queue" {
  name                       = "my-sqs-queue"
  visibility_timeout_seconds = 30
}

output "sqs_queue_url" {
  value       = aws_sqs_queue.my_queue.url
  description = "The full URL of the SQS queue"
}

# Add HTTP subscription for the local Express app.
resource "aws_sns_topic_subscription" "http_subscriber" {
  topic_arn = aws_sns_topic.splitter_status.arn
  protocol  = "http"
  endpoint  = "http://host.docker.internal:3000/sns"
}
