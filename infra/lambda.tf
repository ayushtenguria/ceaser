# ─── ECR repo for the Excel processor image ─────────────────────────────────
resource "aws_ecr_repository" "excel_processor" {
  name                 = "${var.project_name}-excel-processor"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "aws_ecr_lifecycle_policy" "excel_processor" {
  repository = aws_ecr_repository.excel_processor.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

# ─── SQS queue (processing jobs) + DLQ ───────────────────────────────────────
resource "aws_sqs_queue" "excel_processing_dlq" {
  name                      = "${var.project_name}-excel-processing-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_sqs_queue" "excel_processing" {
  name                       = "${var.project_name}-excel-processing"
  visibility_timeout_seconds = 960 # must be ≥ Lambda timeout (900) + 60s buffer
  message_retention_seconds  = 345600
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.excel_processing_dlq.arn
    maxReceiveCount     = 3
  })
}

# Allow the uploads S3 bucket to send ObjectCreated events to this queue.
resource "aws_sqs_queue_policy" "excel_processing" {
  queue_url = aws_sqs_queue.excel_processing.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.excel_processing.arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = aws_s3_bucket.uploads.arn
        }
      }
    }]
  })
}

# S3 → SQS event notification: fire on any new object under `uploads/`
resource "aws_s3_bucket_notification" "uploads" {
  bucket = aws_s3_bucket.uploads.id

  queue {
    queue_arn     = aws_sqs_queue.excel_processing.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "uploads/"
  }

  depends_on = [aws_sqs_queue_policy.excel_processing]
}

# ─── Shared HMAC secret for Lambda ↔ backend callback auth ───────────────────
resource "random_password" "hmac_secret" {
  length  = 48
  special = false
}

# ─── IAM role for the Lambda function ────────────────────────────────────────
resource "aws_iam_role" "lambda_exec" {
  name = "${var.project_name}-lambda-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_exec" {
  name = "${var.project_name}-lambda-policy"
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility",
        ]
        Resource = aws_sqs_queue.excel_processing.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.uploads.arn,
          "${aws_s3_bucket.uploads.arn}/*",
        ]
      },
    ]
  })
}

# ─── Lambda function (container image) ───────────────────────────────────────
# image_uri is set once the image has been built + pushed to ECR.
# Use `:placeholder` for first apply; `terraform apply` will then update after
# the image is pushed and image_tag is bumped.
variable "lambda_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the excel_processor image in ECR"
}

resource "aws_lambda_function" "excel_processor" {
  count         = var.deploy_lambda ? 1 : 0
  function_name = "${var.project_name}-excel-processor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.excel_processor.repository_url}:${var.lambda_image_tag}"
  timeout       = 900
  memory_size   = 3008  # default account quota; request AWS Support increase for up to 10240
  architectures = ["arm64"]

  ephemeral_storage {
    size = 4096 # /tmp — need room for source file + parquet output + temp buffers
  }

  environment {
    variables = {
      PARQUET_BUCKET       = aws_s3_bucket.uploads.bucket
      BACKEND_CALLBACK_URL = "https://${local.api_fqdn}/api/v1"
      HMAC_SHARED_SECRET   = random_password.hmac_secret.result
    }
  }

  lifecycle {
    ignore_changes = [image_uri]
  }

  depends_on = [aws_iam_role_policy.lambda_exec]
}

resource "aws_lambda_event_source_mapping" "excel_processing" {
  count                              = var.deploy_lambda ? 1 : 0
  event_source_arn                   = aws_sqs_queue.excel_processing.arn
  function_name                      = aws_lambda_function.excel_processor[0].arn
  batch_size                         = 1
  maximum_batching_window_in_seconds = 0
  function_response_types            = ["ReportBatchItemFailures"]
}

# ─── Sandbox Executor Lambda ────────────────────────────────────────────────
# Runs Python code (pandas/plotly) for chat queries, offloading heavy compute
# from EC2 (2GB) to Lambda (up to 10GB).

resource "aws_ecr_repository" "sandbox_executor" {
  name                 = "${var.project_name}-sandbox-executor"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "aws_ecr_lifecycle_policy" "sandbox_executor" {
  repository = aws_ecr_repository.sandbox_executor.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = { type = "expire" }
    }]
  })
}

variable "sandbox_lambda_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the sandbox_executor image in ECR"
}

resource "aws_lambda_function" "sandbox_executor" {
  count         = var.deploy_lambda ? 1 : 0
  function_name = "${var.project_name}-sandbox-executor"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.sandbox_executor.repository_url}:${var.sandbox_lambda_image_tag}"
  timeout       = 120
  memory_size   = 3008
  architectures = ["arm64"]

  ephemeral_storage {
    size = 2048
  }

  environment {
    variables = {
      # Sandbox Lambda needs S3 access to read data files via presigned URLs.
      # No callback URL needed — results returned synchronously.
      PARQUET_BUCKET = aws_s3_bucket.uploads.bucket
    }
  }

  lifecycle {
    ignore_changes = [image_uri]
  }

  depends_on = [aws_iam_role_policy.lambda_exec]
}

# Grant EC2 permission to invoke the sandbox Lambda
resource "aws_iam_role_policy" "ec2_invoke_sandbox" {
  count = var.deploy_lambda ? 1 : 0
  name  = "${var.project_name}-ec2-invoke-sandbox"
  role  = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lambda:InvokeFunction"
      Resource = aws_lambda_function.sandbox_executor[0].arn
    }]
  })
}
