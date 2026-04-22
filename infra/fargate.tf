# ─── ECS Fargate: File Processing Pipeline ──────────────────────────────────
# Runs the full Excel orchestrator (8-step pipeline) as an on-demand Fargate
# task. Uses the same Docker image as the backend with a different entrypoint.

resource "aws_ecs_cluster" "processing" {
  name = "${var.project_name}-processing"

  setting {
    name  = "containerInsights"
    value = "disabled"
  }
}

# ─── ECR repo (reuses backend image, but needs its own for task definition) ──
resource "aws_ecr_repository" "file_processor" {
  name                 = "${var.project_name}-file-processor"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = false
  }
}

resource "aws_ecr_lifecycle_policy" "file_processor" {
  repository = aws_ecr_repository.file_processor.name
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

# ─── IAM: Task execution role (pulls image, writes logs) ────────────────────
resource "aws_iam_role" "fargate_execution" {
  name = "${var.project_name}-fargate-execution"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "fargate_execution" {
  role       = aws_iam_role.fargate_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ─── IAM: Task role (S3 access for the running container) ───────────────────
resource "aws_iam_role" "fargate_task" {
  name = "${var.project_name}-fargate-task"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "fargate_task_s3" {
  name = "${var.project_name}-fargate-s3"
  role = aws_iam_role.fargate_task.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
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
    {
      Effect   = "Allow"
      Action   = ["bedrock:InvokeModel", "bedrock:InvokeModelWithResponseStream"]
      Resource = "*"
    }]
  })
}

# ─── Security group (egress-only — needs internet for S3, LLM APIs, callback)
resource "aws_security_group" "fargate_task" {
  name        = "${var.project_name}-fargate-sg"
  description = "Fargate file processor: egress only"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ─── CloudWatch log group ───────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "fargate_processor" {
  name              = "/ecs/${var.project_name}-file-processor"
  retention_in_days = 14
}

# ─── Task definition ────────────────────────────────────────────────────────
variable "fargate_processor_image_tag" {
  type        = string
  default     = "latest"
  description = "Tag of the file-processor image in ECR"
}

resource "aws_ecs_task_definition" "file_processor" {
  count  = var.deploy_fargate ? 1 : 0
  family = "${var.project_name}-file-processor"

  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"  # 2 vCPU
  memory                   = "8192"  # 8 GB — handles large multi-sheet Excel files
  execution_role_arn       = aws_iam_role.fargate_execution.arn
  task_role_arn            = aws_iam_role.fargate_task.arn

  container_definitions = jsonencode([{
    name      = "processor"
    image     = "${aws_ecr_repository.file_processor.repository_url}:${var.fargate_processor_image_tag}"
    essential = true

    command = ["python", "-m", "app.tasks.process_file"]

    environment = [
      { name = "STORAGE_BACKEND", value = "s3" },
      { name = "PARQUET_S3_BUCKET", value = aws_s3_bucket.uploads.bucket },
      { name = "AWS_REGION", value = var.region },
      { name = "BACKEND_CALLBACK_URL", value = "https://${local.api_fqdn}/api/v1" },
      { name = "HMAC_SHARED_SECRET", value = random_password.hmac_secret.result },
      { name = "LLM_PROVIDER", value = "bedrock" },
    ]

    # FILE_ID, S3_BUCKET, S3_KEY, ORG_ID are passed via container overrides at runtime

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.fargate_processor.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "task"
      }
    }
  }])

  lifecycle {
    ignore_changes = [container_definitions]
  }
}

# ─── Grant EC2 permission to launch Fargate tasks ───────────────────────────
resource "aws_iam_role_policy" "ec2_run_fargate" {
  count = var.deploy_fargate ? 1 : 0
  name  = "${var.project_name}-ec2-run-fargate"
  role  = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "ecs:RunTask"
        Resource = aws_ecs_task_definition.file_processor[0].arn
      },
      {
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = [
          aws_iam_role.fargate_execution.arn,
          aws_iam_role.fargate_task.arn,
        ]
      },
    ]
  })
}
