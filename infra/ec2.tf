resource "aws_key_pair" "main" {
  key_name   = "${var.project_name}-key"
  public_key = var.ssh_public_key
}

resource "aws_security_group" "app" {
  name        = "${var.project_name}-app-sg"
  description = "Ceaser app: SSH, HTTP, HTTPS"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  ingress {
    description = "HTTP (Caddy ACME challenge + redirect)"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_iam_role" "ec2" {
  name = "${var.project_name}-ec2-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRole"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "s3_uploads" {
  name = "${var.project_name}-s3-uploads"
  role = aws_iam_role.ec2.id
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
    }]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project_name}-ec2-profile"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "app" {
  ami                         = data.aws_ami.ubuntu_arm.id
  instance_type               = var.instance_type
  key_name                    = aws_key_pair.main.key_name
  vpc_security_group_ids      = [aws_security_group.app.id]
  subnet_id                   = tolist(data.aws_subnets.default.ids)[0]
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true

  user_data = templatefile("${path.module}/user-data.sh.tpl", {
    git_repo_url = var.git_repo_url
    git_branch   = var.git_branch
    api_fqdn     = local.api_fqdn
    s3_bucket    = aws_s3_bucket.uploads.bucket
    region       = var.region
    env_vars     = var.env_vars
  })

  root_block_device {
    volume_type = "gp3"
    volume_size = var.root_volume_gb
    encrypted   = true
  }

  tags = {
    Name = "${var.project_name}-app"
  }

  # Changing env_vars should not replace the instance — SSH in and re-run
  # /opt/ceaser/deploy.sh instead.
  lifecycle {
    ignore_changes = [user_data, ami]
  }
}

resource "aws_eip" "app" {
  instance = aws_instance.app.id
  domain   = "vpc"
}
