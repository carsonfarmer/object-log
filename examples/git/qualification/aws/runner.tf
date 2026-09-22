variable "qualification_runner" {
  description = "Add a disposable same-region host for native qualification commands."
  type        = bool
  default     = false
}

variable "qualification_runner_artifact_path" {
  description = "Local tar.gz containing the source tree to qualify."
  type        = string
  default     = ""
}

variable "qualification_runner_instance_type" {
  description = "x86_64 EC2 instance type used only for qualification."
  type        = string
  default     = "t3.xlarge"
}

variable "qualification_runner_ami_id" {
  description = "Exact x86_64 Amazon Linux 2023 AMI used by the qualification runner."
  type        = string
  default     = ""
}

variable "qualification_runner_lifetime_minutes" {
  description = "Maximum runner lifetime before the instance terminates itself."
  type        = number
  default     = 180
  validation {
    condition     = var.qualification_runner_lifetime_minutes >= 30 && var.qualification_runner_lifetime_minutes <= 480
    error_message = "qualification_runner_lifetime_minutes must be between 30 and 480."
  }
}

data "aws_vpc" "qualification_runner" {
  count   = var.qualification_runner ? 1 : 0
  default = true
}

data "aws_subnets" "qualification_runner" {
  count = var.qualification_runner ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.qualification_runner[0].id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

locals {
  qualification_runner_artifact_sha = var.qualification_runner ? filesha256(var.qualification_runner_artifact_path) : ""
  qualification_runner_artifact_key = "${var.object_prefix}/artifacts/kv-source-${local.qualification_runner_artifact_sha}.tar.gz"
}

resource "aws_s3_object" "qualification_runner" {
  count       = var.qualification_runner ? 1 : 0
  bucket      = aws_s3_bucket.qualification.id
  key         = local.qualification_runner_artifact_key
  source      = var.qualification_runner_artifact_path
  source_hash = local.qualification_runner_artifact_sha
}

resource "aws_iam_role" "qualification_runner" {
  count = var.qualification_runner ? 1 : 0
  name  = "object-log-runner-${var.run_id}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "qualification_runner_ssm" {
  count      = var.qualification_runner ? 1 : 0
  role       = aws_iam_role.qualification_runner[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "qualification_runner" {
  count = var.qualification_runner ? 1 : 0
  role  = aws_iam_role.qualification_runner[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.qualification.arn
        Condition = { StringLike = { "s3:prefix" = [var.object_prefix, "${var.object_prefix}/*"] } }
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = "${aws_s3_bucket.qualification.arn}/${var.object_prefix}/*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "qualification_runner" {
  count = var.qualification_runner ? 1 : 0
  name  = "object-log-runner-${var.run_id}"
  role  = aws_iam_role.qualification_runner[0].name
}

resource "aws_security_group" "qualification_runner" {
  count       = var.qualification_runner ? 1 : 0
  name_prefix = "object-log-runner-${var.run_id}-"
  description = "Egress-only qualification runner; management uses SSM."
  vpc_id      = data.aws_vpc.qualification_runner[0].id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "qualification_runner" {
  count                                = var.qualification_runner ? 1 : 0
  ami                                  = var.qualification_runner_ami_id
  instance_type                        = var.qualification_runner_instance_type
  subnet_id                            = sort(data.aws_subnets.qualification_runner[0].ids)[0]
  associate_public_ip_address          = true
  vpc_security_group_ids               = [aws_security_group.qualification_runner[0].id]
  iam_instance_profile                 = aws_iam_instance_profile.qualification_runner[0].name
  instance_initiated_shutdown_behavior = "terminate"
  user_data_replace_on_change          = true
  user_data                            = <<-SCRIPT
    #!/bin/bash
    set -euo pipefail
    export HOME=/root
    systemd-run --unit object-log-runner-expiry --on-active='${var.qualification_runner_lifetime_minutes}m' /usr/sbin/shutdown -h now
    dnf install -y gcc gcc-c++ git make openssl-devel perl pkgconf-pkg-config
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.97.1
    source /root/.cargo/env
    rustup target add wasm32-wasip2
    install -d /opt/object-log
    aws --region '${var.aws_region}' s3 cp \
      's3://${aws_s3_bucket.qualification.bucket}/${aws_s3_object.qualification_runner[0].key}' \
      /tmp/object-log-source.tar.gz
    printf '%s  %s\n' '${local.qualification_runner_artifact_sha}' /tmp/object-log-source.tar.gz | sha256sum --check
    tar -xzf /tmp/object-log-source.tar.gz -C /opt/object-log
    rm /tmp/object-log-source.tar.gz
  SCRIPT
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }
  root_block_device {
    encrypted   = true
    volume_size = 30
  }
  tags       = { Name = "object-log-runner-${var.run_id}" }
  depends_on = [aws_iam_role_policy.qualification_runner, aws_iam_role_policy_attachment.qualification_runner_ssm]
  lifecycle {
    precondition {
      condition     = var.qualification_runner_artifact_path != ""
      error_message = "qualification_runner_artifact_path is required when qualification_runner is enabled."
    }
    precondition {
      condition     = var.qualification_runner_ami_id != ""
      error_message = "qualification_runner_ami_id is required when qualification_runner is enabled."
    }
  }
}

output "qualification_runner_instance_id" {
  value = var.qualification_runner ? aws_instance.qualification_runner[0].id : null
}

output "qualification_runner_ami_id" {
  value = var.qualification_runner ? var.qualification_runner_ami_id : null
}
