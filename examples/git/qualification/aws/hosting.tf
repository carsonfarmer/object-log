variable "host_service" {
  description = "Add the disposable HTTPS Git host and Cognito identities."
  type        = bool
  default     = false
}

variable "host_subnet_id" {
  description = "Existing public subnet; empty selects a default-VPC default subnet."
  type        = string
  default     = ""
}

variable "host_route53_zone_id" {
  description = "Existing public Route53 zone; no domain is registered."
  type        = string
  default     = ""
}

variable "host_name" {
  description = "Unused DNS name in host_route53_zone_id."
  type        = string
  default     = ""
  validation {
    condition     = var.host_name == "" || can(regex("^([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9]([a-z0-9-]*[a-z0-9])?$", var.host_name))
    error_message = "host_name must be a lowercase DNS name without a trailing dot."
  }
}

variable "host_artifact_path" {
  description = "Local tar.gz containing only the built spin.toml and git.wasm."
  type        = string
  default     = ""
}

variable "host_instance_type" {
  description = "x86_64 EC2 instance type; qualify capacity before choosing workloads."
  type        = string
  default     = "t3.xlarge"
}

variable "host_repositories" {
  description = "Explicit repository provisioning and independent group permissions."
  type = map(object({
    log_id         = string
    format         = string
    default_branch = optional(string, "main")
    read_groups    = optional(list(string), [])
    write_groups   = optional(list(string), [])
    admin_groups   = optional(list(string), [])
  }))
  default = {}
  validation {
    condition     = alltrue([for repo in var.host_repositories : contains(["sha1", "sha256"], repo.format)])
    error_message = "Repository format must be sha1 or sha256."
  }
}

variable "host_groups" {
  description = "Cognito groups to declare; users and membership are managed separately."
  type        = set(string)
  default     = []
}

variable "host_maintenance_interval" {
  description = "Delay after each completed worker run, in seconds."
  type        = number
  default     = 300
  validation {
    condition     = var.host_maintenance_interval >= 1 && floor(var.host_maintenance_interval) == var.host_maintenance_interval
    error_message = "host_maintenance_interval must be a positive integer."
  }
}

variable "host_maintenance_budget_seconds" {
  description = "Maximum worker time per repository, including all HTTP calls."
  type        = number
  default     = 300
  validation {
    condition     = var.host_maintenance_budget_seconds >= 1 && floor(var.host_maintenance_budget_seconds) == var.host_maintenance_budget_seconds
    error_message = "host_maintenance_budget_seconds must be a positive integer."
  }
}

variable "host_maintenance_pause_seconds" {
  description = "Optional per-repository ingress pause; zero disables it. Never clears reader retentions."
  type        = number
  default     = 0
  validation {
    condition = (
      var.host_maintenance_pause_seconds >= 0 &&
      floor(var.host_maintenance_pause_seconds) == var.host_maintenance_pause_seconds &&
      var.host_maintenance_pause_seconds <= var.host_maintenance_budget_seconds
    )
    error_message = "host_maintenance_pause_seconds must be an integer between zero and the repository budget."
  }
}

data "aws_vpc" "default" {
  count   = var.host_service && var.host_subnet_id == "" ? 1 : 0
  default = true
}

data "aws_subnets" "default" {
  count = var.host_service && var.host_subnet_id == "" ? 1 : 0
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

data "aws_subnet" "host" {
  count = var.host_service ? 1 : 0
  id    = var.host_subnet_id != "" ? var.host_subnet_id : sort(data.aws_subnets.default[0].ids)[0]
}

data "aws_route53_zone" "host" {
  count        = var.host_service ? 1 : 0
  zone_id      = var.host_route53_zone_id
  private_zone = false
}

data "aws_ami" "ubuntu" {
  count       = var.host_service ? 1 : 0
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "state"
    values = ["available"]
  }
}

locals {
  host_artifact_sha = var.host_service ? filesha256(var.host_artifact_path) : ""
  host_artifact_key = "${var.object_prefix}/artifacts/${local.host_artifact_sha}.tar.gz"
  host_wal_prefix   = "${var.object_prefix}/git"
  host_variables = var.host_service ? {
    wal_endpoint                   = "https://s3.${var.aws_region}.amazonaws.com"
    wal_bucket                     = aws_s3_bucket.qualification.bucket
    wal_region                     = var.aws_region
    wal_prefix                     = local.host_wal_prefix
    wal_credential_mode            = "instance-role"
    git_repositories               = jsonencode(var.host_repositories)
    git_auth_mode                  = "cognito"
    git_cognito_issuer             = "https://${aws_cognito_user_pool.git[0].endpoint}"
    git_cognito_host               = "https://cognito-idp.${var.aws_region}.amazonaws.com"
    git_cognito_client_id          = aws_cognito_user_pool_client.git[0].id
    git_cognito_operator_client_id = aws_cognito_user_pool_client.maintenance[0].id
    git_cognito_scope              = "git/access"
  } : {}
  host_bootstrap = var.host_service ? templatefile("${path.module}/host-bootstrap.sh.tftpl", {
    region             = var.aws_region
    artifact_uri       = "s3://${aws_s3_bucket.qualification.bucket}/${aws_s3_object.host_artifact[0].key}"
    artifact_sha       = local.host_artifact_sha
    variables_base64   = base64encode(jsonencode(local.host_variables))
    hostname           = var.host_name
    maintenance_script = base64encode(file("${path.module}/maintenance.py"))
    maintenance_config = base64encode(jsonencode({
      region           = var.aws_region
      secret_parameter = aws_ssm_parameter.maintenance_secret[0].name
      client_id        = aws_cognito_user_pool_client.maintenance[0].id
      token_url        = "${local.cognito_login_origin}/oauth2/token"
      service_url      = "https://${var.host_name}"
      repositories     = sort(keys(var.host_repositories))
      budget_seconds   = var.host_maintenance_budget_seconds
      pause_seconds    = var.host_maintenance_pause_seconds
    }))
    interval    = var.host_maintenance_interval
    run_timeout = length(var.host_repositories) * var.host_maintenance_budget_seconds + 65
    retry_after = max(1, length(var.host_repositories) * var.host_maintenance_pause_seconds)
    admin_paths = jsonencode(flatten([for name in sort(keys(var.host_repositories)) : [
      for operation in ["maintenance", "collect", "recover-retentions-after-drain"] : "/${name}/${operation}"
    ]]))
  }) : ""
}

resource "aws_s3_object" "host_artifact" {
  count       = var.host_service ? 1 : 0
  bucket      = aws_s3_bucket.qualification.id
  key         = local.host_artifact_key
  source      = var.host_artifact_path
  source_hash = local.host_artifact_sha
}

resource "aws_iam_role" "host" {
  count = var.host_service ? 1 : 0
  name  = "object-log-host-${var.run_id}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow", Action = "sts:AssumeRole", Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "host_ssm" {
  count      = var.host_service ? 1 : 0
  role       = aws_iam_role.host[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "host" {
  count = var.host_service ? 1 : 0
  role  = aws_iam_role.host[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.qualification.arn
        Condition = { StringLike = { "s3:prefix" = [local.host_wal_prefix, "${local.host_wal_prefix}/*"] } }
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
        Resource = "${aws_s3_bucket.qualification.arn}/${local.host_wal_prefix}/*"
      },
      {
        Effect   = "Allow", Action = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.qualification.arn}/${local.host_artifact_key}"
      },
      {
        Effect = "Allow", Action = ["ssm:GetParameter"], Resource = aws_ssm_parameter.maintenance_secret[0].arn
      },
      {
        # The managed SSM core policy otherwise permits all Parameter Store reads.
        Effect      = "Deny", Action = ["ssm:GetParameter", "ssm:GetParameters"]
        NotResource = aws_ssm_parameter.maintenance_secret[0].arn
      },
    ]
  })
}

resource "aws_iam_instance_profile" "host" {
  count = var.host_service ? 1 : 0
  name  = "object-log-host-${var.run_id}"
  role  = aws_iam_role.host[0].name
}

resource "aws_security_group" "host" {
  count       = var.host_service ? 1 : 0
  name_prefix = "object-log-${var.run_id}-"
  description = "HTTPS Git and ACME; management uses SSM, not SSH"
  vpc_id      = data.aws_subnet.host[0].vpc_id
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
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

resource "aws_instance" "host" {
  count                       = var.host_service ? 1 : 0
  ami                         = data.aws_ami.ubuntu[0].id
  instance_type               = var.host_instance_type
  subnet_id                   = data.aws_subnet.host[0].id
  associate_public_ip_address = true
  vpc_security_group_ids      = [aws_security_group.host[0].id]
  iam_instance_profile        = aws_iam_instance_profile.host[0].name
  user_data_base64            = base64gzip(local.host_bootstrap)
  user_data_replace_on_change = true
  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "disabled"
  }
  root_block_device {
    encrypted = true
  }
  tags       = { Name = "object-log-${var.run_id}" }
  depends_on = [aws_iam_role_policy.host, aws_iam_role_policy_attachment.host_ssm]
  lifecycle {
    precondition {
      condition = (
        !data.aws_route53_zone.host[0].private_zone &&
        (var.host_name == trimsuffix(data.aws_route53_zone.host[0].name, ".") ||
        endswith(var.host_name, ".${trimsuffix(data.aws_route53_zone.host[0].name, ".")}"))
      )
      error_message = "host_name must belong to the existing public Route53 zone."
    }
    precondition {
      condition     = length(var.host_repositories) > 0 && length(jsonencode(var.host_repositories)) <= 65536
      error_message = "Hosting requires a nonempty repository map of at most 64 KiB."
    }
    precondition {
      condition = alltrue(flatten([for repo in var.host_repositories : [
        for group in concat(repo.read_groups, repo.write_groups, repo.admin_groups) : contains(var.host_groups, group)
      ]]))
      error_message = "Every repository policy group must be declared in host_groups."
    }
    precondition {
      condition     = length(base64gzip(local.host_bootstrap)) <= 21844
      error_message = "Compressed bootstrap exceeds EC2's 16 KiB user-data limit."
    }
  }
}

resource "aws_eip" "host" {
  count    = var.host_service ? 1 : 0
  domain   = "vpc"
  instance = aws_instance.host[0].id
}

resource "aws_route53_record" "host" {
  count   = var.host_service ? 1 : 0
  zone_id = data.aws_route53_zone.host[0].zone_id
  name    = var.host_name
  type    = "A"
  ttl     = 60
  records = [aws_eip.host[0].public_ip]
}

output "host_service_url" {
  value = var.host_service ? "https://${var.host_name}" : null
}

output "host_instance_id" {
  value = var.host_service ? aws_instance.host[0].id : null
}
