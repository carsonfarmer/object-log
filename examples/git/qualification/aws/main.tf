terraform {
  required_version = ">= 1.10.0"
  backend "local" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  profile = var.aws_profile
  region  = var.aws_region
  default_tags {
    tags = {
      Campaign = var.campaign_id
      Project  = "object-log"
      Purpose  = "live-s3-qualification"
    }
  }
}

variable "aws_profile" {
  type = string
}

variable "aws_region" {
  type = string
}

variable "campaign_id" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{0,38}$", var.campaign_id))
    error_message = "campaign_id must be 1-39 lowercase letters, digits, or hyphens."
  }
}

variable "bucket_name" {
  type = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be a 3-63 character S3 bucket name."
  }
}

variable "object_prefix" {
  type = string
  validation {
    condition = (
      can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*[A-Za-z0-9]$", var.object_prefix)) &&
      !strcontains(var.object_prefix, "//") &&
      !strcontains("/${var.object_prefix}/", "/./") &&
      !strcontains("/${var.object_prefix}/", "/../") &&
      strcontains(var.object_prefix, var.campaign_id)
    )
    error_message = "object_prefix must be normalized, relative, and contain campaign_id."
  }
}

resource "aws_s3_bucket" "qualification" {
  bucket        = var.bucket_name
  force_destroy = false
}

resource "aws_s3_bucket_ownership_controls" "qualification" {
  bucket = aws_s3_bucket.qualification.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "qualification" {
  bucket                  = aws_s3_bucket.qualification.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "qualification" {
  bucket = aws_s3_bucket.qualification.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# "Suspended" is not unversioned and would leave old versions at teardown.
resource "aws_s3_bucket_versioning" "qualification" {
  bucket = aws_s3_bucket.qualification.id
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_iam_user" "qualification" {
  name = "object-log-qualification-${var.campaign_id}"
  path = "/object-log-qualification/"
}

resource "aws_iam_user_policy" "qualification" {
  name = "object-log-live-s3-qualification"
  user = aws_iam_user.qualification.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadBucketConfiguration"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning",
          "s3:GetEncryptionConfiguration",
          "s3:GetLifecycleConfiguration",
        ]
        Resource = aws_s3_bucket.qualification.arn
      },
      {
        Sid      = "ListCampaignObjects"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:ListBucketVersions"]
        Resource = aws_s3_bucket.qualification.arn
        Condition = {
          StringLike = {
            "s3:prefix" = [var.object_prefix, "${var.object_prefix}/*"]
          }
        }
      },
      {
        Sid    = "UseCampaignObjects"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:ListMultipartUploadParts",
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.qualification.arn}/${var.object_prefix}/*"
      },
    ]
  })
}

output "aws_region" {
  value = var.aws_region
}

output "bucket_name" {
  value = aws_s3_bucket.qualification.bucket
}

output "object_prefix" {
  value = var.object_prefix
}

output "qualification_user_name" {
  value = aws_iam_user.qualification.name
}
