terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- Default VPC / subnet (demo lift-and-shift, no custom networking) ------

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# --- S3 warehouse bucket -----------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "warehouse" {
  bucket = "lakehouse-${random_id.bucket_suffix.hex}"
}

# --- Glue Data Catalog: bootstrap namespace ---------------------------------
# analytics/dim/etc namespaces are created on demand by dbt/streaming via
# CREATE DATABASE IF NOT EXISTS, which GlueCatalog supports at runtime. This
# one is just to prove the IAM wiring.

resource "aws_glue_catalog_database" "staging" {
  name = "staging"
}

# --- Security group -----------------------------------------------------------

resource "aws_security_group" "instance" {
  name        = "lakehouse-demo-sg"
  description = "Lift-and-shift demo instance: SSH + Spark UI + Grafana + Prometheus"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_admin_cidr]
  }

  ingress {
    description = "Spark master UI"
    from_port   = 8086
    to_port     = 8086
    protocol    = "tcp"
    cidr_blocks = [var.allowed_admin_cidr]
  }

  ingress {
    description = "Grafana"
    from_port   = 3000
    to_port     = 3000
    protocol    = "tcp"
    cidr_blocks = [var.allowed_admin_cidr]
  }

  ingress {
    description = "Prometheus"
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = [var.allowed_admin_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- IAM: EC2 instance profile with scoped S3 + Glue access -----------------
# No AWS access keys anywhere - auth flows through this instance profile via
# the AWS SDK v2 default credential chain.

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "instance" {
  name = "lakehouse-demo-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "instance" {
  name = "lakehouse-demo-instance-policy"
  role = aws_iam_role.instance.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Warehouse"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.warehouse.arn,
          "${aws_s3_bucket.warehouse.arn}/*",
        ]
      },
      {
        Sid    = "GlueCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:BatchCreatePartition",
          "glue:GetPartitions",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:database/*",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/*/*",
        ]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "instance" {
  name = "lakehouse-demo-instance-profile"
  role = aws_iam_role.instance.name
}

# --- EC2 instance ------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }
}

resource "aws_instance" "lakehouse" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = data.aws_subnets.default.ids[0]
  vpc_security_group_ids = [aws_security_group.instance.id]
  iam_instance_profile   = aws_iam_instance_profile.instance.name

  user_data = templatefile("${path.module}/user_data.sh.tpl", {
    repo_url             = var.repo_url
    aws_region            = var.aws_region
    s3_warehouse_bucket   = aws_s3_bucket.warehouse.bucket
    glue_database         = aws_glue_catalog_database.staging.name
  })

  tags = {
    Name = "lakehouse-demo"
  }
}
