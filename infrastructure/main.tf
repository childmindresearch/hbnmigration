terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }

  backend "s3" {
    bucket         = "hbn-migration-terraform-state"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "hbn-migration-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "HBN Migration"
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# EC2 Instance
resource "aws_instance" "hbn_migration" {
  ami           = var.ami_id
  instance_type = var.instance_type
  key_name      = var.key_name
  ebs_optimized = true
  monitoring    = true

  vpc_security_group_ids = [aws_security_group.hbn_migration.id]
  subnet_id              = var.subnet_id

  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }

  root_block_device {
    encrypted = true
  }

  user_data = templatefile("${path.module}/user_data.sh", {
    github_repo       = var.github_repo
    github_branch     = var.github_branch
    working_directory = var.working_directory
    python_venv       = var.python_venv
    service_user      = var.service_user
    service_group     = var.service_group
  })

  tags = {
    Name = "hbn-migration-${var.environment}"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Security Group
resource "aws_security_group" "hbn_migration" {
  name        = "hbn-migration-${var.environment}"
  description = "Security group for HBN migration webhook services"
  vpc_id      = var.vpc_id

  # SSH access (conditional - only if CIDRs specified)
  dynamic "ingress" {
    for_each = length(var.ssh_allowed_cidrs) > 0 ? [1] : []
    content {
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.ssh_allowed_cidrs
      description = "SSH access"
    }
  }

  # REDCap to REDCap webhook
  ingress {
    from_port   = 8001
    to_port     = 8001
    protocol    = "tcp"
    cidr_blocks = var.webhook_allowed_cidrs
    description = "REDCap to Intake webhook"
  }

  # REDCap to Curious webhook
  ingress {
    from_port   = 8002
    to_port     = 8002
    protocol    = "tcp"
    cidr_blocks = var.webhook_allowed_cidrs
    description = "REDCap to Curious webhook"
  }

  # Outbound traffic (required for apt, pip, API calls to REDCap/Curious)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound traffic (required for updates and API calls)"
  }

  tags = {
    Name = "hbn-migration-${var.environment}"
  }
}

# Generate systemd service files
resource "local_file" "redcap_to_redcap_service" {
  content = templatefile("${path.module}/services/redcap-to-redcap.service.tpl", {
    service_user      = var.service_user
    service_group     = var.service_group
    working_directory = var.working_directory
    python_venv       = var.python_venv
  })
  filename = "${path.module}/generated/redcap-to-redcap.service"
}

resource "local_file" "redcap_to_curious_service" {
  content = templatefile("${path.module}/services/redcap-to-curious.service.tpl", {
    service_user      = var.service_user
    service_group     = var.service_group
    working_directory = var.working_directory
    python_venv       = var.python_venv
  })
  filename = "${path.module}/generated/redcap-to-curious.service"
}
