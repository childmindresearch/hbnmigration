variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment (dev, staging, production)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Must be dev, staging, or production"
  }
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for EC2 instance"
  type        = string
}

variable "ami_id" {
  description = "AMI ID (Ubuntu 22.04 recommended)"
  type        = string
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.small"
}

variable "key_name" {
  description = "EC2 key pair name"
  type        = string
}

variable "ssh_allowed_cidrs" {
  description = "CIDR blocks allowed for SSH (leave empty to disable SSH)"
  type        = list(string)
  default     = []
}

variable "webhook_allowed_cidrs" {
  description = "CIDR blocks allowed for webhooks (REDCap server IPs)"
  type        = list(string)
  validation {
    condition     = length(var.webhook_allowed_cidrs) > 0
    error_message = "Must specify at least one CIDR for webhook access"
  }
}

variable "service_user" {
  description = "System user for services"
  type        = string
  default     = "hbnmigration"
}

variable "service_group" {
  description = "System group for services"
  type        = string
  default     = "hbnmigration"
}

variable "working_directory" {
  description = "Working directory"
  type        = string
  default     = "/opt/hbnmigration"
}

variable "python_venv" {
  description = "Python virtual environment path"
  type        = string
  default     = "/opt/hbnmigration/venv"
}

variable "github_repo" {
  description = "GitHub repository URL"
  type        = string
  default     = "https://github.com/childmindresearch/hbnmigration.git"
}

variable "github_branch" {
  description = "GitHub branch"
  type        = string
  default     = "main"
}
