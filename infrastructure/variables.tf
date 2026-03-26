variable "ripple_sync_interval_minutes" {
  description = "How often to run the Ripple sync (in minutes)"
  type        = number
  default     = 5
}
variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "websocket_url" {
  description = "WebSocket URL"
  type        = string
}

variable "venv_path" {
  description = "Path to Python virtual environment"
  type        = string
}

variable "instance_name" {
  description = "EC2 instance name"
  type        = string
}

variable "user_group" {
  description = "User and group for running services"
  type        = string
}

variable "instance_id" {
  description = "EC2 instance ID"
  type        = string
}
