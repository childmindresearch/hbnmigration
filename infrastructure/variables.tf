variable "project_root" {
  description = "Base directory for the application"
  type        = string
  default     = "/data/hbnmigration"
}

variable "user_group" {
  description = "System user/group for running services"
  type        = string
  default     = "hbnmigration"
}

variable "venv_path" {
  description = "Python virtual environment path (relative to project_root or absolute)"
  type        = string
  default     = "python_jobs/.venv"
}

variable "log_directory" {
  description = "Where logs are stored"
  type        = string
  default     = "/data/logs/hbnmigration"
}

variable "sync_interval_minutes" {
  description = "How often the batch sync timer runs (minutes)"
  type        = number
  default     = 1
  validation {
    condition     = var.sync_interval_minutes >= 1 && var.sync_interval_minutes <= 1440
    error_message = "Must be between 1 and 1440 minutes"
  }
}

variable "project_status" {
  description = "Project status (prod, dev, test)"
  type        = string
  default     = "prod"
}

variable "recovery_mode" {
  description = "Enable recovery mode"
  type        = bool
  default     = false
}
