variable "project_root" {
  description = "Root directory of the project"
  type        = string
  default     = "/opt/hbnmigration"
}

variable "venv_path" {
  description = "Path to Python virtual environment (relative to project_root or absolute)"
  type        = string
  default     = "venv"
}

variable "user_group" {
  description = "User and group for running services (format: 'user' or 'user:group')"
  type        = string
  validation {
    condition     = can(regex("^[a-z_][a-z0-9_-]*[$]?(:[a-z_][a-z0-9_-]*[$]?)?$", var.user_group))
    error_message = "The user_group must be a valid Unix username, optionally followed by ':groupname'."
  }
}

variable "sync_interval_minutes" {
  description = "How often to run the API sync (in minutes)"
  type        = number
  default     = 5
  validation {
    condition     = var.sync_interval_minutes > 0 && var.sync_interval_minutes <= 1440
    error_message = "The sync_interval_minutes must be between 1 and 1440 (24 hours)."
  }
}

variable "log_directory" {
  description = "Directory for service logs (relative to project_root or absolute)"
  type        = string
  default     = "/var/log/hbnmigration"
}

variable "project_status" {
  description = "Project status (dev or prod) for HBN migration"
  type        = string
  default     = "prod"
  validation {
    condition     = contains(["dev", "prod"], lower(var.project_status))
    error_message = "The project_status must be either 'dev' or 'prod'."
  }
}

variable "recovery_mode" {
  description = "Enable recovery mode for full-day data pull on downtime"
  type        = bool
  default     = false
}
