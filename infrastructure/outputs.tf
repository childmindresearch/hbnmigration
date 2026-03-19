output "configuration" {
  description = "Deployed configuration"
  value = {
    project_root          = var.project_root
    venv_path             = local.venv_full_path
    user_group            = var.user_group
    sync_interval_minutes = var.sync_interval_minutes
    log_directory         = local.log_full_path
    backup_directory      = "${local.log_full_path}/terraform-backups"
  }
}

output "user_info" {
  description = "User and group information"
  value       = <<-EOT
    User/Group: ${var.user_group}

    Verify user exists:
      id ${split(":", var.user_group)[0]}

    Check user groups:
      groups ${split(":", var.user_group)[0]}
  EOT
}

output "permissions_summary" {
  description = "Summary of file permissions"
  value       = <<-EOT
    Project Root: ${var.project_root}
      Owner: ${var.user_group}:${var.user_group}
      Directories: 750 (rwxr-x---)
      Files: 640 (rw-r-----)
      Executables: 750 (rwxr-x---)

    Config: ${var.project_root}/python_jobs/src/hbnmigration/_config_variables
      Owner: ${var.user_group}:${var.user_group}
      Permissions: 600 (rw-------)

    Logs: ${local.log_full_path}
      Owner: ${var.user_group}:${var.user_group}
      Permissions: 755 (rwxr-xr-x)
      Log Files: 640 (rw-r-----)
      Rotation: 14 days

    State Backups: ${local.log_full_path}/terraform-backups
      Owner: ${var.user_group}:${var.user_group}
      Permissions: 640 (rw-r-----)
      Retention: 30 timestamped, 7 daily
      Cron: Daily at 06:25 (via /etc/cron.daily/)
  EOT
}

output "backup_info" {
  description = "Backup configuration details"
  value       = <<-EOT
    Automated Backups:
      Script: /usr/local/bin/terraform-state-backup
      Cron: /etc/cron.daily/terraform-state-backup
      Directory: ${local.log_full_path}/terraform-backups/

    Manual backup:
      sudo /usr/local/bin/terraform-state-backup

    List backups:
      ls -lh ${local.log_full_path}/terraform-backups/
      ls -lh ${local.log_full_path}/terraform-backups/daily/

    Restore a backup:
      cp ${local.log_full_path}/terraform-backups/terraform.tfstate.YYYYMMDD-HHMMSS terraform.tfstate

    View backup log:
      cat ${local.log_full_path}/terraform-backups/backup.log
  EOT
}

output "monitoring_commands" {
  description = "Commands to monitor the services"
  value       = <<-EOT
    View logs:
      sudo journalctl -u curious-alerts-websocket -f
      sudo journalctl -u ripple-to-redcap -u redcap-to-redcap -u redcap-to-curious -f
      tail -f ${local.log_full_path}/*.log

    Check permissions:
      ls -la ${var.project_root}
      ls -la ${var.project_root}/python_jobs/src/hbnmigration/_config_variables
      ls -la ${local.log_full_path}

    Check service status:
      sudo systemctl status curious-alerts-websocket.service
      sudo systemctl status hbn-sync.timer
      sudo systemctl list-timers

    Check user/group:
      id ${split(":", var.user_group)[0]}
      groups ${split(":", var.user_group)[0]}

    Check backups:
      ls -lh ${local.log_full_path}/terraform-backups/
      cat ${local.log_full_path}/terraform-backups/backup.log
  EOT
}
