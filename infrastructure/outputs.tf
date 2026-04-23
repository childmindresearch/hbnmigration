output "configuration" {
  description = "Deployed configuration"
  value = {
    workspace             = terraform.workspace
    project_root          = var.project_root
    venv_path             = local.venv_full_path
    user_group            = var.user_group
    sync_interval_minutes = var.sync_interval_minutes
    log_directory         = local.log_full_path
    backup_directory      = "${local.log_full_path}/terraform-backups"
  }
}

output "services" {
  description = "Deployed services"
  value = {
    timer = local.services.hbn_sync_timer
    always_on = [
      "${local.services.curious_alerts_websocket}.service"
    ]
    timer_triggered = [
      "${local.services.ripple_sync}.service",
      "${local.services.redcap_sync}.service",
      "${local.services.redcap_to_curious}.service",
      "${local.services.curious_accounts_to_redcap}.service",
      "${local.services.curious_data_to_redcap}.service",
    ]
  }
}

output "user_info" {
  description = "User and group information"
  value       = <<-EOT
    User/Group: ${var.user_group}
    Workspace: ${terraform.workspace}

    Verify user exists:
      id ${split(":", var.user_group)[0]}

    Check user groups:
      groups ${split(":", var.user_group)[0]}
  EOT
}

output "permissions_summary" {
  description = "Summary of file permissions"
  value       = <<-EOT
    Workspace: ${terraform.workspace}

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
    Workspace: ${terraform.workspace}

    Automated Backups:
      Script: /usr/local/bin/terraform-state-backup${local.workspace_suffix}
      Cron: /etc/cron.daily/terraform-state-backup${local.workspace_suffix}
      Directory: ${local.log_full_path}/terraform-backups/

    Manual backup:
      sudo /usr/local/bin/terraform-state-backup${local.workspace_suffix}

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
    Workspace: ${terraform.workspace}

    Timer Status:
      sudo systemctl status ${local.services.hbn_sync_timer}
      sudo systemctl list-timers ${local.services.hbn_sync_timer}

    Always-On Service:
      sudo systemctl status ${local.services.curious_alerts_websocket}.service
      sudo journalctl -u ${local.services.curious_alerts_websocket}.service -f

    Timer-Triggered Services (last runs):
      sudo systemctl status ${local.services.ripple_sync}.service
      sudo systemctl status ${local.services.redcap_sync}.service
      sudo systemctl status ${local.services.redcap_to_curious}.service
      sudo systemctl status ${local.services.curious_accounts_to_redcap}.service
      sudo systemctl status ${local.services.curious_data_to_redcap}.service

    View all sync logs:
      sudo journalctl -u ${local.services.ripple_sync}.service \
                      -u ${local.services.redcap_sync}.service \
                      -u ${local.services.redcap_to_curious}.service \
                      -u ${local.services.curious_accounts_to_redcap}.service \
                      -u ${local.services.curious_data_to_redcap}.service -f

    View application logs:
      tail -f ${local.log_full_path}/*.log
      tail -f ${local.log_full_path}/curious-alerts-websocket.log
      tail -f ${local.log_full_path}/ripple-to-redcap.log

    Check permissions:
      ls -la ${var.project_root}
      ls -la ${var.project_root}/python_jobs/src/hbnmigration/_config_variables
      ls -la ${local.log_full_path}

    Check user/group:
      id ${split(":", var.user_group)[0]}
      groups ${split(":", var.user_group)[0]}

    Check backups:
      ls -lh ${local.log_full_path}/terraform-backups/
      cat ${local.log_full_path}/terraform-backups/backup.log
  EOT
}

output "quick_troubleshooting" {
  description = "Quick troubleshooting guide"
  value       = <<-EOT
    Workspace: ${terraform.workspace}
    Sync Interval: Every ${var.sync_interval_minutes} minute(s)

    Check if timer is running:
      sudo systemctl is-active ${local.services.hbn_sync_timer}

    Check when services will run next:
      sudo systemctl list-timers ${local.services.hbn_sync_timer}

    Manually trigger a sync cycle:
      sudo systemctl start ${local.services.hbn_sync}.service

    Restart websocket service:
      sudo systemctl restart ${local.services.curious_alerts_websocket}.service

    View recent failures:
      sudo systemctl --failed
      sudo journalctl -p err -u ${local.services.ripple_sync}.service --since "1 hour ago"

    Reset failed services:
      sudo systemctl reset-failed

    Check service timeouts (should be 300s for sync, 60s for websocket):
      systemctl show ${local.services.ripple_sync}.service -p TimeoutStartUSec
      systemctl show ${local.services.curious_alerts_websocket}.service -p TimeoutStartUSec
  EOT
}
