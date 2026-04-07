terraform {
  required_version = ">= 1.5.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
  # Use local backend with workspace-aware state path
  backend "local" {
    path = "/opt/hbnmigration/terraform/terraform.tfstate"
  }
}

# Workspace-specific configuration
locals {
  workspace_suffix = terraform.workspace == "default" ? "" : "-${terraform.workspace}"
  workspace_dir    = var.project_root
  venv_full_path   = "${local.workspace_dir}/${var.venv_path}"
  log_full_path    = "${var.log_directory}${local.workspace_suffix}"

  # Workspace-specific service names
  service_prefix = terraform.workspace == "default" ? "" : "${terraform.workspace}-"

  services = {
    ripple_sync                = "${local.service_prefix}ripple-to-redcap"
    redcap_sync                = "${local.service_prefix}redcap-to-redcap"
    redcap_to_curious          = "${local.service_prefix}redcap-to-curious"
    curious_alerts_websocket   = "${local.service_prefix}curious-alerts-websocket"
    curious_accounts_to_redcap = "${local.service_prefix}curious-accounts-to-redcap"
    curious_data_to_redcap     = "${local.service_prefix}curious-data-to-redcap"
    hbn_sync_timer             = "${local.service_prefix}hbn-sync.timer"
  }
}

# Create user and group if they don't exist
resource "null_resource" "ensure_user_group" {
  triggers = {
    user_group = var.user_group
    workspace  = terraform.workspace
  }
  provisioner "local-exec" {
    command     = <<-EOT
      #!/bin/bash
      set -e
      USER="${var.user_group}"
      # Check if user exists
      if ! id "$USER" &>/dev/null; then
        echo "Creating user: $USER"
        sudo useradd --system --create-home --shell /bin/bash "$USER"
        echo "✓ User $USER created"
      else
        echo "✓ User $USER already exists"
      fi
      # Check if group exists (in case user:group format is used)
      if [[ "$USER" == *":"* ]]; then
        GROUP="$${USER#*:}"
        USER="$${USER%:*}"
        if ! getent group "$GROUP" &>/dev/null; then
          echo "Creating group: $GROUP"
          sudo groupadd --system "$GROUP"
          echo "✓ Group $GROUP created"
        else
          echo "✓ Group $GROUP already exists"
        fi
        # Add user to group
        if ! id -nG "$USER" | grep -qw "$GROUP"; then
          echo "Adding user $USER to group $GROUP"
          sudo usermod -a -G "$GROUP" "$USER"
          echo "✓ User $USER added to group $GROUP"
        else
          echo "✓ User $USER is already in group $GROUP"
        fi
      fi
      # Verify user exists
      if id "$USER" &>/dev/null; then
        echo "✓ User verification successful"
        id "$USER"
      else
        echo "ERROR: User $USER does not exist after creation attempt"
        exit 1
      fi
      echo "✓ Workspace: ${terraform.workspace}"
    EOT
    interpreter = ["bash", "-c"]
  }
}

# Generate systemd service files from templates
resource "local_file" "ripple_sync_service" {
  content = templatefile("${path.module}/services/ripple-to-redcap.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.ripple_sync}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "redcap_sync_service" {
  content = templatefile("${path.module}/services/redcap-to-redcap.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.redcap_sync}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "redcap_to_curious_service" {
  content = templatefile("${path.module}/services/redcap-to-curious.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.redcap_to_curious}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "curious_alerts_websocket_service" {
  content = templatefile("${path.module}/services/curious-alerts-websocket.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.curious_alerts_websocket}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "curious_accounts_to_redcap_service" {
  content = templatefile("${path.module}/services/curious-accouts-to-redcap.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.curious_accounts_to_redcap}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "curious_data_to_redcap_service" {
  content = templatefile("${path.module}/services/curious-data-to-redcap.service.tpl", {
    user_group     = var.user_group
    project_root   = local.workspace_dir
    venv_path      = local.venv_full_path
    log_directory  = local.log_full_path
    workspace      = terraform.workspace
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  })
  filename   = "${path.module}/.generated/${local.services.curious_data_to_redcap}.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "hbn_sync_service" {
  content = templatefile("${path.module}/services/hbn-sync.service.tpl", {
    services = [
      local.services.ripple_sync,
      local.services.redcap_sync,
      local.services.redcap_to_curious,
      local.services.curious_accounts_to_redcap,
      local.services.curious_alerts_websocket,
      local.services.curious_data_to_redcap
    ]
  })
  filename   = "${path.module}/.generated/hbn-sync.service"
  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "hbn_sync_timer" {
  content = templatefile("${path.module}/services/hbn-sync.timer.tpl", {
    sync_interval_minutes = var.sync_interval_minutes
    service_prefix        = local.service_prefix
    workspace             = terraform.workspace
  })
  filename   = "${path.module}/.generated/${local.services.hbn_sync_timer}"
  depends_on = [null_resource.ensure_user_group]
}

# Create workspace-specific directories
resource "null_resource" "create_workspace_dirs" {
  triggers = {
    workspace     = terraform.workspace
    workspace_dir = local.workspace_dir
    log_dir       = local.log_full_path
  }

  provisioner "local-exec" {
    command = <<-EOT
      echo "Setting up workspace: ${terraform.workspace}"
      sudo mkdir -p ${local.workspace_dir}
      sudo mkdir -p ${local.log_full_path}
      echo "✓ Workspace directories created"
      echo "  - Project: ${local.workspace_dir}"
      echo "  - Logs: ${local.log_full_path}"
    EOT
  }

  depends_on = [null_resource.ensure_user_group]
}

# Deploy generated service files
resource "null_resource" "deploy_services" {
  triggers = {
    workspace = terraform.workspace
    services_hash = sha256(join("", [
      local_file.ripple_sync_service.content,
      local_file.redcap_sync_service.content,
      local_file.redcap_to_curious_service.content,
      local_file.curious_alerts_websocket_service.content,
      local_file.curious_accounts_to_redcap_service.content,
      local_file.curious_data_to_redcap_service.content,
      local_file.hbn_sync_service.content,
      local_file.hbn_sync_timer.content,
    ]))
  }
  provisioner "local-exec" {
    command = <<-EOT
      sudo cp ${path.module}/.generated/*.service /etc/systemd/system/
      sudo cp ${path.module}/.generated/*.timer /etc/systemd/system/
      sudo systemctl daemon-reload
      echo "✓ Services deployed for workspace: ${terraform.workspace}"
    EOT
  }
  depends_on = [
    null_resource.create_workspace_dirs,
    local_file.ripple_sync_service,
    local_file.redcap_sync_service,
    local_file.redcap_to_curious_service,
    local_file.curious_alerts_websocket_service,
    local_file.curious_accounts_to_redcap_service,
    local_file.curious_data_to_redcap_service,
    local_file.hbn_sync_service,
    local_file.hbn_sync_timer,
  ]
}

# Set ownership and permissions for workspace project root
resource "null_resource" "set_project_ownership" {
  triggers = {
    user_group    = var.user_group
    workspace_dir = local.workspace_dir
    workspace     = terraform.workspace
  }
  provisioner "local-exec" {
    command = <<-EOT
      # Extract user (handle both "user" and "user:group" formats)
      USER="${var.user_group}"
      if [[ "$USER" == *":"* ]]; then
        USER="$${USER%:*}"
      fi
      # Set ownership of workspace project root
      sudo chown -R ${var.user_group}:${var.user_group} ${local.workspace_dir}
      # Set directory permissions (rwxr-x---)
      sudo find ${local.workspace_dir} -type d -exec chmod 750 {} \;
      # Set file permissions (rw-r-----)
      sudo find ${local.workspace_dir} -type f -exec chmod 640 {} \;
      # Make venv binaries executable
      if [ -d "${local.venv_full_path}/bin" ]; then
        sudo find ${local.venv_full_path}/bin -type f -exec chmod 750 {} \;
      fi
      # Make any shell scripts executable
      sudo find ${local.workspace_dir} -type f -name "*.sh" -exec chmod 750 {} \;
      echo "✓ Permissions set for workspace: ${terraform.workspace}"
    EOT
  }
  depends_on = [null_resource.deploy_services]
}

# Secure config files (stricter permissions)
resource "null_resource" "secure_config" {
  triggers = {
    user_group    = var.user_group
    workspace_dir = local.workspace_dir
    workspace     = terraform.workspace
  }
  provisioner "local-exec" {
    command = <<-EOT
      CONFIG_DIR="${local.workspace_dir}/python_jobs/src/hbnmigration/_config_variables"
      if [ -d "$CONFIG_DIR" ]; then
        # Secure config directory
        sudo chown -R ${var.user_group}:${var.user_group} "$CONFIG_DIR"
        sudo chmod -R 600 "$CONFIG_DIR"
        # Make the directory itself executable/searchable
        sudo find "$CONFIG_DIR" -type d -exec chmod 700 {} \;
        echo "✓ Config secured for workspace: ${terraform.workspace}"
      else
        echo "⚠ Config directory not found (may not be deployed yet): $CONFIG_DIR"
      fi
    EOT
  }
  depends_on = [null_resource.set_project_ownership]
}

# Set up systemd journal access and log directory
resource "null_resource" "setup_logging" {
  triggers = {
    user_group    = var.user_group
    log_directory = local.log_full_path
    workspace     = terraform.workspace
  }
  provisioner "local-exec" {
    command = <<-EOT
      # Extract user (handle both "user" and "user:group" formats)
      USER="${var.user_group}"
      if [[ "$USER" == *":"* ]]; then
        USER="$${USER%:*}"
      fi
      # Add user to systemd-journal group for log access
      sudo usermod -a -G systemd-journal "$USER"
      # Create custom log directory
      sudo mkdir -p ${local.log_full_path}
      sudo chown ${var.user_group}:${var.user_group} ${local.log_full_path}
      sudo chmod 755 ${local.log_full_path}
      # Set up log rotation (workspace-specific)
      sudo tee /etc/logrotate.d/hbnmigration${local.workspace_suffix} > /dev/null <<'LOGROTATE'
${local.log_full_path}/*.log {
    daily
    missingok
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 ${var.user_group} ${var.user_group}
    sharedscripts
}
LOGROTATE
      echo "✓ Logging configured for workspace: ${terraform.workspace}"
    EOT
  }
  depends_on = [null_resource.secure_config]
}

# Set up automated state backups via cron (workspace-aware)
resource "null_resource" "setup_state_backup_cron" {
  triggers = {
    project_root  = local.workspace_dir
    log_directory = local.log_full_path
    user_group    = var.user_group
    workspace     = terraform.workspace
  }
  provisioner "local-exec" {
    command = <<-EOT
      # Create the backup script (workspace-aware)
      sudo tee /usr/local/bin/terraform-state-backup${local.workspace_suffix} > /dev/null <<'BACKUP_SCRIPT'
#!/bin/bash
# Automated Terraform state backup for workspace: ${terraform.workspace}
STATE_FILE="/opt/hbnmigration/terraform/terraform.tfstate.d/${terraform.workspace}/terraform.tfstate"
# Use default state if in default workspace
if [ "${terraform.workspace}" = "default" ]; then
  STATE_FILE="/opt/hbnmigration/terraform/terraform.tfstate"
fi
BACKUP_DIR="${local.log_full_path}/terraform-backups"
DAILY_BACKUP_DIR="$${BACKUP_DIR}/daily"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DATE=$(date +%Y%m%d)
# Create backup directories
mkdir -p "$${DAILY_BACKUP_DIR}"
if [ -f "$${STATE_FILE}" ]; then
    # Timestamped backup (for immediate reference)
    cp "$${STATE_FILE}" "$${BACKUP_DIR}/terraform.tfstate.$${TIMESTAMP}"
    # Daily backup (one per day)
    cp "$${STATE_FILE}" "$${DAILY_BACKUP_DIR}/terraform.tfstate.$${DATE}"
    # Keep only last 30 timestamped backups
    cd "$${BACKUP_DIR}"
    ls -t terraform.tfstate.* 2>/dev/null | tail -n +31 | xargs -r rm
    # Keep only last 7 daily backups
    cd "$${DAILY_BACKUP_DIR}"
    ls -t terraform.tfstate.* 2>/dev/null | tail -n +8 | xargs -r rm
    # Set ownership
    chown -R ${var.user_group}:${var.user_group} "$${BACKUP_DIR}"
    chmod -R 640 "$${BACKUP_DIR}"/*.tfstate.* 2>/dev/null || true
    # Log success
    echo "$(date): [${terraform.workspace}] Terraform state backed up successfully" >> "$${BACKUP_DIR}/backup.log"
else
    echo "$(date): [${terraform.workspace}] ERROR - State file not found: $${STATE_FILE}" >> "$${BACKUP_DIR}/backup.log"
fi
BACKUP_SCRIPT
      # Make script executable
      sudo chmod +x /usr/local/bin/terraform-state-backup${local.workspace_suffix}
      # Create cron job (workspace-specific)
      sudo tee /etc/cron.daily/terraform-state-backup${local.workspace_suffix} > /dev/null <<'CRON_SCRIPT'
#!/bin/bash
/usr/local/bin/terraform-state-backup${local.workspace_suffix}
CRON_SCRIPT
      sudo chmod +x /etc/cron.daily/terraform-state-backup${local.workspace_suffix}
      # Run initial backup
      sudo /usr/local/bin/terraform-state-backup${local.workspace_suffix}
      echo "✓ State backup cron job installed for workspace: ${terraform.workspace}"
      echo "  - Script: /usr/local/bin/terraform-state-backup${local.workspace_suffix}"
      echo "  - Cron: /etc/cron.daily/terraform-state-backup${local.workspace_suffix}"
      echo "  - Backups: ${local.log_full_path}/terraform-backups/"
      echo "  - Retention: 30 timestamped backups, 7 daily backups"
    EOT
  }
  depends_on = [null_resource.setup_logging]
}

# Enable and start services (workspace-specific)
resource "null_resource" "enable_services" {
  triggers = {
    services_hash = null_resource.deploy_services.triggers.services_hash
    workspace     = terraform.workspace
  }
  provisioner "local-exec" {
    command = <<-EOT
      echo "Enabling services for workspace: ${terraform.workspace}"
      sudo systemctl enable ${local.services.hbn_sync_timer}
      sudo systemctl enable ${local.services.curious_alerts_websocket}
      sudo systemctl restart ${local.services.hbn_sync_timer} 2>/dev/null || sudo systemctl start ${local.services.hbn_sync_timer}
      sudo systemctl restart ${local.services.curious_alerts_websocket} 2>/dev/null || sudo systemctl start ${local.services.curious_alerts_websocket}
      echo "✓ Services started for workspace: ${terraform.workspace}"
      echo ""
      echo "Service names:"
      echo "  - ${local.services.ripple_sync}.service"
      echo "  - ${local.services.redcap_sync}.service"
      echo "  - ${local.services.redcap_to_curious}.service"
      echo "  - ${local.services.curious_alerts_websocket}.service"
      echo "  - ${local.services.hbn_sync_timer}"
    EOT
  }
  depends_on = [null_resource.setup_state_backup_cron]
}
