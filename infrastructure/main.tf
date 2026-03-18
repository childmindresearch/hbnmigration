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

  # Use local backend with built-in file locking
  backend "local" {
    path = "/opt/hbnmigration/terraform/terraform.tfstate"
  }
}

# Create user and group if they don't exist
resource "null_resource" "ensure_user_group" {
  triggers = {
    user_group = var.user_group
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
    EOT
    interpreter = ["bash", "-c"]
  }
}

# Generate systemd service files from templates
resource "local_file" "ripple_sync_service" {
  content = templatefile("${path.module}/services/ripple-sync.service.tpl", {
    user_group    = var.user_group
    project_root  = var.project_root
    venv_path     = local.venv_full_path
    log_directory = local.log_full_path
  })
  filename = "${path.module}/.generated/ripple-sync.service"

  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "redcap_sync_service" {
  content = templatefile("${path.module}/services/redcap-sync.service.tpl", {
    user_group    = var.user_group
    project_root  = var.project_root
    venv_path     = local.venv_full_path
    log_directory = local.log_full_path
  })
  filename = "${path.module}/.generated/redcap-sync.service"

  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "redcap_to_curious_service" {
  content = templatefile("${path.module}/services/redcap-to-curious.service.tpl", {
    user_group    = var.user_group
    project_root  = var.project_root
    venv_path     = local.venv_full_path
    log_directory = local.log_full_path
  })
  filename = "${path.module}/.generated/redcap-to-curious.service"

  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "curious_alerts_websocket_service" {
  content = templatefile("${path.module}/services/curious-alerts-websocket.service.tpl", {
    user_group    = var.user_group
    project_root  = var.project_root
    venv_path     = local.venv_full_path
    log_directory = local.log_full_path
  })
  filename = "${path.module}/.generated/curious-alerts-websocket.service"

  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "hbn_sync_timer" {
  content = templatefile("${path.module}/services/hbn-sync.timer.tpl", {
    sync_interval_minutes = var.sync_interval_minutes
  })
  filename = "${path.module}/.generated/hbn-sync.timer"

  depends_on = [null_resource.ensure_user_group]
}

resource "local_file" "hbn_sync_target" {
  content  = file("${path.module}/services/hbn-sync.target")
  filename = "${path.module}/.generated/hbn-sync.target"

  depends_on = [null_resource.ensure_user_group]
}

# Deploy generated service files
resource "null_resource" "deploy_services" {
  triggers = {
    services_hash = sha256(join("", [
      local_file.ripple_sync_service.content,
      local_file.redcap_sync_service.content,
      local_file.redcap_to_curious_service.content,
      local_file.curious_alerts_websocket_service.content,
      local_file.hbn_sync_timer.content,
      local_file.hbn_sync_target.content,
    ]))
  }

  provisioner "local-exec" {
    command = <<-EOT
      sudo cp ${path.module}/.generated/*.service /etc/systemd/system/
      sudo cp ${path.module}/.generated/*.timer /etc/systemd/system/
      sudo cp ${path.module}/.generated/*.target /etc/systemd/system/
      sudo systemctl daemon-reload
    EOT
  }

  depends_on = [
    local_file.ripple_sync_service,
    local_file.redcap_sync_service,
    local_file.redcap_to_curious_service,
    local_file.curious_alerts_websocket_service,
    local_file.hbn_sync_timer,
    local_file.hbn_sync_target,
  ]
}

# Set ownership and permissions for project root
resource "null_resource" "set_project_ownership" {
  triggers = {
    user_group   = var.user_group
    project_root = var.project_root
  }

  provisioner "local-exec" {
    command = <<-EOT
      # Extract user (handle both "user" and "user:group" formats)
      USER="${var.user_group}"
      if [[ "$USER" == *":"* ]]; then
        USER="$${USER%:*}"
      fi

      # Set ownership of project root
      sudo chown -R ${var.user_group}:${var.user_group} ${var.project_root}

      # Set directory permissions (rwxr-x---)
      sudo find ${var.project_root} -type d -exec chmod 750 {} \;

      # Set file permissions (rw-r-----)
      sudo find ${var.project_root} -type f -exec chmod 640 {} \;

      # Make venv binaries executable
      sudo find ${local.venv_full_path}/bin -type f -exec chmod 750 {} \;

      # Make any shell scripts executable
      sudo find ${var.project_root} -type f -name "*.sh" -exec chmod 750 {} \;
    EOT
  }

  depends_on = [null_resource.deploy_services, null_resource.ensure_user_group]
}

# Secure config files (stricter permissions)
resource "null_resource" "secure_config" {
  triggers = {
    user_group   = var.user_group
    project_root = var.project_root
  }

  provisioner "local-exec" {
    command = <<-EOT
      # Secure config directory
      sudo chown -R ${var.user_group}:${var.user_group} ${var.project_root}/python_jobs/src/hbnmigration/_config_variables
      sudo chmod -R 600 ${var.project_root}/python_jobs/src/hbnmigration/_config_variables

      # Make the directory itself executable/searchable
      sudo find ${var.project_root}/python_jobs/src/hbnmigration/_config_variables -type d -exec chmod 700 {} \;
    EOT
  }

  depends_on = [null_resource.set_project_ownership]
}

# Set up systemd journal access and log directory
resource "null_resource" "setup_logging" {
  triggers = {
    user_group    = var.user_group
    log_directory = local.log_full_path
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

      # Set up log rotation
      sudo tee /etc/logrotate.d/hbnmigration > /dev/null <<'LOGROTATE'
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
    EOT
  }

  depends_on = [null_resource.secure_config]
}

# Set up automated state backups via cron
resource "null_resource" "setup_state_backup_cron" {
  triggers = {
    project_root  = var.project_root
    log_directory = local.log_full_path
    user_group    = var.user_group
  }

  provisioner "local-exec" {
    command = <<-EOT
      # Create the backup script
      sudo tee /usr/local/bin/terraform-state-backup > /dev/null <<'BACKUP_SCRIPT'
#!/bin/bash
# Automated Terraform state backup

STATE_FILE="${var.project_root}/infrastructure/terraform.tfstate"
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
    echo "$(date): Terraform state backed up successfully" >> "$${BACKUP_DIR}/backup.log"
else
    echo "$(date): ERROR - State file not found: $${STATE_FILE}" >> "$${BACKUP_DIR}/backup.log"
fi
BACKUP_SCRIPT

      # Make script executable
      sudo chmod +x /usr/local/bin/terraform-state-backup

      # Create cron job
      sudo tee /etc/cron.daily/terraform-state-backup > /dev/null <<'CRON_SCRIPT'
#!/bin/bash
/usr/local/bin/terraform-state-backup
CRON_SCRIPT

      sudo chmod +x /etc/cron.daily/terraform-state-backup

      # Run initial backup
      sudo /usr/local/bin/terraform-state-backup

      echo "✓ State backup cron job installed"
      echo "  - Script: /usr/local/bin/terraform-state-backup"
      echo "  - Cron: /etc/cron.daily/terraform-state-backup"
      echo "  - Backups: ${local.log_full_path}/terraform-backups/"
      echo "  - Retention: 30 timestamped backups, 7 daily backups"
    EOT
  }

  depends_on = [null_resource.setup_logging]
}

# Enable and start services
resource "null_resource" "enable_services" {
  triggers = {
    services_hash = null_resource.deploy_services.triggers.services_hash
  }

  provisioner "local-exec" {
    command = <<-EOT
      sudo systemctl enable hbn-sync.timer
      sudo systemctl enable curious-alerts-websocket.service
      sudo systemctl restart hbn-sync.timer 2>/dev/null || sudo systemctl start hbn-sync.timer
      sudo systemctl restart curious-alerts-websocket.service 2>/dev/null || sudo systemctl start curious-alerts-websocket.service
    EOT
  }

  depends_on = [null_resource.setup_state_backup_cron]
}
