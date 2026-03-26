# main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    external = {
      source  = "hashicorp/external"
      version = "~> 2.3"
    }
  }
}

data "external" "current_user" {
  program = ["bash", "-c", <<-EOF
    echo '{"user":"'$${SUDO_USER:-$(whoami)}'"}'
  EOF
  ]
}

locals {
  current_user = data.external.current_user.result.user
  instance_id  = var.instance_id
}

# Generate setup script from template
locals {
  setup_script_content = templatefile("${path.module}/templates/setup_services.sh.tpl", {
    AWS_REGION    = var.aws_region
    ENVIRONMENT   = var.environment
    WEBSOCKET_URL = var.websocket_url
    VENV_PATH     = var.venv_path
    INSTANCE_NAME = var.instance_name
    USER          = local.current_user
    USER_GROUP    = var.user_group
  })
}

# Write generated script to disk
resource "local_file" "setup_script" {
  content         = local.setup_script_content
  filename        = "${path.module}/generated_setup.sh"
  file_permission = "0755"
}

# Execute the generated script
resource "null_resource" "setup_services" {
  triggers = {
    script_hash = sha256(local.setup_script_content)
  }

  provisioner "local-exec" {
    command = "sudo -E bash ${local_file.setup_script.filename}"
  }

  depends_on = [
    local_file.setup_script
  ]
}

# ============================================================================
# Sync Services (Ripple + REDCap)
# ============================================================================

# Generate ripple-sync service file
resource "local_file" "ripple_sync_service" {
  content = templatefile("${path.module}/templates/ripple-sync.service.tpl", {
    USER        = local.current_user
    USER_GROUP  = var.user_group
    VENV_PATH   = var.venv_path
    ENVIRONMENT = var.environment
  })
  filename        = "${path.module}/generated_ripple-sync.service"
  file_permission = "0644"
}

# Generate redcap-sync service file
resource "local_file" "redcap_sync_service" {
  content = templatefile("${path.module}/templates/redcap-sync.service.tpl", {
    USER        = local.current_user
    USER_GROUP  = var.user_group
    VENV_PATH   = var.venv_path
    ENVIRONMENT = var.environment
  })
  filename        = "${path.module}/generated_redcap-sync.service"
  file_permission = "0644"
}

# Generate redcap-to-curious service file
resource "local_file" "redcap_to_curious_service" {
  content = templatefile("${path.module}/templates/redcap-to-curious.service.tpl", {
    USER        = local.current_user
    USER_GROUP  = var.user_group
    VENV_PATH   = var.venv_path
    ENVIRONMENT = var.environment
  })
  filename        = "${path.module}/generated_redcap-to-curious.service"
  file_permission = "0644"
}

# Generate combined systemd timer file
resource "local_file" "sync_timer" {
  filename = "./generated_sync-services.timer"
  content  = <<-EOT
    [Unit]
    Description=Ripple and REDCap Sync Timer

    [Timer]
    # Run 2 minutes after boot
    OnBootSec=2min
    # Run every $ripple_sync_interval_minutes after the last activation
    OnUnitActiveSec=${var.ripple_sync_interval_minutes}min
    # Keep timer accurate to within 1 second
    AccuracySec=1s
    # If the system was off when timer should have triggered, run it on next boot
    Persistent=true

    [Install]
    WantedBy=timers.target
  EOT

  file_permission      = "0644"
  directory_permission = "0777"
}

resource "local_file" "sync_services_wrapper" {
  filename = "./generated_sync-services.service"
  content  = <<-EOT
    [Unit]
    Description=Run Ripple, REDCap and Curious Sync Services
    After=network.target

    [Service]
    Type=oneshot
    ExecStart=/bin/systemctl start ripple-sync.service
    ExecStart=/bin/systemctl start redcap-sync.service
    ExecStart=/bin/systemctl start redcap-to-curious.service

    [Install]
    WantedBy=multi-user.target
  EOT

  file_permission      = "0644"
  directory_permission = "0777"
}

# Install and enable systemd services and timer
resource "null_resource" "install_sync_services" {
  triggers = {
    ripple_service_hash            = sha256(local_file.ripple_sync_service.content)
    redcap_service_hash            = sha256(local_file.redcap_sync_service.content)
    redcap_to_curious_service_hash = sha256(local_file.redcap_to_curious_service.content)
    wrapper_service_hash           = sha256(local_file.sync_services_wrapper.content)
    timer_hash                     = sha256(local_file.sync_timer.content)
  }

  provisioner "local-exec" {
    command = <<-EOT
      set -e

      # Copy service files
      sudo cp ./generated_ripple-sync.service /etc/systemd/system/ripple-sync.service
      sudo cp ./generated_redcap-sync.service /etc/systemd/system/redcap-sync.service
      sudo cp ./generated_redcap-to-curious.service /etc/systemd/system/redcap-to-curious.service
      sudo cp ./generated_sync-services.service /etc/systemd/system/sync-services.service
      sudo cp ./generated_sync-services.timer /etc/systemd/system/sync-services.timer

      # Create log directories
      sudo mkdir -p /var/log/ripple-sync
      sudo chown ${local.current_user}:hbnmigration /var/log/ripple-sync

      sudo mkdir -p /var/log/redcap-sync
      sudo chown ${local.current_user}:hbnmigration /var/log/redcap-sync

      sudo mkdir -p /var/log/redcap-to-curious
      sudo chown ${local.current_user}:hbnmigration /var/log/redcap-to-curious

      # Reload systemd
      sudo systemctl daemon-reload

      # Enable and start timer (not the wrapper service directly)
      sudo systemctl enable sync-services.timer
      sudo systemctl restart sync-services.timer

      # Show status
      echo "Timer status:"
      sudo systemctl status sync-services.timer --no-pager || true
      echo ""
      echo "Next run times:"
      sudo systemctl list-timers sync-services.timer --no-pager || true
    EOT
  }

  depends_on = [
    local_file.ripple_sync_service,
    local_file.redcap_sync_service,
    local_file.redcap_to_curious_service,
    local_file.sync_services_wrapper,
    local_file.sync_timer,
    null_resource.setup_services
  ]
}

# Output timer status
output "sync_services_status" {
  value = "Run 'systemctl status sync-services.timer' to check status"
}
