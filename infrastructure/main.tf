terraform {
  required_version = ">= 1.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4"
    }
  }
}

locals {
  workspace = terraform.workspace
  common_vars = {
    workspace      = terraform.workspace
    user_group     = var.user_group
    project_root   = var.project_root
    venv_path      = "${var.project_root}/${var.venv_path}"
    log_directory  = var.log_directory
    project_status = var.project_status
    recovery_mode  = var.recovery_mode
  }

  common_service_config  = templatefile("${path.module}/services/_common_metadata.tpl", local.common_vars)
  oneshot_timeout_config = templatefile("${path.module}/services/_oneshot_timeouts.tpl", local.common_vars)
  async_timeout_config   = templatefile("${path.module}/services/_async_timeouts.tpl", local.common_vars)

  template_vars = merge(local.common_vars, {
    common_config    = local.common_service_config
    oneshot_timeouts = local.oneshot_timeout_config,
    async_timeouts   = local.async_timeout_config
  })
}


# Webhook services (long-running uvicorn servers)
resource "local_file" "redcap_to_redcap_service" {
  content  = templatefile("${path.module}/services/redcap-to-redcap.service.tpl", local.template_vars)
  filename = "${path.module}/generated/redcap-to-redcap.service"
}

resource "local_file" "redcap_to_curious_service" {
  content  = templatefile("${path.module}/services/redcap-to-curious.service.tpl", local.template_vars)
  filename = "${path.module}/generated/redcap-to-curious.service"
}

# Batch services (oneshot, triggered by timer)
resource "local_file" "redcap_to_redcap_batch_service" {
  content  = templatefile("${path.module}/services/redcap-to-redcap-batch.service.tpl", local.template_vars)
  filename = "${path.module}/generated/redcap-to-redcap-batch.service"
}

resource "local_file" "redcap_to_curious_batch_service" {
  content  = templatefile("${path.module}/services/redcap-to-curious-batch.service.tpl", local.template_vars)
  filename = "${path.module}/generated/redcap-to-curious-batch.service"
}

# Other existing services
resource "local_file" "ripple_to_redcap_service" {
  content  = templatefile("${path.module}/services/ripple-to-redcap.service.tpl", local.template_vars)
  filename = "${path.module}/generated/ripple-to-redcap.service"
}

resource "local_file" "curious_alerts_service" {
  content  = templatefile("${path.module}/services/curious-alerts-websocket.service.tpl", local.template_vars)
  filename = "${path.module}/generated/curious-alerts-websocket.service"
}

resource "local_file" "curious_data_service" {
  content  = templatefile("${path.module}/services/curious-data-to-redcap.service.tpl", local.template_vars)
  filename = "${path.module}/generated/curious-data-to-redcap.service"
}

resource "local_file" "curious_accounts_service" {
  content  = templatefile("${path.module}/services/curious-accounts-to-redcap.service.tpl", local.template_vars)
  filename = "${path.module}/generated/curious-accounts-to-redcap.service"
}

# Timer and sync target
resource "local_file" "hbn_sync_service" {
  content = templatefile("${path.module}/services/hbn-sync.service.tpl", {
    workspace = local.workspace
  })
  filename = "${path.module}/generated/hbn-sync.service"
}

resource "local_file" "hbn_sync_timer" {
  content = templatefile("${path.module}/services/hbn-sync.timer.tpl", {
    workspace             = local.workspace
    sync_interval_minutes = var.sync_interval_minutes
  })
  filename = "${path.module}/generated/hbn-sync.timer"
}

resource "local_file" "redcap_track_curious_service" {
  content  = templatefile("${path.module}/services/redcap-track-curious.service.tpl", local.template_vars)
  filename = "${path.module}/generated/redcap-track-curious.service"
}
