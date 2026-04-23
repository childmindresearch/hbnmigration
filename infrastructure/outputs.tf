output "redcap_to_intake_webhook_path" {
  description = "Webhook path for REDCap to Intake (configure in REDCap Data Entry Trigger)"
  value       = "/webhook/redcap-to-intake (port 8001)"
}

output "redcap_to_curious_webhook_path" {
  description = "Webhook path for REDCap to Curious (configure in REDCap Data Entry Trigger)"
  value       = "/webhook/redcap-to-curious (port 8002)"
}

output "service_commands" {
  description = "Commands to manage services (run on the VM)"
  value = {
    install_webhooks = "sudo cp generated/redcap-to-redcap.service generated/redcap-to-curious.service /etc/systemd/system/ && sudo systemctl daemon-reload"
    install_timer    = "sudo cp generated/hbn-sync.service generated/hbn-sync.timer /etc/systemd/system/ && sudo systemctl daemon-reload"
    enable_webhooks  = "sudo systemctl enable --now redcap-to-redcap.service redcap-to-curious.service"
    enable_timer     = "sudo systemctl enable --now hbn-sync.timer"
    disable_timer    = "sudo systemctl disable --now hbn-sync.timer"
    status           = "sudo systemctl status redcap-to-redcap.service redcap-to-curious.service hbn-sync.timer"
    logs             = "sudo journalctl -u redcap-to-redcap.service -u redcap-to-curious.service -f"
  }
}

output "health_check_commands" {
  description = "Commands to check service health (run on the VM)"
  value = {
    redcap_to_intake  = "curl http://localhost:8001/health"
    redcap_to_curious = "curl http://localhost:8002/health"
  }
}

output "migration_note" {
  description = "Steps to migrate from timer to webhooks"
  value       = <<-EOT
    Once security group rules are in place for ports 8001/8002:
    1. Enable webhooks: sudo systemctl enable --now redcap-to-redcap.service redcap-to-curious.service
    2. Configure REDCap Data Entry Triggers
    3. Test webhooks: curl http://localhost:8001/health
    4. Disable timer: sudo systemctl disable --now hbn-sync.timer
  EOT
}

output "security_group_note" {
  description = "Manual step required for webhooks"
  value       = "Ask AWS admin to add inbound rules for ports 8001 and 8002 (TCP) from your REDCap server IP(s)"
}
