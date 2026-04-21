output "instance_id" {
  description = "EC2 instance ID"
  value       = aws_instance.hbn_migration.id
}

output "instance_public_ip" {
  description = "Public IP of EC2 instance"
  value       = aws_instance.hbn_migration.public_ip
}

output "instance_private_ip" {
  description = "Private IP of EC2 instance"
  value       = aws_instance.hbn_migration.private_ip
}

output "redcap_to_intake_webhook_url" {
  description = "Webhook URL for REDCap to Intake (configure in REDCap Data Entry Trigger)"
  value       = "http://${aws_instance.hbn_migration.public_ip}:8001/webhook/redcap-to-intake"
}

output "redcap_to_curious_webhook_url" {
  description = "Webhook URL for REDCap to Curious (configure in REDCap Data Entry Trigger)"
  value       = "http://${aws_instance.hbn_migration.public_ip}:8002/webhook/redcap-to-curious"
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh -i /path/to/${var.key_name}.pem ubuntu@${aws_instance.hbn_migration.public_ip}"
}

output "security_group_id" {
  description = "Security group ID"
  value       = aws_security_group.hbn_migration.id
}

output "health_check_commands" {
  description = "Commands to check service health"
  value = {
    redcap_to_intake  = "curl http://${aws_instance.hbn_migration.public_ip}:8001/health"
    redcap_to_curious = "curl http://${aws_instance.hbn_migration.public_ip}:8002/health"
  }
}
