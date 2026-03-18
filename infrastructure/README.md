# HBN Migration Infrastructure

Terraform configuration for managing HBN Migration systemd services on the VM.

## Services Managed

- **ripple-sync**: Syncs data from Ripple to REDCap
- **redcap-sync**: Syncs REDCap data
- **redcap-to-curious**: Syncs data from REDCap to Curious
- **curious-alerts-websocket**: Always-on WebSocket service for Curious alerts (NEW)
- **hbn-sync.timer**: Runs the above sync services on schedule

## Quick Start

### 1. Initial Setup

```bash
# Install Terraform and dependencies
./setup_terraform.sh

# Copy and configure variables
cp terraform.tfvars.example terraform.tfvars
vim terraform.tfvars  # Edit with your values

# Initialize Terraform
terraform init
```

### 2. Deploy services

```bash
./safe-apply.sh
```

### 3. Verify deployment

```bash
# Check services
sudo systemctl status curious-alerts-websocket.service
sudo systemctl status hbn-sync.timer
sudo systemctl list-timers

# View logs
sudo journalctl -u curious-alerts-websocket -f
tail -f /var/log/hbnmigration/*.log
```

## Configuration

Edit `terraform.tfvars`.

## State management

### Backups

State backups are automatic:

- Location: `/var/log/hbnmigration/terraform-backups/`
- Retention: 30 timestamped backups, 7 daily backups
- Schedule: Daily via cron
- Manual backup:

```bash
./backup-state.sh
```

#### Restore from backup

```bash
# List backups
ls -lh /var/log/hbnmigration/terraform-backups/

# Restore
cp /var/log/hbnmigration/terraform-backups/terraform.tfstate.YYYYMMDD-HHMMSS \
   terraform.tfstate

# Verify
terraform state list
```

## Maintenance

### Update service configuration

1. Edit service templates in [`./services/*.tpl`](./services).
2. Run [`./safe-apply.sh`](./safe-apply.sh).
3. Services are automatically restarted.

### Change sync interval

1. Edit `sync_interval_minutes` in [`./terraform.tfvars`](./terraform.tfvars).
2. Run [`./safe-apply.sh`](./safe-apply.sh).
3. Timer is automatically updated.

### View service logs

```bash
# WebSocket service
sudo journalctl -u curious-alerts-websocket -f

# Sync services
sudo journalctl -u ripple-sync -u redcap-sync -u redcap-to-curious -f

# All logs
tail -f /var/log/hbnmigration/*.log
```
