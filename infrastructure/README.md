# HBN Migration Infrastructure

Terraform configuration for managing HBN Migration systemd services on the VM.

## Services Managed

- **ripple-to-redcap**: Syncs data from Ripple to REDCap
- **redcap-to-redcap**: Syncs REDCap data
- **redcap-to-curious**: Syncs data from REDCap to Curious
- **curious-alerts-websocket**: Always-on WebSocket service for Curious alerts
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

### 2. Deploy Services

```bash
./safe-apply.sh
```

### 3. Verify Deployment

```bash
# Check services
sudo systemctl status curious-alerts-websocket.service
sudo systemctl status hbn-sync.timer
sudo systemctl list-timers

# View logs
sudo journalctl -u curious-alerts-websocket -f
tail -f /var/log/hbnmigration/*.log
```

## Workspaces

This infrastructure supports Terraform workspaces for isolated testing.

### Available Workspaces

- `default` - Production environment
- `test` - Testing environment (or create your own)

### Workspace Commands

```bash
# Create and switch to test workspace
terraform workspace new test

# List workspaces
terraform workspace list

# Switch between workspaces
terraform workspace select test
terraform workspace select default

# Apply to current workspace
terraform apply
```

### Workspace Isolation

Each workspace maintains:

- Separate state files (`terraform.tfstate.d/<workspace>/`)
- Isolated service names (e.g., `test-ripple-to-redcap.service`)
- Separate project directories (e.g., `/opt/hbnmigration-test`)
- Isolated log directories (e.g., `/var/log/hbnmigration-test`)
- Independent cron jobs for state backups

### Testing Workflow

```bash
# 1. Create test workspace
terraform workspace new test

# 2. Deploy test services
terraform apply

# 3. Verify test services
sudo systemctl status test-ripple-to-redcap.service
sudo journalctl -u test-curious-alerts-websocket -f

# 4. When satisfied, deploy to production
terraform workspace select default
terraform apply
```

## Configuration

Edit `terraform.tfvars` to customize:

- `project_root` - Base directory for the application
- `user_group` - System user/group for running services
- `sync_interval_minutes` - How often sync services run (1-1440)
- `venv_path` - Virtual environment location (relative or absolute)
- `log_directory` - Where logs are stored

## State Management

### Automatic Backups

State backups run automatically:

- **Location**: `/var/log/hbnmigration/terraform-backups/` (or `/var/log/hbnmigration-<workspace>/terraform-backups/`)
- **Retention**: 30 timestamped backups, 7 daily backups
- **Schedule**: Daily via cron
- **Manual backup**:

```bash
./backup-state.sh
```

### Restore from Backup

```bash
# List backups
ls -lh /var/log/hbnmigration/terraform-backups/

# Restore (make sure you're in the correct workspace!)
terraform workspace select default  # or test
cp /var/log/hbnmigration/terraform-backups/terraform.tfstate.YYYYMMDD-HHMMSS \
   terraform.tfstate

# Verify
terraform state list
```

## Service Configuration

All services receive a `WORKSPACE` environment variable indicating which workspace deployed them. Use this in your Python code to load workspace-specific configurations:

```python
import os

workspace = os.environ.get('WORKSPACE', 'default')
config_file = f'config.{workspace}.yaml'
```

## Monitoring Services

### Default Workspace

```bash
# Service status
sudo systemctl status ripple-to-redcap.service
sudo systemctl status redcap-to-redcap.service
sudo systemctl status redcap-to-curious.service
sudo systemctl status curious-alerts-websocket.service

# Live logs
sudo journalctl -u curious-alerts-websocket -f
sudo journalctl -u ripple-to-redcap -u redcap-to-redcap -u redcap-to-curious -f

# File logs
tail -f /var/log/hbnmigration/*.log
```

### Test Workspace

```bash
# Service status
sudo systemctl status test-ripple-to-redcap.service
sudo systemctl status test-curious-alerts-websocket.service

# Live logs
sudo journalctl -u test-curious-alerts-websocket -f
sudo journalctl -u test-ripple-to-redcap -f

# File logs
tail -f /var/log/hbnmigration-test/*.log
```

## Cleaning Up Workspaces

To remove a test workspace:

```bash
# 1. Stop and disable services
sudo systemctl stop test-*.service test-*.timer
sudo systemctl disable test-*.service test-*.timer

# 2. Remove service files
sudo rm /etc/systemd/system/test-*.service
sudo rm /etc/systemd/system/test-*.timer
sudo rm /etc/systemd/system/test-*.target
sudo systemctl daemon-reload

# 3. (Optional) Remove workspace directories and logs
sudo rm -rf /opt/hbnmigration-test
sudo rm -rf /var/log/hbnmigration-test

# 4. Delete workspace
terraform workspace select default
terraform workspace delete test
```

## Maintenance

### Update Service Configuration

1. Edit service templates in `./services/*.tpl`
2. Run `./safe-apply.sh`
3. Services are automatically restarted

### Change Sync Interval

1. Edit `sync_interval_minutes` in `terraform.tfvars`
2. Run `./safe-apply.sh`
3. Timer is automatically updated

### View All Service Logs

```bash
# WebSocket service (always-on)
sudo journalctl -u curious-alerts-websocket -f

# Sync services (triggered by timer)
sudo journalctl -u ripple-to-redcap -u redcap-to-redcap -u redcap-to-curious -f

# All file logs
tail -f /var/log/hbnmigration/*.log

# Check timer schedule
sudo systemctl list-timers hbn-sync.timer
```

## Troubleshooting

### Services won't start

```bash
# Check service status
sudo systemctl status <service-name>

# View full logs
sudo journalctl -u <service-name> -n 100

# Check permissions
ls -la /opt/hbnmigration
ls -la /var/log/hbnmigration
```

### Wrong workspace deployed

```bash
# Check current workspace
terraform workspace show

# View deployed services
systemctl list-units 'test-*.service' --all
systemctl list-units '*ripple*.service' --all
```
