#!/bin/bash
set -euo pipefail

# Variables injected by Terraform
# shellcheck disable=SC2154
GITHUB_REPO="${github_repo}"
# shellcheck disable=SC2154
GITHUB_BRANCH="${github_branch}"
# shellcheck disable=SC2154
WORKING_DIR="${working_directory}"
# shellcheck disable=SC2154
PYTHON_VENV="${python_venv}"
# shellcheck disable=SC2154
SERVICE_USER="${service_user}"
# shellcheck disable=SC2154
SERVICE_GROUP="${service_group}"

# Log output
exec > >(tee -a /var/log/hbn-migration-setup.log)
exec 2>&1

echo "Starting HBN Migration setup at $(date)"

# Update system
apt-get update
apt-get upgrade -y

# Install dependencies
apt-get install -y \
  git \
  python3 \
  python3-pip \
  python3-venv \
  curl \
  jq

# Create service user and group
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --create-home --shell /bin/bash "$SERVICE_USER"
  echo "Created user: $SERVICE_USER"
fi

# Create working directory
mkdir -p "$WORKING_DIR"
chown "$SERVICE_USER:$SERVICE_GROUP" "$WORKING_DIR"

# Clone repository
if [ ! -d "$WORKING_DIR/.git" ]; then
  sudo -u "$SERVICE_USER" git clone --branch "$GITHUB_BRANCH" "$GITHUB_REPO" "$WORKING_DIR"
  echo "Cloned repository"
else
  cd "$WORKING_DIR"
  sudo -u "$SERVICE_USER" git pull origin "$GITHUB_BRANCH"
  echo "Updated repository"
fi

# Create Python virtual environment
if [ ! -d "$PYTHON_VENV" ]; then
  sudo -u "$SERVICE_USER" python3 -m venv "$PYTHON_VENV"
  echo "Created virtual environment"
fi

# Install Python packages
cd "$WORKING_DIR"
sudo -u "$SERVICE_USER" "$PYTHON_VENV/bin/pip" install --upgrade pip
sudo -u "$SERVICE_USER" "$PYTHON_VENV/bin/pip" install -e python_jobs
echo "Installed Python packages"

# Set proper permissions on config files
# These files contain sensitive credentials
CONFIG_DIR="$WORKING_DIR/python_jobs/src/hbnmigration/_config_variables"

if [ -d "$CONFIG_DIR" ]; then
  echo "Setting permissions on config files..."

  # Set directory permissions: rwx for owner, rx for group, no access for others
  find "$CONFIG_DIR" -type d -exec chmod 750 {} \;

  # Set file permissions: rw for owner, r for group, no access for others
  # .py files contain credentials, so we're restrictive
  find "$CONFIG_DIR" -type f -name "*.py" -exec chmod 640 {} \;
  find "$CONFIG_DIR" -type f -name "*.pyi" -exec chmod 640 {} \;

  # Ensure ownership is correct
  chown -R "$SERVICE_USER:$SERVICE_GROUP" "$CONFIG_DIR"

  echo "Config file permissions set:"
  echo "  Directories: 750 (rwxr-x---)"
  echo "  Files: 640 (rw-r-----)"
  echo "  Owner: $SERVICE_USER:$SERVICE_GROUP"

  # Verify
  ls -la "$CONFIG_DIR"
  ls -la "$CONFIG_DIR/redcap_variables/" || true
  ls -la "$CONFIG_DIR/curious_variables/" || true
else
  echo "WARNING: Config directory not found at $CONFIG_DIR"
fi

# Create log directory
LOG_DIR="/var/log/hbnmigration"
mkdir -p "$LOG_DIR"
chown "$SERVICE_USER:$SERVICE_GROUP" "$LOG_DIR"
chmod 755 "$LOG_DIR"

# Create cache directory
CACHE_DIR="/tmp/hbn_cache"
mkdir -p "$CACHE_DIR"
chown "$SERVICE_USER:$SERVICE_GROUP" "$CACHE_DIR"
chmod 750 "$CACHE_DIR"

echo "Setup completed at $(date)"
echo "Next steps:"
echo "1. Copy service files to /etc/systemd/system/"
echo "2. Enable and start services"
echo "3. Configure REDCap Data Entry Triggers"
