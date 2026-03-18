#!/bin/bash
set -e

# Source terraform.tfvars to get log_directory
if [ -f terraform.tfvars ]; then
    LOG_DIR=$(grep '^log_directory' terraform.tfvars | cut -d'"' -f2 | tr -d ' ')
else
    LOG_DIR="/var/log/hbnmigration"
fi

# Resolve relative paths
if [[ "$LOG_DIR" != /* ]]; then
    PROJECT_ROOT=$(grep '^project_root' terraform.tfvars | cut -d'"' -f2 | tr -d ' ' || echo "/opt/hbnmigration")
    LOG_DIR="${PROJECT_ROOT}/${LOG_DIR}"
fi

LOCK_FILE="${LOG_DIR}/terraform.lock"
LOCK_FD=200

# Ensure log directory exists
sudo mkdir -p "$LOG_DIR"

# Function to acquire lock
acquire_lock() {
    eval "exec $LOCK_FD>$LOCK_FILE"
    if ! flock -n $LOCK_FD; then
        echo "ERROR: Another terraform process is running!"
        echo "Lock file: $LOCK_FILE"
        echo ""
        echo "If you're sure no other process is running, remove the lock:"
        echo "  sudo rm $LOCK_FILE"
        exit 1
    fi
    echo "✓ Lock acquired: $LOCK_FILE"
}

# Function to release lock
release_lock() {
    flock -u $LOCK_FD
    rm -f "$LOCK_FILE"
    echo "✓ Lock released"
}

# Ensure lock is released on exit
trap release_lock EXIT

# Acquire lock
acquire_lock

# Run terraform command
echo "Running: terraform $*"
terraform "$@"
