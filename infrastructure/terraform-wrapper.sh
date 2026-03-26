#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCK_FILE="$SCRIPT_DIR/.terraform.lock"
LOCK_FD=200

# Function to acquire lock
acquire_lock() {
    eval "exec $LOCK_FD>$LOCK_FILE"
    if ! flock -n $LOCK_FD; then
        echo "ERROR: Another terraform process is running!"
        echo "Lock file: $LOCK_FILE"
        echo ""
        echo "If you're sure no other process is running, remove the lock:"
        echo "  rm $LOCK_FILE"
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
