#!/bin/bash
set -e

STATE_DIR="/opt/hbnmigration/terraform"
BACKUP_DIR="/opt/hbnmigration/terraform/backups"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

echo "Backing up Terraform state..."

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Backup state file
if [ -f "$STATE_DIR/terraform.tfstate" ]; then
    cp "$STATE_DIR/terraform.tfstate" "$BACKUP_DIR/terraform.tfstate.$TIMESTAMP"
    echo "✓ State backed up to: $BACKUP_DIR/terraform.tfstate.$TIMESTAMP"

    # Keep only last 30 backups
    cd "$BACKUP_DIR"
    find . -maxdepth 1 -name "terraform.tfstate.*" -type f -printf '%T@ %p\n' | \
        sort -rn | \
        tail -n +31 | \
        cut -d' ' -f2- | \
        xargs -r rm
    echo "✓ Cleaned up old backups (keeping last 30)"
else
    echo "⚠️  No state file found at $STATE_DIR/terraform.tfstate"
fi

# Backup to git if in a git repo
if git -C "$STATE_DIR" rev-parse --git-dir > /dev/null 2>&1; then
    cd "$STATE_DIR"
    git add "backups/terraform.tfstate.$TIMESTAMP" 2>/dev/null || true
    echo "✓ State backup staged for git commit"
fi
