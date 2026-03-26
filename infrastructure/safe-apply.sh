#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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

echo "========================================"
echo "Terraform Safe Apply"
echo "========================================"
echo ""

# Check for existing lock
if [ -f "$LOCK_FILE" ]; then
    echo "⚠️  Lock file exists: $LOCK_FILE"
    echo "Another terraform process may be running."
    echo ""
    read -r -p "Remove lock and continue? (type 'yes'): " confirm
    if [ "$confirm" == "yes" ]; then
        sudo rm -f "$LOCK_FILE"
        echo "✓ Lock removed"
    else
        echo "Aborted."
        exit 1
    fi
    echo ""
fi

# Backup state before any operation
if [ -f terraform.tfstate ]; then
    ./backup-state.sh
    echo ""
fi

# Run plan
echo "Running terraform plan..."
./terraform-wrapper.sh plan -out=tfplan

echo ""
echo "========================================"
echo "Review the plan above carefully!"
echo "========================================"
echo ""
echo "Pay attention to:"
echo "  • Resources being DESTROYED (-)"
echo "  • Resources being CREATED (+)"
echo "  • Resources being MODIFIED (~)"
echo ""

read -r -p "Do you want to apply these changes? (type 'yes' to continue): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Aborted."
    rm -f tfplan
    exit 1
fi

echo ""
echo "Applying changes..."
./terraform-wrapper.sh apply tfplan

rm -f tfplan
echo ""
echo "✓ Apply complete!"
