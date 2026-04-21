#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Testing Terraform Configuration"
echo "=========================================="
echo ""

# Track if any tests failed
FAILED=0

# Function to run a test
run_test() {
    local test_name="$1"
    local test_command="$2"

    echo -e "${YELLOW}Running: ${test_name}${NC}"

    if eval "$test_command"; then
        echo -e "${GREEN}✓ PASSED: ${test_name}${NC}"
        echo ""
        return 0
    else
        echo -e "${RED}✗ FAILED: ${test_name}${NC}"
        echo ""
        FAILED=1
        return 1
    fi
}

# Change to infrastructure directory
cd "$(dirname "$0")/.."

# Test 1: Format check
run_test "Terraform format check" "terraform fmt -check -recursive"

# Test 2: Initialize (without backend)
run_test "Terraform init" "terraform init -backend=false"

# Test 3: Validation
run_test "Terraform validate" "terraform validate"

# Test 4: Security scan with Checkov (if installed)
if command -v checkov &> /dev/null; then
    run_test "Security scan (Checkov)" "checkov -d . --quiet --compact --config-file tests/.checkov.yaml"
else
    echo -e "${YELLOW}⊘ SKIPPED: Security scan (Checkov not installed)${NC}"
    echo "  Install with: pip install checkov"
    echo ""
fi

# Test 5: Lint with tflint (if installed)
if command -v tflint &> /dev/null; then
    run_test "Terraform lint (tflint)" "tflint --config tests/.tflint.hcl --init && tflint --config tests/.tflint.hcl"
else
    echo -e "${YELLOW}⊘ SKIPPED: Terraform lint (tflint not installed)${NC}"
    echo "  Install with: curl -s https://raw.githubusercontent.com/terraform-linters/tflint/master/install_linux.sh | bash"
    echo ""
fi

# Test 6: Check required files exist
run_test "Required files exist" "test -f main.tf && test -f variables.tf && test -f outputs.tf"

# Test 7: Check service templates exist
run_test "Service templates exist" "test -f services/redcap-to-redcap.service.tpl && test -f services/redcap-to-curious.service.tpl"

# Test 8: Check user_data script exists
run_test "User data script exists" "test -f user_data.sh"

# Test 9: Check for common issues
echo -e "${YELLOW}Running: Common issues check${NC}"
issues_found=0

# Check for hardcoded secrets
if grep -r "password\s*=\s*\"" . --include="*.tf" --exclude-dir=".terraform" 2>/dev/null; then
    echo "Found hardcoded passwords in .tf files"
    issues_found=1
fi

# Check for example values in tfvars (if it exists and is not the example)
if [ -f terraform.tfvars ] && grep -E "(xxxxx|CHANGEME|TODO)" terraform.tfvars 2>/dev/null; then
    echo "Found placeholder values in terraform.tfvars"
    issues_found=1
fi

if [ "$issues_found" -eq 0 ]; then
    echo -e "${GREEN}✓ PASSED: Common issues check${NC}"
    echo ""
else
    echo -e "${RED}✗ FAILED: Common issues check${NC}"
    echo ""
    FAILED=1
fi

# Summary
echo "=========================================="
if [ "$FAILED" -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    echo "=========================================="
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    echo "=========================================="
    exit 1
fi
