#!/bin/bash
set -e

echo "========================================"
echo "Setting up Terraform on EC2 Instance"
echo "========================================"

# Update system
echo "Updating system packages..."
sudo apt-get update

# Install dependencies
echo "Installing dependencies..."
sudo apt-get install -y wget unzip git jq curl

# Install Terraform
TERRAFORM_VERSION="1.7.0"
echo "Installing Terraform ${TERRAFORM_VERSION}..."

# Check if already installed
if command -v terraform &> /dev/null; then
    CURRENT_VERSION=$(terraform version -json | jq -r '.terraform_version')
    echo "Terraform ${CURRENT_VERSION} is already installed"
    if [ "$CURRENT_VERSION" == "$TERRAFORM_VERSION" ]; then
        echo "✓ Correct version already installed"
    else
        echo "Upgrading to ${TERRAFORM_VERSION}..."
        wget -q https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip
        unzip -q terraform_${TERRAFORM_VERSION}_linux_amd64.zip
        sudo mv terraform /usr/local/bin/
        rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip
        echo "✓ Terraform upgraded to ${TERRAFORM_VERSION}"
    fi
else
    wget -q https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip
    unzip -q terraform_${TERRAFORM_VERSION}_linux_amd64.zip
    sudo mv terraform /usr/local/bin/
    rm terraform_${TERRAFORM_VERSION}_linux_amd64.zip
    echo "✓ Terraform ${TERRAFORM_VERSION} installed"
fi

# Verify installation
terraform version

# Install TFLint
echo ""
echo "Installing TFLint..."
TFLINT_VERSION="0.50.3"

if command -v tflint &> /dev/null; then
    CURRENT_TFLINT_VERSION=$(tflint --version | grep -oP 'version \K[0-9]+\.[0-9]+\.[0-9]+')
    echo "TFLint v${CURRENT_TFLINT_VERSION} is already installed"
    if [ "$CURRENT_TFLINT_VERSION" == "$TFLINT_VERSION" ]; then
        echo "✓ Correct version already installed"
    else
        echo "Upgrading to ${TFLINT_VERSION}..."
        curl -s https://raw.githubusercontent.com/terraform-linters/tflint/master/install_linux.sh | bash
        echo "✓ TFLint upgraded"
    fi
else
    echo "Installing TFLint ${TFLINT_VERSION}..."
    curl -s https://raw.githubusercontent.com/terraform-linters/tflint/master/install_linux.sh | bash
    echo "✓ TFLint installed"
fi

tflint --version

# Initialize TFLint plugins
echo ""
echo "Initializing TFLint plugins..."
if [ -f .tflint.hcl ]; then
    tflint --init
    echo "✓ TFLint plugins initialized"
elif [ -f ../.tflint.hcl ]; then
    cd ..
    tflint --init
    cd infrastructure
    echo "✓ TFLint plugins initialized"
else
    echo "⚠️  No .tflint.hcl found, skipping plugin initialization"
fi

# Install Terragrunt
TERRAGRUNT_VERSION="0.55.1"
echo ""
echo "Installing Terragrunt ${TERRAGRUNT_VERSION}..."

if command -v terragrunt &> /dev/null; then
    CURRENT_TG_VERSION=$(terragrunt --version | grep -oP 'v\K[0-9]+\.[0-9]+\.[0-9]+' | head -1)
    echo "Terragrunt v${CURRENT_TG_VERSION} is already installed"
    if [ "$CURRENT_TG_VERSION" == "$TERRAGRUNT_VERSION" ]; then
        echo "✓ Correct version already installed"
    else
        echo "Upgrading to ${TERRAGRUNT_VERSION}..."
        wget -q https://github.com/gruntwork-io/terragrunt/releases/download/v${TERRAGRUNT_VERSION}/terragrunt_linux_amd64
        chmod +x terragrunt_linux_amd64
        sudo mv terragrunt_linux_amd64 /usr/local/bin/terragrunt
        echo "✓ Terragrunt upgraded to v${TERRAGRUNT_VERSION}"
    fi
else
    wget -q https://github.com/gruntwork-io/terragrunt/releases/download/v${TERRAGRUNT_VERSION}/terragrunt_linux_amd64
    chmod +x terragrunt_linux_amd64
    sudo mv terragrunt_linux_amd64 /usr/local/bin/terragrunt
    echo "✓ Terragrunt v${TERRAGRUNT_VERSION} installed"
fi

# Verify installation
terragrunt --version

# Check if AWS CLI is installed
echo ""
echo "Checking AWS CLI..."
if ! command -v aws &> /dev/null; then
    echo "Installing AWS CLI..."
    curl -s "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
    unzip -q awscliv2.zip
    sudo ./aws/install
    rm -rf aws awscliv2.zip
    echo "✓ AWS CLI installed"
else
    echo "✓ AWS CLI already installed"
    aws --version
fi

# Install linting tools
echo ""
echo "Installing linting tools..."

# ShellCheck for bash linting
if ! command -v shellcheck &> /dev/null; then
    echo "Installing ShellCheck..."
    sudo apt-get install -y shellcheck
    echo "✓ ShellCheck installed"
else
    echo "✓ ShellCheck already installed"
    shellcheck --version | head -2
fi

# Python 3 and pip
if ! command -v python3 &> /dev/null || ! command -v pipx &> /dev/null; then
    echo "Installing Python 3 and pipx..."
    sudo apt-get install -y python3 python3-pip pipx
    echo "✓ Python 3 and pipx installed"
else
    echo "✓ Python 3 already installed"
    python3 --version
fi

# Python linters: ruff and mypy
echo "Installing Python linters..."
pipx install --quiet mypy ruff
pipx ensurepath

# Add pipx bin to PATH if not already there
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    echo "export PATH=\"\$HOME/.local/bin:\$PATH\"" >> ~/.bashrc
    export PATH="$HOME/.local/bin:$PATH"
fi

# Verify Python linters
if command -v ruff &> /dev/null; then
    echo "✓ ruff installed: $(ruff --version)"
else
    echo "⚠️  ruff not found in PATH, you may need to log out and back in"
fi

if command -v mypy &> /dev/null; then
    echo "✓ mypy installed: $(mypy --version)"
else
    echo "⚠️  mypy not found in PATH, you may need to log out and back in"
fi

# Install pre-commit (optional but recommended)
echo ""
echo "Installing pre-commit (optional)..."
if ! command -v pre-commit &> /dev/null; then
    pipx install --quiet pre-commit
    echo "✓ pre-commit installed"
else
    echo "✓ pre-commit already installed"
    pre-commit --version
fi

# Verify IAM role is attached
echo ""
echo "Checking IAM role attachment..."
if aws sts get-caller-identity &> /dev/null; then
    echo "✓ IAM role is properly configured"
    echo ""
    aws sts get-caller-identity
else
    echo ""
    echo "⚠️  WARNING: No IAM role detected!"
    echo ""
    echo "You need to attach an IAM role to this instance with permissions for:"
    echo "  • S3 (read/write for Terraform state)"
    echo "  • Any other AWS services you'll manage"
    echo ""
    echo "Attach an IAM role to this instance:"
    echo "  aws ec2 associate-iam-instance-profile \\"
    echo "    --instance-id <your-instance-id> \\"
    echo "    --iam-instance-profile Name=<profile-name>"
    echo ""
fi

# Create .generated directory for templated files
echo ""
echo "Creating .generated directory..."
mkdir -p .generated
echo "✓ Created .generated directory"

echo ""
echo "========================================"
echo "✓ Setup Complete!"
echo "========================================"
echo ""
echo "Installed tools:"
echo "  • Terraform $(terraform version -json | jq -r '.terraform_version')"
echo "  • Terragrunt $(terragrunt --version 2>&1 | grep -oP 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
echo "  • AWS CLI $(aws --version 2>&1 | cut -d' ' -f1)"
echo "  • ShellCheck $(shellcheck --version | grep version | cut -d' ' -f2)"
echo "  • Python $(python3 --version | cut -d' ' -f2)"
echo "  • ruff $(ruff --version 2>/dev/null || echo 'not in PATH yet')"
echo "  • mypy $(mypy --version 2>/dev/null || echo 'not in PATH yet')"
echo "  • pre-commit $(pre-commit --version 2>/dev/null || echo 'not in PATH yet')"
echo ""
echo "Next steps:"
echo "  1. Configure your settings:"
echo "       cp terraform.tfvars.example terraform.tfvars"
echo "       vim terraform.tfvars"
echo ""
echo "  2. Initialize and apply Terraform:"
echo "       terraform init"
echo "       terraform plan"
echo "       terraform apply"
echo ""
echo "  3. Verify services are running:"
echo "       sudo systemctl status curious-alerts-websocket.service"
echo "       sudo systemctl status hbn-sync.timer"
echo "       sudo systemctl list-timers"
echo ""
echo "  4. Monitor logs:"
echo "       sudo journalctl -u curious-alerts-websocket -f"
echo "       sudo journalctl -u ripple-to-redcap -u redcap-to-redcap -u redcap-to-curious -f"
echo ""
echo "Optional - Set up pre-commit hooks:"
echo "  pre-commit install"
echo "  pre-commit run --all-files"
echo ""
echo "Lint templates manually:"
echo "  make -f lint.Makefile lint-all"
echo ""
echo "⚠️  If ruff/mypy are 'not in PATH', log out and back in."
echo ""
echo "========================================"
