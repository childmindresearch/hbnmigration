#!/bin/bash
# Install testing tools

set -e

echo "Installing Terraform testing tools..."

# Detect OS
OS=$(uname -s)

# Install tflint
if ! command -v tflint &> /dev/null; then
    echo "Installing tflint..."
    if [ "$OS" = "Darwin" ]; then
        brew install tflint
    else
        curl -s https://raw.githubusercontent.com/terraform-linters/tflint/master/install_linux.sh | bash
    fi
else
    echo "tflint already installed"
fi

# Install checkov
if ! command -v checkov &> /dev/null; then
    echo "Installing checkov..."
    pip install checkov
else
    echo "checkov already installed"
fi

# Install terraform-docs (optional)
if ! command -v terraform-docs &> /dev/null; then
    echo "Installing terraform-docs..."
    if [ "$OS" = "Darwin" ]; then
        brew install terraform-docs
    else
        echo "Skipping terraform-docs (requires Go)"
    fi
else
    echo "terraform-docs already installed"
fi

echo ""
echo "All tools installed!"
echo ""
echo "Run tests with: ./tests/test-terraform.sh"
echo "Or use make: make test"
