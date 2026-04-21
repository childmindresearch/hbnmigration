.PHONY: help test init plan apply destroy fmt validate lint security clean install-tools

help:
    @echo "Available targets:"
    @echo "  test          - Run all tests"
    @echo "  install-tools - Install testing dependencies"
    @echo "  init          - Initialize Terraform"
    @echo "  plan          - Show execution plan"
    @echo "  apply         - Apply infrastructure changes"
    @echo "  destroy       - Destroy infrastructure"
    @echo "  fmt           - Format Terraform files"
    @echo "  validate      - Validate configuration"
    @echo "  lint          - Run tflint"
    @echo "  security      - Run security scan"
    @echo "  clean         - Clean up generated files"

test:
    @./tests/test-terraform.sh

install-tools:
    @chmod +x tests/install-tools.sh
    @./tests/install-tools.sh

init:
    terraform init

plan: init
    terraform plan

apply: test
    ./safe-apply.sh

destroy:
    terraform destroy

fmt:
    terraform fmt -recursive

validate: init
    terraform validate

lint:
    @if command -v tflint >/dev/null 2>&1; then \
        tflint --config tests/.tflint.hcl --init; \
        tflint --config tests/.tflint.hcl; \
    else \
        echo "tflint not installed. Run: make install-tools"; \
        exit 1; \
    fi

security:
    @if command -v checkov >/dev/null 2>&1; then \
        checkov -d . --config-file tests/.checkov.yaml; \
    else \
        echo "checkov not installed. Run: make install-tools"; \
        exit 1; \
    fi

clean:
    rm -rf .terraform
    rm -f .terraform.lock.hcl
    rm -f terraform.tfstate
    rm -f terraform.tfstate.backup
    rm -f plan.tfplan
    rm -rf generated/
