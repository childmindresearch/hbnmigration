# Terraform Tests

This directory contains testing tools and configurations for the Terraform infrastructure.

## Test Suite

The test suite validates:

- Terraform syntax and formatting
- Configuration validation
- Security best practices
- Linting rules
- Required files existence
- Common configuration issues

## Quick Start

Install testing tools:

```bash
chmod +x tests/install-tools.sh
./tests/install-tools.sh
```

Run all tests:

```bash
./tests/test-terraform.sh
```

Or use make from the infrastructure directory:

```bash
make test
```

## Test Files

- `test-terraform.sh` - Main test runner script
- `.tflint.hcl` - TFLint configuration
- `.checkov.yaml` - Checkov security scanner configuration
- `install-tools.sh` - Script to install testing dependencies

## Testing Tools

### tflint

Terraform linter that checks for:

- Deprecated syntax
- Naming conventions
- Best practices
- AWS-specific issues

### Checkov

Security scanner that checks for:

- Security misconfigurations
- Compliance violations
- Best practices

### Built-in Terraform Commands

- `terraform fmt` - Format checking
- `terraform validate` - Configuration validation

## Running Individual Tests

Format check only:

```bash
terraform fmt -check -recursive
```

Validation only:

```bash
terraform init -backend=false
terraform validate
```

Security scan only:

```bash
checkov -d . --config-file tests/.checkov.yaml
```

Linting only:

```bash
tflint --config tests/.tflint.hcl
```

## CI/CD Integration

The test suite is designed to run in CI/CD pipelines. See `.github/workflows/terraform-test.yaml` for GitHub Actions integration.

## Adding New Tests

To add a new test to the suite:

1. Open `test-terraform.sh`
2. Add a new `run_test` call with your test name and command
3. Follow the existing pattern for consistency

Example:

```bash
run_test "My new test" "my-test-command"
```

## Pre-commit Hook

To run tests before every commit:

```bash
echo '#!/bin/bash' > .git/hooks/pre-commit
echo 'cd infrastructure && ./tests/test-terraform.sh' >> .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```
