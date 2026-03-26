# HBN Migration VM

Infrastructure and code for Healthy Brain Network data migration.

## Overview

AWS infrastructure running on EC2 with:

- Python services for data processing
- Node.js services for export automation
- Apache Iceberg for logging and data lakehouse
- Automated deployment via Terraform

## Prerequisites

### On EC2 Instance

The setup script installs all required tools:

- **Infrastructure:**
  - Terraform >= 1.7.0
  - AWS CLI v2

- **Linting:**
  - ShellCheck (bash linting)
  - ruff (Python linting)
  - mypy (Python type checking)
  - pre-commit (git hook framework)

## Quick Start

- SSH into your EC2 instance
- Clone or copy your project
- One-time setup: Install Terraform

   ```BASH
   ./infrastructure/setup_terraform.sh
   ```

- Configure

   ```BASH
   cp infrastructure/terraform.tfvars.example infrastructure/terraform.tfvars
   ```

- Deploy

   ```BASH
   cd infrastructure
   terraform init
   terraform validate
   terraform plan
   terraform apply
   ```

## Python jobs

See READMEs in [`./python_jobs`](./python_jobs) for details.
