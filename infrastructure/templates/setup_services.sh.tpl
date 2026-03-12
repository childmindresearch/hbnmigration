#!/bin/bash
set -e
CURRENT_USER="${USER}"
REPO_ROOT="/home/${USER_GROUP}/hbnmigration"
PYTHON_JOBS_PATH="$REPO_ROOT/python_jobs"
NODE_JOBS_PATH="$REPO_ROOT/node_jobs"
echo "============================================"
echo "Setting up HBN migration monitoring services"
echo "============================================"
echo "Current User: $CURRENT_USER:${USER_GROUP}"
echo "Instance: ${INSTANCE_NAME}"
echo "Region: ${AWS_REGION}"
echo "Environment: ${ENVIRONMENT}"
echo "Virtual Environment path: ${VENV_PATH}"
echo "Jobs paths: $PYTHON_JOBS_PATH, $NODE_JOBS_PATH"
echo "WebSocket URL: ${WEBSOCKET_URL}"
echo "============================================"

# Update system packages
echo "Updating system packages..."
apt-get update -qq

# Install build tools
apt-get install -y bash build-essential git curl wget unzip

# Install Python 3.12
if ! command -v python3.12 &> /dev/null; then
    echo "Installing Python 3.12..."
    apt-get install -y software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update
    apt-get install -y python3.12 python3.12-venv python3.12-dev
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 1
    update-alternatives --set python3 /usr/bin/python3.12
    echo "✓ Python 3.12 installed"
else
    echo "✓ Python 3.12 already installed"
fi

# Install UV
if ! command -v uv &> /dev/null; then
    echo "Installing UV..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    echo "PATH: $PATH"
    echo "✓ UV installed"
else
    echo "✓ UV already installed"
fi

if [ -f "$HOME/.local/bin/env" ]; then
    # shellcheck source=/dev/null
    source "$HOME/.local/bin/env"
fi

if ! command -v uv &> /dev/null; then
    echo "PATH: $PATH"
    echo "❌ Error: UV installation failed"
    exit 1
fi

echo "✓ UV version: $(uv --version)"

# Install NodeJS
if ! command -v node &> /dev/null; then
    echo "Installing NodeJS..."
    curl -fsSL https://deb.nodesource.com/setup_lts.x | bash -
    apt-get install -y nodejs
    echo "✓ NodeJS installed"
else
    echo "✓ NodeJS already installed"
fi

# Create directories
echo "Creating directories..."
mkdir -p /opt/app
mkdir -p /var/log/app
mkdir -p /opt/iceberg/warehouse
mkdir -p /etc/iceberg

# Create virtual environment
echo "Creating Python virtual environment..."
if [ ! -d "${VENV_PATH}" ]; then
    uv venv --python python3.12 "${VENV_PATH}"
    echo "✓ Virtual environment created at ${VENV_PATH}"
else
    echo "✓ Virtual environment already exists"
fi

chown -R "$CURRENT_USER":"${USER_GROUP}" /opt/app /var/log/app /opt/iceberg

# Install Python packages
echo "Installing Python packages..."
if [ -d "$PYTHON_JOBS_PATH" ] && [ -f "$PYTHON_JOBS_PATH/pyproject.toml" ]; then
    echo "Installing from: $PYTHON_JOBS_PATH"
    cd "$PYTHON_JOBS_PATH"
    uv sync --python "${VENV_PATH}/bin/python"
    cd -

    "${VENV_PATH}/bin/python" -c "import hbnmigration; print(f'✓ Installed hbnmigration v{hbnmigration.__version__}')" || {
        echo "❌ Failed to import hbnmigration"
        exit 1
    }
else
    echo "❌ Error: python_jobs not found at $PYTHON_JOBS_PATH"
    exit 1
fi
echo "✓ Python packages installed"

# Install Node.js packages
echo "Installing Node.js packages..."
if [ -d "$NODE_JOBS_PATH" ] && [ -f "$NODE_JOBS_PATH/package.json" ]; then
    echo "Installing from: $NODE_JOBS_PATH"
    cd "$NODE_JOBS_PATH"
    npm install --production --quiet
    echo "✓ Node.js packages installed"
else
    echo "⚠️  node_jobs not found, skipping"
fi

# Create systemd services
echo "Creating service files..."

cat > /etc/systemd/system/websocket-monitor.service <<EOF
[Unit]
Description=WebSocket Monitor Service
After=network.target

[Service]
Type=simple
User=$CURRENT_USER
Group=${USER_GROUP}
WorkingDirectory=$PYTHON_JOBS_PATH
Environment="PATH=${VENV_PATH}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="VIRTUAL_ENV=${VENV_PATH}"
Environment="PYTHONUNBUFFERED=1"
Environment="AWS_REGION=${AWS_REGION}"
Environment="WEBSOCKET_URL=${WEBSOCKET_URL}"
Environment="ENVIRONMENT=${ENVIRONMENT}"
ExecStart=${VENV_PATH}/bin/python -m hbnmigration.websocket_monitor
Restart=always
RestartSec=10
StandardOutput=append:/var/log/app/websocket-monitor.log
StandardError=append:/var/log/app/websocket-monitor.log

[Install]
WantedBy=multi-user.target
EOF

cat > /etc/systemd/system/api-jobs.service <<EOF
[Unit]
Description=API Jobs Service
After=network.target

[Service]
Type=simple
User=$CURRENT_USER
Group=${USER_GROUP}
WorkingDirectory=$NODE_JOBS_PATH
Environment="PATH=/usr/local/bin:/usr/bin:/bin"
Environment="NODE_ENV=production"
Environment="AWS_REGION=${AWS_REGION}"
Environment="WEBSOCKET_URL=${WEBSOCKET_URL}"
Environment="ENVIRONMENT=${ENVIRONMENT}"
ExecStart=/usr/bin/node index.js
Restart=always
RestartSec=10
StandardOutput=append:/var/log/app/api-jobs.log
StandardError=append:/var/log/app/api-jobs.log

[Install]
WantedBy=multi-user.target
EOF

# Create Iceberg configuration
cat > /etc/iceberg/catalog.yaml <<EOF
catalog:
  default:
    type: hadoop
    warehouse: file:///opt/iceberg/warehouse
EOF
chown "$CURRENT_USER":"${USER_GROUP}" /etc/iceberg/catalog.yaml

# Create environment files
cat > /etc/profile.d/app.sh <<EOF
export AWS_REGION=${AWS_REGION}
export AWS_DEFAULT_REGION=${AWS_REGION}
export WEBSOCKET_URL=${WEBSOCKET_URL}
export ENVIRONMENT=${ENVIRONMENT}
export PATH="/root/.local/bin:${VENV_PATH}/bin:\$PATH"
export VIRTUAL_ENV=${VENV_PATH}
export ICEBERG_CATALOG_CONFIG=/etc/iceberg/catalog.yaml
export ICEBERG_WAREHOUSE=file:///opt/iceberg/warehouse
EOF

echo "✓ Configuration files created"

# Reload and start services
echo "Configuring systemd services..."
systemctl daemon-reload
systemctl enable websocket-monitor.service api-jobs.service
systemctl restart websocket-monitor.service
systemctl restart api-jobs.service

sleep 3

echo ""
echo "========================================"
echo "Service Status"
echo "========================================"
systemctl is-active --quiet websocket-monitor.service && echo "✓ websocket-monitor.service is running" || echo "✗ websocket-monitor.service is NOT running"
systemctl is-active --quiet api-jobs.service && echo "✓ api-jobs.service is running" || echo "✗ api-jobs.service is NOT running"

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo "Virtual Environment: ${VENV_PATH}"
echo "Python package: $PYTHON_JOBS_PATH"
echo "Node.js package: $NODE_JOBS_PATH"
echo "========================================"
