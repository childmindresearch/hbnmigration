[Unit]
Description=REDCap to REDCap Sync Service
After=network.target

[Service]
Type=oneshot
User=${USER}
Group=${USER_GROUP}

# Use the virtual environment Python
ExecStart=${VENV_PATH}/bin/redcap-to-redcap
Environment="USER_GROUP=${USER_GROUP}"

# Logging
StandardOutput=append:/var/log/redcap-sync/sync.log
StandardError=append:/var/log/redcap-sync/error.log

# Restart policy (don't restart on failure for oneshot)
Restart=no

# Security settings
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
