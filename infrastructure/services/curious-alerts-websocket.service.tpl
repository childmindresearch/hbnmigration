[Unit]
Description=Curious Alerts to REDCap WebSocket Service (Always-On) [${workspace}]
After=network.target

[Service]
Type=simple
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/curious-alerts-to-redcap --asynchronous
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Restart=always
RestartSec=10

# Logging
StandardOutput=append:${log_directory}/curious-alerts-websocket.log
StandardError=append:${log_directory}/curious-alerts-websocket-error.log
SyslogIdentifier=curious-alerts-websocket-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
