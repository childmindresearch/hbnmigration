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
Environment="HBNMIGRATION_LOG_PATH=${log_directory}"
Environment="HBNMIGRATION_PROJECT_STATUS=${project_status}"
Environment="HBNMIGRATION_RECOVERY_MODE=${recovery_mode ? "1" : "0"}"
Restart=always
RestartSec=10

# Logging
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
StandardOutput=append:${log_directory}/curious-alerts-websocket.log
StandardError=append:${log_directory}/curious-alerts-websocket-error.log
SyslogIdentifier=curious-alerts-websocket-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
