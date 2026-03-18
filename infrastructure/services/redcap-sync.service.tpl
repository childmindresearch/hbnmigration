[Unit]
Description=REDCap Sync Service
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/redcap-sync

# Logging
StandardOutput=append:${log_directory}/redcap-sync.log
StandardError=append:${log_directory}/redcap-sync-error.log
SyslogIdentifier=redcap-sync

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
