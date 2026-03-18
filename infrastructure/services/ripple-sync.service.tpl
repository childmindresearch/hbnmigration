[Unit]
Description=Ripple to REDCap Sync Service
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/ripple-to-redcap

# Logging
StandardOutput=append:${log_directory}/ripple-sync.log
StandardError=append:${log_directory}/ripple-sync-error.log
SyslogIdentifier=ripple-sync

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
