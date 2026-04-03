[Unit]
Description=REDCap to Curious Sync Service [${workspace}]
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/redcap-to-curious
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"

# Logging
StandardOutput=append:${log_directory}/redcap-to-curious.log
StandardError=append:${log_directory}/redcap-to-curious-error.log
SyslogIdentifier=redcap-to-curious-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
