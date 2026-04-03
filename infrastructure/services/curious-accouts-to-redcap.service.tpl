[Unit]
Description=Curious invitations to REDCap Sync Service [${workspace}]
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/.venv/bin/curious-invitations-to-redcap
Environment="WORKSPACE=${workspace}"

# Logging
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
StandardOutput=append:${log_directory}/curious-invitations-to-redcap.log
StandardError=append:${log_directory}/curious-invitations-to-redcap.log
SyslogIdentifier=curious-invitations-to-redcap-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
