[Unit]
Description=Curious invitations to REDCap Sync Service [${workspace}]
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/curious-invitations-to-redcap
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Environment="HBNMIGRATION_LOG_PATH=${log_directory}"

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
ReadWritePaths=%h/.hbnmigration_cache

[Install]
WantedBy=multi-user.target
