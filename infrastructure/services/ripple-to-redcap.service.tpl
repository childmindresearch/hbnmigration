[Unit]
Description=Ripple to REDCap Sync Service [${workspace}]
After=network.target

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/ripple-to-redcap
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Environment="HBNMIGRATION_LOG_ROOT=${log_directory}"
Environment="HBNMIGRATION_PROJECT_STATUS=${project_status}"
Environment="HBNMIGRATION_RECOVERY_MODE=${recovery_mode ? "1" : "0"}"

# Logging
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
StandardOutput=append:${log_directory}/ripple-to-redcap.log
StandardError=append:${log_directory}/ripple-to-redcap-error.log
SyslogIdentifier=ripple-to-redcap-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}
ReadWritePaths=${project_root}/.hbnmigration_cache

[Install]
WantedBy=multi-user.target
