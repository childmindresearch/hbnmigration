[Unit]
Description=REDCap to REDCap Webhook Service [${workspace}]
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
Environment="PATH=${venv_path}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="PYTHONPATH=${project_root}/python_jobs/src"
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Environment="HBNMIGRATION_LOG_ROOT=${log_directory}"
Environment="HBNMIGRATION_PROJECT_STATUS=${project_status}"
Environment="HBNMIGRATION_RECOVERY_MODE=${recovery_mode ? "1" : "0"}"
ExecStart=${venv_path}/bin/uvicorn hbnmigration.from_redcap.to_redcap:app --host 0.0.0.0 --port 8001 --workers 2
Restart=always
RestartSec=10

# Logging
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
StandardOutput=append:${log_directory}/redcap-to-redcap-webhook.log
StandardError=append:${log_directory}/redcap-to-redcap-webhook.log
SyslogIdentifier=redcap-to-redcap-webhook-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}

[Install]
WantedBy=multi-user.target
