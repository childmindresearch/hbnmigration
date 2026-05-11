[Unit]
Description=REDCap to REDCap Webhook Service [${workspace}]
After=network.target
Wants=network-online.target

[Service]
Type=simple
Environment="PATH=${venv_path}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="PYTHONPATH=${project_root}/python_jobs/src"

# Inject SSOT common config
${common_config}
${async_timeouts}

ExecStart=${venv_path}/bin/uvicorn hbnmigration.from_redcap.to_redcap:app --host 0.0.0.0 --port 8001 --workers 2
Restart=always
RestartSec=10

# Logging
StandardOutput=append:${log_directory}/redcap-to-redcap-webhook.log
StandardError=append:${log_directory}/redcap-to-redcap-webhook.log
SyslogIdentifier=redcap-to-redcap-webhook-${workspace}

[Install]
WantedBy=multi-user.target
