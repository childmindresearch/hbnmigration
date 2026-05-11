[Unit]
Description=REDCap to Curious Webhook Service [${workspace}]
After=network.target
Wants=network-online.target

[Service]
Type=simple

# Inject SSOT common config
${common_config}
${async_timeouts}

ExecStart=${venv_path}/bin/uvicorn hbnmigration.from_redcap.to_curious:app --host 0.0.0.0 --port 8002 --workers 2
Restart=always
RestartSec=10

# Logging
StandardOutput=append:${log_directory}/redcap-to-curious-webhook.log
StandardError=append:${log_directory}/redcap-to-curious-webhook.log
SyslogIdentifier=redcap-to-curious-webhook-${workspace}

[Install]
WantedBy=multi-user.target
