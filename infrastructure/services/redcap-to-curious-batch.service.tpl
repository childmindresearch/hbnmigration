[Unit]
Description=REDCap to Curious Batch Sync [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/redcap-to-curious

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging
StandardOutput=append:${log_directory}/redcap-to-curious.log
StandardError=append:${log_directory}/redcap-to-curious.log
SyslogIdentifier=redcap-to-curious-batch-${workspace}

[Install]
WantedBy=hbn-sync.timer
