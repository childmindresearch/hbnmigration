[Unit]
Description=REDCap to REDCap Batch Sync [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/redcap-to-redcap

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging
StandardOutput=append:${log_directory}/redcap-to-redcap.log
StandardError=append:${log_directory}/redcap-to-redcap.log
SyslogIdentifier=redcap-to-redcap-batch-${workspace}

[Install]
WantedBy=hbn-sync.timer
