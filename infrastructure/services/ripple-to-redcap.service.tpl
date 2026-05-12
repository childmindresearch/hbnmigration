[Unit]
Description=Ripple to REDCap Sync Service [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/ripple-to-redcap

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging

StandardOutput=append:${log_directory}/ripple-to-redcap.log
StandardError=append:${log_directory}/ripple-to-redcap-error.log
SyslogIdentifier=ripple-to-redcap-${workspace}

[Install]
WantedBy=hbn-sync.timer
