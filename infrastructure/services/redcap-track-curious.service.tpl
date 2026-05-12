[Unit]
Description=REDCap track Curious data Service [${workspace}]
After=network.target curious-data-to-redcap.service
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/redcap-track-curious

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging
StandardOutput=append:${log_directory}/redcap-track-curious.log
StandardError=append:${log_directory}/redcap-track-curious.log
SyslogIdentifier=redcap-track-curious-${workspace}

[Install]
WantedBy=hbn-sync.timer
