[Unit]
Description=Curious data to REDCap Sync Service [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/curious-data-to-redcap

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging

StandardOutput=append:${log_directory}/curious-data-to-redcap.log
StandardError=append:${log_directory}/curious-data-to-redcap.log
SyslogIdentifier=curious-data-to-redcap-${workspace}

[Install]
WantedBy=hbn-sync.timer
