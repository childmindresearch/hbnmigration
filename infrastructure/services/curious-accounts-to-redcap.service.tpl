[Unit]
Description=Curious invitations to REDCap Sync Service [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
ExecStart=${venv_path}/bin/curious-invitations-to-redcap

# Inject SSOT common config
${common_config}
${oneshot_timeouts}

# Logging
StandardOutput=append:${log_directory}/curious-invitations-to-redcap.log
StandardError=append:${log_directory}/curious-invitations-to-redcap.log
SyslogIdentifier=curious-invitations-to-redcap-${workspace}


[Install]
WantedBy=hbn-sync.timer
