[Unit]
Description=Curious Alerts to REDCap WebSocket Service (Always-On) [${workspace}]
After=network.target

[Service]
Type=simple
ExecStart=${venv_path}/bin/curious-alerts-to-redcap --asynchronous

# Inject SSOT common config
${common_config}

# Timeouts
TimeoutStartSec=60
TimeoutStopSec=30

# Logging
StandardOutput=append:${log_directory}/curious-alerts-websocket.log
StandardError=append:${log_directory}/curious-alerts-websocket-error.log
SyslogIdentifier=curious-alerts-websocket-${workspace}

[Install]
WantedBy=multi-user.target
