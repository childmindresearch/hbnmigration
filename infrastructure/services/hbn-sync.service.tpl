[Unit]
Description=HBN Sync Service - Runs All Sync Services [default]
After=network.target
Wants=ripple-to-redcap.service redcap-to-redcap.service redcap-to-curious.service curious-accounts-to-redcap.service curious-alerts-websocket.service curious-data-to-redcap.service

[Service]
Type=oneshot
# This service just triggers the other services
ExecStart=/bin/true

# The real work happens via dependencies
ExecStartPost=/usr/bin/systemctl start ripple-to-redcap.service
ExecStartPost=/usr/bin/systemctl start redcap-to-redcap.service
ExecStartPost=/usr/bin/systemctl start redcap-to-curious.service
ExecStartPost=/usr/bin/systemctl start curious-accounts-to-redcap.service
ExecStartPost=/usr/bin/systemctl start curious-alerts-websocket.service
ExecStartPost=/usr/bin/systemctl start curious-data-to-redcap.service

[Install]
WantedBy=multi-user.target
