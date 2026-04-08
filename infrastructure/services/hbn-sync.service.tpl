[Unit]
Description=HBN Sync Service - Runs All Sync Services [${workspace}]
After=network.target
Wants=ripple-to-redcap.service redcap-to-redcap.service redcap-to-curious.service curious-accounts-to-redcap.service curious-data-to-redcap.service

[Service]
Type=oneshot
ExecStart=/bin/true

[Install]
WantedBy=multi-user.target
