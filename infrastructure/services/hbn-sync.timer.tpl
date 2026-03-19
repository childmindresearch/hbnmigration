[Unit]
Description=Ripple, REDCap and Curious Sync Timer [${workspace}]
Requires=${service_name}.service

[Timer]
OnCalendar=*:0/${sync_interval_minutes}
Persistent=true

[Install]
WantedBy=timers.target
