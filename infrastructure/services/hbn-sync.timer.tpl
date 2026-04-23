[Unit]
Description=Ripple, REDCap and Curious Sync Timer [${workspace}]

[Timer]
OnCalendar=*:0/${sync_interval_minutes}
Persistent=true

[Install]
WantedBy=timers.target
