[Unit]
Description=Ripple, REDCap and Curious Sync Timer [${workspace}]
Requires=${service_prefix}ripple-to-redcap.service
Requires=${service_prefix}redcap-to-redcap.service
Requires=${service_prefix}redcap-to-curious.service

[Timer]
OnCalendar=*:0/${sync_interval_minutes}
Persistent=true

[Install]
WantedBy=timers.target
