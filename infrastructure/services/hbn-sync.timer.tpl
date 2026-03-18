[Unit]
Description=Ripple, REDCap and Curious Sync Timer
Requires=ripple-sync.service
Requires=redcap-sync.service
Requires=redcap-to-curious.service

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
