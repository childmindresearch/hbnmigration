[Unit]
Description=Ripple, REDCap and Curious Sync Timer [default]
Requires=hbn-sync.service

[Timer]
OnCalendar=*:0/1
Persistent=true

[Install]
WantedBy=timers.target
