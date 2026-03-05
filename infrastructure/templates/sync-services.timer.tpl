[Unit]
Description=Ripple and REDCap Sync Timer
Requires=ripple-sync.service
Requires=redcap-sync.service

[Timer]
# Run 2 minutes after boot
OnBootSec=2min
# Run every ${INTERVAL_MINUTES} minutes after the last activation
OnUnitActiveSec=${INTERVAL_MINUTES}min
# Keep timer accurate to within 1 second
AccuracySec=1s
# If the system was off when timer should have triggered, run it on next boot
Persistent=true

[Install]
WantedBy=timers.target
