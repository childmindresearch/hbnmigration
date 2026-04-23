[Unit]
Description=REDCap to Curious Batch Sync [${workspace}]
After=network.target
PartOf=hbn-sync.service

[Service]
Type=oneshot
User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
ExecStart=${venv_path}/bin/redcap-to-curious
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Environment="HBNMIGRATION_LOG_ROOT=${log_directory}"
Environment="HBNMIGRATION_PROJECT_STATUS=${project_status}"
Environment="HBNMIGRATION_RECOVERY_MODE=${recovery_mode ? "1" : "0"}"

# Timeouts
TimeoutStartSec=300
TimeoutStopSec=30

# Logging
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
StandardOutput=append:${log_directory}/redcap-to-curious.log
StandardError=append:${log_directory}/redcap-to-curious.log
SyslogIdentifier=redcap-to-curious-batch-${workspace}

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}
ReadWritePaths=${project_root}/.hbnmigration_cache

[Install]
WantedBy=hbn-sync.timer
