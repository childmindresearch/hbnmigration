[Unit]
Description=REDCap to Curious Webhook Service
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=${service_user}
Group=${service_group}
WorkingDirectory=${working_directory}
Environment="PATH=${python_venv}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="PYTHONPATH=${working_directory}/python_jobs/src"
ExecStart=${python_venv}/bin/uvicorn hbnmigration.from_redcap.to_curious:app --host 0.0.0.0 --port 8002 --workers 2
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=redcap-to-curious

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=${working_directory} /var/log/hbnmigration /tmp/hbn_cache

[Install]
WantedBy=multi-user.target
