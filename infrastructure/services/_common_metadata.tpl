User=${user_group}
Group=${user_group}
WorkingDirectory=${project_root}
Environment="WORKSPACE=${workspace}"
Environment="HBNMIGRATION_PROJECT_ROOT=${project_root}"
Environment="HBNMIGRATION_LOG_ROOT=${log_directory}"
Environment="HBNMIGRATION_PROJECT_STATUS=${project_status}"
Environment="HBNMIGRATION_RECOVERY_MODE=${recovery_mode ? "1" : "0"}"
Environment="PATH=${venv_path}/bin:/usr/local/bin:/usr/bin:/bin"
Environment="PYTHONPATH=${project_root}/python_jobs/src"

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ReadWritePaths=${project_root}
ReadWritePaths=${log_directory}
BindPaths=/data/logs/hbnmigration:/home/hbnmigration/hbnmigration/.hbnmigration_logs
