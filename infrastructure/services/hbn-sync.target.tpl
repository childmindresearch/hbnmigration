[Unit]
Description=HBN Sync Target - Groups All Sync Services [${workspace}]
Wants=${join(" ", [for s in services : "${s}.service"])}

[Install]
WantedBy=multi-user.target
