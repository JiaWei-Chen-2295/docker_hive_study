#!/bin/bash

# Clean up HiveServer2/Metastore PID files to prevent "already running" errors on restart
rm -f /tmp/hive/hiveserver2.pid /tmp/hive/metastore.pid 2>/dev/null || true
rm -f /opt/hive/hiveserver2.pid /opt/hive/metastore.pid 2>/dev/null || true

# Execute the original entrypoint
exec /entrypoint.sh "$@"