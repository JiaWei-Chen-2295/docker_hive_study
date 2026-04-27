#!/bin/bash

# hiveserver2.sh (Hive 4) writes $$ to $HIVE_CONF_DIR/hiveserver2.pid before exec. In Docker the
# launcher shell is often PID 1, so the file contains "1". On the next start, before_start runs
# `kill -0` on that pid; kill -0 1 is always true, so startup fails with
# "HiveServer2 running as process 1. Stop it first."
HIVE_CONF="${HIVE_CONF_DIR:-/opt/hive/conf}"
rm -f "$HIVE_CONF/hiveserver2.pid" 2>/dev/null || true

# Legacy / alternate locations (kept for safety)
rm -f /tmp/hive/hiveserver2.pid /tmp/hive/metastore.pid 2>/dev/null || true
rm -f /opt/hive/hiveserver2.pid /opt/hive/metastore.pid 2>/dev/null || true

# Execute the original entrypoint
exec /entrypoint.sh "$@"