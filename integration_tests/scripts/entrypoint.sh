#!/bin/bash
#
# Docker entrypoint for Spark+Livy integration test container.
# Configs are bind-mounted from integration_tests/config/.
# The metastore database is created by the metastore-init service.
# This script starts Livy and keeps the container alive.
#
set -e

export SPARK_HOME=/opt/spark
export LIVY_HOME=/opt/livy

echo "=== Spark+Livy Integration Test Container ==="

# Ensure warehouse directory exists and is writable
mkdir -p /opt/spark/warehouse

# Start Livy
echo "Starting Livy server..."
$LIVY_HOME/bin/livy-server start

# Wait for Livy to be healthy
retries=0
max_retries=60
until curl -s http://localhost:8998/sessions >/dev/null 2>&1; do
    sleep 1
    retries=$((retries + 1))
    if [ $retries -ge $max_retries ]; then
        echo "ERROR: Livy failed to start within ${max_retries}s timeout"
        cat /opt/livy/logs/*.log 2>/dev/null || true
        exit 1
    fi
done

echo "Livy is ready at http://localhost:8998"

# Keep the container running
exec tail -f /dev/null
