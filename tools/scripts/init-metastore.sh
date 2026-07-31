#!/bin/bash
#
#       Initialize the Hive metastore schema in MSSQL Server
#       using a local copy of the Hive 4.0.0 schema.
#
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker/Compose.sqlserver.metastore.yaml"
CONTAINER="metastore-mssql-1"
SCHEMA_FILE="${SCRIPT_DIR}/hive-schema-4.0.0.mssql.sql"
QUERY_TABLE="SET NOCOUNT ON; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
SA_PASSWORD="Hive@Pass123"
EXPECTED_TABLE_COUNT=82

sqlcmd() {
    docker exec "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C "$@"
}

docker compose -f "$COMPOSE_FILE" up -d --wait

echo "Waiting for MSSQL Server to be ready..."

READY=0
for _ in $(seq 1 60); do
    if sqlcmd -b -Q "SELECT 1" &>/dev/null; then
        READY=1
        break
    fi
    echo "  ...waiting for MSSQL to accept connections"
    sleep 2
done
if [ "$READY" -ne 1 ]; then
    echo "ERROR: MSSQL Server did not become ready in time"
    exit 1
fi
echo "MSSQL Server is ready."

DB_EXISTS=$(sqlcmd -h -1 -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='metastore'" 2>/dev/null | tr -d ' \r\n\t')
if [ -z "$DB_EXISTS" ] || [ "$DB_EXISTS" -eq "0" ] 2>/dev/null; then
    sqlcmd -b -Q "CREATE DATABASE metastore"
fi

TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "$QUERY_TABLE" 2>/dev/null | tr -d ' \r\n\t' || true)
if [ -z "$TABLE_COUNT" ] || [ "$TABLE_COUNT" != "$EXPECTED_TABLE_COUNT" ] 2>/dev/null; then
    docker cp "$SCHEMA_FILE" "$CONTAINER:/tmp/hive-schema.sql" >/dev/null
    sqlcmd -d metastore -b -i /tmp/hive-schema.sql >/dev/null
    TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "$QUERY_TABLE" 2>/dev/null | tr -d ' \r\n\t' || true)
    echo "Schema initialized ($TABLE_COUNT tables)"
else
    echo "Schema exists ($TABLE_COUNT tables)"
fi

if [ "$TABLE_COUNT" != "$EXPECTED_TABLE_COUNT" ]; then
    echo "ERROR: Hive metastore schema is incomplete — expected $EXPECTED_TABLE_COUNT tables, found '${TABLE_COUNT:-0}'"
    exit 1
fi
