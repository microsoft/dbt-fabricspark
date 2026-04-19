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

docker compose -f "$COMPOSE_FILE" up -d

echo "Waiting for MSSQL Server to be ready..."
until sqlcmd -Q "SELECT 1" &>/dev/null; do
    echo "  ...waiting for MSSQL to accept connections"
    sleep 2
done
echo "MSSQL Server is ready."

DB_EXISTS=$(sqlcmd -h -1 -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='metastore'" 2>/dev/null | tr -d ' \r\n\t')
if [ -z "$DB_EXISTS" ] || [ "$DB_EXISTS" -eq "0" ] 2>/dev/null; then
    sqlcmd -Q "CREATE DATABASE metastore"
fi

TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "$QUERY_TABLE" 2>/dev/null | tr -d ' \r\n\t')
if [ -z "$TABLE_COUNT" ] || [ "$TABLE_COUNT" != "$EXPECTED_TABLE_COUNT" ] 2>/dev/null; then
    cat "$SCHEMA_FILE" | docker exec -i "$CONTAINER" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -C -d metastore -b >/dev/null
    NEW_TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "$QUERY_TABLE" 2>/dev/null | tr -d ' \r\n')
    echo "Schema initialized ($NEW_TABLE_COUNT tables)"
else
    echo "Schema exists ($TABLE_COUNT tables)"
fi
