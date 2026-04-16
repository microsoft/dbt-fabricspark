#!/bin/bash
#
# Initialize the Hive metastore database and schema in MSSQL Server.
# Runs as the metastore-init Docker Compose service.
#
set -e

SA_PASSWORD="Hive@Pass123"
SCHEMA_FILE="/opt/init/hive-schema-4.0.0.mssql.sql"
EXPECTED_TABLE_COUNT=82

sqlcmd() {
    /opt/mssql-tools18/bin/sqlcmd -S mssql -U sa -P "$SA_PASSWORD" -C "$@"
}

echo "Creating metastore database if not exists..."
sqlcmd -Q "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'metastore') CREATE DATABASE metastore"

TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'" 2>/dev/null | tr -d ' \r\n')
if [ "$TABLE_COUNT" != "$EXPECTED_TABLE_COUNT" ]; then
    echo "Initializing Hive metastore schema..."
    sqlcmd -d metastore -b -i "$SCHEMA_FILE" >/dev/null
    NEW_TABLE_COUNT=$(sqlcmd -d metastore -h -1 -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'" 2>/dev/null | tr -d ' \r\n')
    echo "Schema initialized ($NEW_TABLE_COUNT tables)"
else
    echo "Schema already exists ($TABLE_COUNT tables)"
fi
