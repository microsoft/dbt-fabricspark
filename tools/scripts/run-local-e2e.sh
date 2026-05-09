#!/usr/bin/env bash
#
# End-to-end test: build the dbt-fabricspark wheel and run a full dbt lifecycle
# against the jaffle-shop project on local Livy (devcontainer).
#
# Prerequisites: Local Livy on port 8998, SQL Server metastore (via init target), uv
#
set -euo pipefail

TARGET="${1:-local-local}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAFFLE_SHOP_SRC="${ADAPTER_DIR}/tests/fixtures/dbt-jaffle-shop"
WORK_DIR="$(mktemp -d)"
VENV_DIR="${WORK_DIR}/.venv-e2e"
JAFFLE_SHOP_DIR="${WORK_DIR}/dbt-jaffle-shop"
SESSION_ID_FILE="${WORK_DIR}/livy-session-id.txt"
PROFILES_DIR="${WORK_DIR}/profiles"
WAREHOUSE_DIR="${ADAPTER_DIR}/warehouse"
JAFFLE_SCHEMA="dbt_jaffle_shop_dwh"
DBT_THREADS="${DBT_THREADS:-4}"
export DBT_THREADS

trap 'echo ""; echo "[cleanup] Removing temp venv and work dir..."; rm -rf "${VENV_DIR}" "${WORK_DIR}" 2>/dev/null || true' EXIT

echo "============================================"
echo " dbt-fabricspark local end-to-end test"
echo " Target: ${TARGET}"
echo "============================================"

source "${SCRIPT_DIR}/run-livy.sh"
kill_all_sessions

rm -rf "${WAREHOUSE_DIR}/${JAFFLE_SCHEMA}.db" 2>/dev/null || true

WHEEL=$(ls "${ADAPTER_DIR}"/dist/*.whl | head -1)
echo "  Using wheel: ${WHEEL}"

rm -rf "${VENV_DIR}"
uv venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
uv pip install "${WHEEL}" dbt-core

cp -r "${JAFFLE_SHOP_SRC}" "${JAFFLE_SHOP_DIR}"
sed -i '/+database:/d; /+schema:/d' "${JAFFLE_SHOP_DIR}/dbt_project.yml"
sed -i 's/schema: dbt_jaffle_shop_seed/schema: dbt_jaffle_shop_dwh/' "${JAFFLE_SHOP_DIR}/models/staging/sources.yml"

cat > "${JAFFLE_SHOP_DIR}/macros/local_overrides.sql" <<'MACROEOF'
{% macro generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(none) %}
{%- endmacro %}

{% macro ensure_database_exists(schema_name, database=none, workspace=none) -%}
  {%- call statement('ensure_database_exists') -%}
    create database if not exists {{ schema_name }}
  {%- endcall -%}
{%- endmacro %}
MACROEOF

mkdir -p "${PROFILES_DIR}"
cat > "${PROFILES_DIR}/profiles.yml" <<EOF
jaffle_shop:
  target: ${TARGET}
  outputs:
    local-local:
      authentication: cli
      method: livy
      livy_mode: local
      session_id_file: ${SESSION_ID_FILE}
      connect_retries: 25
      connect_timeout: 10
      lakehouse: ${JAFFLE_SCHEMA}
      schema: ${JAFFLE_SCHEMA}
      threads: ${DBT_THREADS}
      type: fabricspark
      retry_all: true
      spark_config:
        name: dbt-jaffle-shop-e2e
EOF

export DBT_PROFILES_DIR="${PROFILES_DIR}"
cd "${JAFFLE_SHOP_DIR}"

dbt debug --target "${TARGET}"
dbt clean --target "${TARGET}"
dbt seed --target "${TARGET}" --full-refresh
dbt run --target "${TARGET}"
dbt test --target "${TARGET}"

echo ""
echo "============================================"
echo " Incremental / full-refresh cycle"
echo "============================================"

echo ""
echo "--- [incremental_orders] Trickle-inserting 3 new orders via Livy SQL ---"
echo "    (simulates new source data arriving after the initial load)"
TRICKLE_SESSION=$(create_session pyspark | tail -1)
execute_code "${TRICKLE_SESSION}" \
  "spark.sql(\"\"\"INSERT INTO ${JAFFLE_SCHEMA}.raw_orders (id, user_id, order_date, status) VALUES (100, 1, '2018-04-10', 'placed'), (101, 2, '2018-04-11', 'completed'), (102, 3, '2018-04-12', 'shipped')\"\"\")" \
  60
kill_session "${TRICKLE_SESSION}"

echo ""
echo "--- [incremental_orders] Incremental run (picks up new rows by order_date) ---"
dbt run --select incremental_orders --target "${TARGET}"

echo ""
echo "--- [incremental_orders] Full-refresh run (regression: must not raise TABLE_OR_VIEW_ALREADY_EXISTS) ---"
dbt run --select incremental_orders --full-refresh --target "${TARGET}"

echo ""
echo "============================================"
echo " Resuming remaining dbt lifecycle commands"
echo "============================================"

dbt build --exclude resource_type:seed --target "${TARGET}"
dbt compile --target "${TARGET}"
dbt ls --target "${TARGET}"
dbt show --select customers --limit 5 --target "${TARGET}"
dbt parse --target "${TARGET}"
dbt docs generate --target "${TARGET}"

echo ""
echo "============================================"
echo " All dbt commands completed successfully!"
echo "============================================"

deactivate
