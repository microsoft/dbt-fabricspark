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
JAFFLE_SHOP_SRC="${ADAPTER_DIR}/.temp/git/spark-sandbox/projects/spark-dbt/dbt-jaffle-shop"
WORK_DIR="$(mktemp -d)"
VENV_DIR="${WORK_DIR}/.venv-e2e"
JAFFLE_SHOP_DIR="${WORK_DIR}/dbt-jaffle-shop"
SESSION_ID_FILE="${WORK_DIR}/livy-session-id.txt"
PROFILES_DIR="${WORK_DIR}/profiles"
WAREHOUSE_DIR="${ADAPTER_DIR}/warehouse"
DBT_THREADS="${DBT_THREADS:-4}"
export DBT_THREADS

trap 'echo ""; echo "[cleanup] Removing temp venv and work dir..."; rm -rf "${VENV_DIR}" "${WORK_DIR}" 2>/dev/null || true' EXIT

echo "============================================"
echo " dbt-fabricspark local end-to-end test"
echo " Target: ${TARGET}"
echo "============================================"

source "${SCRIPT_DIR}/run-livy.sh"
kill_all_sessions

rm -rf "${WAREHOUSE_DIR}/dbt_jaffle_shop_dwh.db" 2>/dev/null || true

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

{% macro ensure_database_exists(schema_name, database=none) -%}
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
      lakehouse: dbt_jaffle_shop_dwh
      schema: dbt_jaffle_shop_dwh
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
