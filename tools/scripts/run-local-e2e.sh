#!/usr/bin/env bash
#
# End-to-end test for one local connection backend. Nx launches the Livy and
# Spark Session variants concurrently against separate databases.
#
set -euo pipefail

BACKEND="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
JAFFLE_SHOP_SRC="${ADAPTER_DIR}/tests/fixtures/dbt-jaffle-shop"
WORK_DIR="$(mktemp -d)"
VENV_DIR="${WORK_DIR}/.venv-e2e"
JAFFLE_SHOP_DIR="${WORK_DIR}/dbt-jaffle-shop"
PROFILES_DIR="${WORK_DIR}/profiles"
DBT_THREADS="${DBT_THREADS:-4}"
export DBT_THREADS
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export SPARK_CONF_DIR="${SPARK_CONF_DIR:-${SPARK_HOME}/conf}"

case "${BACKEND}" in
    livy)
        TARGET="local-livy"
        E2E_DATABASE="dbt_livy_e2e"
        SESSION_ID_FILE="${WORK_DIR}/livy-session-id.txt"
        ;;
    session)
        TARGET="local-session"
        E2E_DATABASE="dbt_session_e2e"
        ;;
    *)
        echo "Usage: $0 <livy|session>" >&2
        exit 2
        ;;
esac

cleanup() {
    if [[ "${BACKEND}" == "livy" && -s "${SESSION_ID_FILE:-}" ]]; then
        local session_id
        session_id=$(tr -d '[:space:]' < "${SESSION_ID_FILE}")
        if [[ "${session_id}" =~ ^[0-9]+$ ]]; then
            curl -fsS -X DELETE "http://localhost:8998/sessions/${session_id}" >/dev/null || true
        fi
    fi
    echo ""
    echo "[cleanup:${BACKEND}] Removing work directory..."
    rm -rf "${WORK_DIR:?}" 2>/dev/null || true
}
trap cleanup EXIT

echo "============================================"
echo " dbt-fabricspark local end-to-end test"
echo " Backend: ${BACKEND}"
echo " Database: ${E2E_DATABASE}"
echo "============================================"

WHEEL=$(find "${ADAPTER_DIR}/dist" -maxdepth 1 -name '*.whl' -print -quit)
if [[ -z "${WHEEL}" ]]; then
    echo "ERROR: No wheel found under ${ADAPTER_DIR}/dist" >&2
    exit 1
fi
echo "  [${BACKEND}] Using wheel: ${WHEEL}"

uv venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"

if [[ "${BACKEND}" == "session" ]]; then
    SPARK_RUNTIME_VERSION=$(
        sed -n "s/^__version__: str = ['\"]\\([^'\"]*\\)['\"]$/\\1/p" \
            "${SPARK_HOME}/python/pyspark/version.py"
    )
    if [[ -z "${SPARK_RUNTIME_VERSION}" ]]; then
        echo "ERROR: Could not determine the PySpark version from ${SPARK_HOME}" >&2
        exit 1
    fi
    uv pip install \
        "${WHEEL}[spark]" \
        "pyspark==${SPARK_RUNTIME_VERSION}" \
        "dbt-core<2.0"
    export PYSPARK_PYTHON="${VENV_DIR}/bin/python"
else
    uv pip install "${WHEEL}" "dbt-core<2.0"
    python -c "import importlib.util; assert importlib.util.find_spec('pyspark') is None"
fi

cp -r "${JAFFLE_SHOP_SRC}" "${JAFFLE_SHOP_DIR}"
sed -i '/+database:/d; /+schema:/d' "${JAFFLE_SHOP_DIR}/dbt_project.yml"
sed -i "s/schema: dbt_jaffle_shop_seed/schema: ${E2E_DATABASE}/" \
    "${JAFFLE_SHOP_DIR}/models/staging/sources.yml"

cat > "${JAFFLE_SHOP_DIR}/macros/local_overrides.sql" <<'MACROEOF'
{% macro generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(none) %}
{%- endmacro %}

{% macro ensure_database_exists(schema_name, database=none, workspace=none) -%}
  {%- call statement('ensure_database_exists') -%}
    create database if not exists {{ schema_name }}
  {%- endcall -%}
{%- endmacro %}

{% macro reset_e2e_database(database_name) -%}
  {%- call statement('reset_e2e_database') -%}
    drop database if exists {{ database_name }} cascade
  {%- endcall -%}
{%- endmacro %}

{% macro insert_e2e_source_rows(database_name) -%}
  {%- call statement('insert_e2e_orders') -%}
    insert into {{ database_name }}.raw_orders (id, user_id, order_date, status)
    values
      (100, 1, '2018-04-10', 'placed'),
      (101, 2, '2018-04-11', 'completed'),
      (102, 3, '2018-04-12', 'shipped')
  {%- endcall -%}
  {%- call statement('insert_e2e_payments') -%}
    insert into {{ database_name }}.raw_payments (id, order_id, payment_method, amount)
    values
      (1000, 100, 'credit_card', 1000),
      (1001, 101, 'coupon', 2000),
      (1002, 102, 'gift_card', 3000)
  {%- endcall -%}
{%- endmacro %}
MACROEOF

mkdir -p "${PROFILES_DIR}"
if [[ "${BACKEND}" == "livy" ]]; then
    cat > "${PROFILES_DIR}/profiles.yml" <<EOF
jaffle_shop:
  target: ${TARGET}
  outputs:
    ${TARGET}:
      authentication: cli
      method: livy
      livy_mode: local
      session_id_file: ${SESSION_ID_FILE}
      connect_retries: 25
      connect_timeout: 10
      lakehouse: ${E2E_DATABASE}
      schema: ${E2E_DATABASE}
      threads: ${DBT_THREADS}
      type: fabricspark
      retry_all: true
      spark_config:
        name: dbt-jaffle-shop-e2e-livy
EOF
else
    cat > "${PROFILES_DIR}/profiles.yml" <<EOF
jaffle_shop:
  target: ${TARGET}
  outputs:
    ${TARGET}:
      method: session
      lakehouse: ${E2E_DATABASE}
      schema: ${E2E_DATABASE}
      threads: ${DBT_THREADS}
      type: fabricspark
      spark_config:
        name: dbt-jaffle-shop-e2e-session
        conf:
          spark.master: local[${DBT_THREADS}]
EOF
fi

export DBT_PROFILES_DIR="${PROFILES_DIR}"
cd "${JAFFLE_SHOP_DIR}"

dbt debug --target "${TARGET}"
dbt run-operation reset_e2e_database \
    --args "{database_name: ${E2E_DATABASE}}" \
    --target "${TARGET}"
dbt clean --target "${TARGET}"
dbt seed --target "${TARGET}" --full-refresh
dbt run --target "${TARGET}"
dbt test --exclude tag:merge_options --target "${TARGET}"

echo ""
echo "============================================"
echo " [${BACKEND}] Incremental / full-refresh cycle"
echo "============================================"

echo ""
echo "--- [${BACKEND}:incremental_orders] Trickle-inserting 3 new orders and payments ---"
dbt run-operation insert_e2e_source_rows \
    --args "{database_name: ${E2E_DATABASE}}" \
    --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders] Incremental run ---"
dbt run --select incremental_orders --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders] Full-refresh run ---"
dbt run --select incremental_orders --full-refresh --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders_delete_insert] Incremental run ---"
dbt run --select incremental_orders_delete_insert --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders_delete_insert] Full-refresh run ---"
dbt run --select incremental_orders_delete_insert --full-refresh --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders_merge_options] Advanced merge run ---"
dbt run --select incremental_orders_merge_options --target "${TARGET}"

echo ""
echo "--- [${BACKEND}:incremental_orders_merge_options] CDC merge assertion ---"
dbt test --select incremental_orders_merge_options --target "${TARGET}"

echo ""
echo "============================================"
echo " [${BACKEND}] Remaining dbt lifecycle commands"
echo "============================================"

dbt build --exclude resource_type:seed --target "${TARGET}"
dbt compile --target "${TARGET}"
dbt ls --target "${TARGET}"
dbt show --select customers --limit 5 --target "${TARGET}"
dbt parse --target "${TARGET}"
dbt docs generate --target "${TARGET}"

METASTORE_COUNT=$(
    docker exec metastore-mssql-1 \
        /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P "Hive@Pass123" -C -d metastore -h -1 \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM DBS WHERE NAME='${E2E_DATABASE}'" |
        tr -d '[:space:]'
)
if [[ "${METASTORE_COUNT}" != "1" ]]; then
    echo "ERROR: Database ${E2E_DATABASE} was not registered in the shared Hive metastore" >&2
    exit 1
fi

echo ""
echo "============================================"
echo " [${BACKEND}] All dbt commands completed successfully!"
echo "============================================"
