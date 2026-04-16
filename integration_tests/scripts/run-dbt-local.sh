#!/bin/bash
#
# End-to-end dbt CLI lifecycle test for dbt-fabricspark.
#
# Downloads seeds, starts Spark+Livy via Docker Compose (unless SPARK_SKIP_DOCKER=1),
# builds the adapter wheel, creates a temporary venv, installs everything, and runs
# the full dbt lifecycle: debug → deps → seed → run → test → docs generate.
#
# Set SPARK_SKIP_DOCKER=1 to skip Docker start/stop (e.g. when called from run.sh all).
# Set SKIP_TEARDOWN=1 to keep Spark+Livy running after the test.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEGRATION_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_DIR="$(cd "${INTEGRATION_DIR}/.." && pwd)"
DBT_PROJECT="${INTEGRATION_DIR}/dbt-adventureworks"
SEED_MANIFEST="ci_seeds.yaml"
DOCKER_COMPOSE_FILE="${INTEGRATION_DIR}/docker-compose.yml"
DOCKER_PROJECT="fabricspark-dbt-test"
LIVY_URL="${LIVY_URL:-http://localhost:8998}"
SKIP_DOCKER="${SPARK_SKIP_DOCKER:-}"
SKIP_TEARDOWN="${SKIP_TEARDOWN:-}"

E2E_VENV=""

nuke_docker() {
    docker compose -f "$DOCKER_COMPOSE_FILE" -p "$DOCKER_PROJECT" down -v --remove-orphans 2>/dev/null || true
    local stale
    stale=$(docker ps -aq --filter "label=com.docker.compose.project=${DOCKER_PROJECT}" 2>/dev/null || true)
    if [[ -n "$stale" ]]; then
        echo "$stale" | xargs -r docker rm -f 2>/dev/null || true
    fi
    docker network rm spark-network 2>/dev/null || true
    local vols
    vols=$(docker volume ls -q --filter "label=com.docker.compose.project=${DOCKER_PROJECT}" 2>/dev/null || true)
    if [[ -n "$vols" ]]; then
        echo "$vols" | xargs -r docker volume rm -f 2>/dev/null || true
    fi
}

cleanup() {
    local rc=$?
    if [[ -n "$E2E_VENV" && -d "$E2E_VENV" ]]; then
        echo "Cleaning up temp venv..."
        rm -rf "$(dirname "$E2E_VENV")"
    fi
    if [[ -d "${PROJECT_DIR}/.venv" ]]; then
        # shellcheck disable=SC1091
        source "${PROJECT_DIR}/.venv/bin/activate" 2>/dev/null || true
    fi
    if [[ -z "$SKIP_TEARDOWN" && -z "$SKIP_DOCKER" ]]; then
        echo "Tearing down Docker Compose..."
        nuke_docker
    fi
    return $rc
}
trap cleanup EXIT

echo "=== dbt-fabricspark e2e test ==="

# 1. Download seeds
echo ""; echo "── Downloading seed data ──"
python "${SCRIPT_DIR}/download_seeds.py" --manifest "$SEED_MANIFEST" "$DBT_PROJECT"

# 2. Start Docker Compose (if not skipped)
if [[ -z "$SKIP_DOCKER" ]]; then
    echo ""; echo "── Starting Spark+Livy via Docker Compose ──"
    nuke_docker
    rm -rf "${DBT_PROJECT}/target" "${DBT_PROJECT}/dbt_packages" "${DBT_PROJECT}/logs"
    rm -f /tmp/dbt-fabricspark-integration/livy-session-id.txt
    docker compose -f "$DOCKER_COMPOSE_FILE" -p "$DOCKER_PROJECT" up -d --wait --wait-timeout 300

    for i in $(seq 1 60); do
        if curl -sf --connect-timeout 3 "${LIVY_URL}/sessions" >/dev/null 2>&1; then
            echo "Livy is healthy at ${LIVY_URL}"
            break
        fi
        if [[ $i -eq 60 ]]; then echo "ERROR: Livy did not become healthy"; exit 1; fi
        echo "Waiting for Livy... ($i/60)"
        sleep 5
    done
fi

# 3. Build the wheel
echo ""; echo "── Building adapter wheel ──"
cd "$PROJECT_DIR"
rm -rf dist/
uv build --wheel 2>&1 | tail -5
WHEEL=$(ls dist/*.whl)
echo "Built: ${WHEEL}"

# 4. Create temp venv and install
echo ""; echo "── Creating temp venv ──"
E2E_VENV=$(mktemp -d)/e2e-venv
uv venv "$E2E_VENV"
# shellcheck disable=SC1091
source "${E2E_VENV}/bin/activate"
uv pip install "${WHEEL}" dbt-core

# 5. Run dbt lifecycle
echo ""; echo "── dbt debug ──"
dbt debug --target local-local --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT"

echo ""; echo "── dbt deps ──"
dbt deps --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT"

echo ""; echo "── dbt seed ──"
dbt seed --target local-local --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT" --full-refresh

echo ""; echo "── dbt run ──"
dbt run --target local-local --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT"

echo ""; echo "── dbt test ──"
dbt test --target local-local --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT"

echo ""; echo "── dbt docs generate ──"
dbt docs generate --target local-local --profiles-dir "$DBT_PROJECT" --project-dir "$DBT_PROJECT"

echo ""
echo "=== e2e test completed successfully ==="
