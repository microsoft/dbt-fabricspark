#!/bin/bash
#
# dbt-fabricspark dev/test script. Requires Python 3.10+, uv, Docker (for integration/e2e).
#
# Usage: .scripts/run.sh <target>
#
# Targets: venv | build | fix | lint | unit-test | seed-ci | integration-test | e2e | all
#
# Each target is idempotent — auto-creates the venv if missing.
# Set SPARK_SKIP_DOCKER=1 / LIVY_URL to control Docker behavior.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
DOCKER_COMPOSE_FILE="${PROJECT_DIR}/integration_tests/docker-compose.yml"
DOCKER_PROJECT="fabricspark-dbt-test"
DBT_PROJECT="${PROJECT_DIR}/integration_tests/dbt-adventureworks"
SEED_MANIFEST="ci_seeds.yaml"

declare -A TARGETS=(
    ["venv"]="Fresh venv + install deps"
    ["build"]="Build wheel to dist/"
    ["fix"]="ruff auto-fix + format"
    ["lint"]="ruff check + format"
    ["unit-test"]="pytest unit tests"
    ["seed-ci"]="Download seed data from GitHub Gist"
    ["integration-test"]="pytest integration with Spark+Livy in Docker"
    ["e2e"]="dbt CLI end-to-end with Spark+Livy in Docker"
    ["all"]="Run all targets in sequence"
)
TARGET_ORDER=("venv" "build" "fix" "lint" "unit-test" "seed-ci" "integration-test" "e2e")

print_usage() {
    echo
    printf "  %-20s %s\n" "TARGET" "DESCRIPTION"
    printf "  %-20s %s\n" "all" "${TARGETS[all]}"
    for t in "${TARGET_ORDER[@]}"; do printf "  %-20s %s\n" "$t" "${TARGETS[$t]}"; done
    echo
    echo "Usage: .scripts/run.sh <target>"
    echo
}

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then echo "ERROR: No target provided."; print_usage; exit 1; fi
if [[ "$TARGET" != "all" && -z "${TARGETS[$TARGET]:-}" ]]; then echo "ERROR: Unknown target '$TARGET'"; print_usage; exit 1; fi

# ---------------------------------------------------------------------------
# Venv helpers
# ---------------------------------------------------------------------------

activate_venv() {
    # shellcheck disable=SC1091
    source "${VENV_DIR}/bin/activate"
}

create_venv() {
    rm -rf "${VENV_DIR}"
    cd "${PROJECT_DIR}"
    uv venv "${VENV_DIR}"
    activate_venv
    uv sync --all-extras
}

ensure_venv() {
    if [[ ! -d "${VENV_DIR}" ]]; then create_venv; else activate_venv; fi
}

# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------

clean_dbt_state() {
    rm -rf "${DBT_PROJECT}/target" "${DBT_PROJECT}/dbt_packages" "${DBT_PROJECT}/logs"
    rm -f /tmp/dbt-fabricspark-integration/livy-session-id.txt
}

nuke_docker() {
    # Compose down (best-effort)
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" down -v --remove-orphans 2>/dev/null || true

    # Force-remove any surviving containers by project label
    local stale
    stale=$(docker ps -aq --filter "label=com.docker.compose.project=${DOCKER_PROJECT}" 2>/dev/null || true)
    if [[ -n "$stale" ]]; then
        echo "$stale" | xargs -r docker rm -f 2>/dev/null || true
    fi

    # Remove the named network if it survived
    docker network rm spark-network 2>/dev/null || true

    # Remove project volumes
    local vols
    vols=$(docker volume ls -q --filter "label=com.docker.compose.project=${DOCKER_PROJECT}" 2>/dev/null || true)
    if [[ -n "$vols" ]]; then
        echo "$vols" | xargs -r docker volume rm -f 2>/dev/null || true
    fi
}

start_spark() {
    nuke_docker
    clean_dbt_state
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" up -d --wait --wait-timeout 300
}

wait_for_livy() {
    local base="${LIVY_URL:-http://localhost:8998}"
    local alt=""
    if [[ "$base" == *localhost* || "$base" == *127.0.0.1* ]]; then
        alt="${base//localhost/host.docker.internal}"
        alt="${alt//127.0.0.1/host.docker.internal}"
    fi

    # Phase 1: wait for Livy HTTP to respond
    for i in $(seq 1 60); do
        if curl -sf --connect-timeout 3 "${base}/sessions" >/dev/null 2>&1; then
            export LIVY_URL="$base"
            echo "Livy HTTP is up at ${base}"
            break
        fi
        if [[ -n "$alt" ]] && curl -sf --connect-timeout 3 "${alt}/sessions" >/dev/null 2>&1; then
            export LIVY_URL="$alt"
            echo "Livy HTTP is up at ${alt} (Docker-in-Docker fallback)"
            break
        fi
        if [[ $i -eq 60 ]]; then
            echo "ERROR: Livy did not become healthy in time"
            return 1
        fi
        echo "Waiting for Livy... ($i/60)"
        sleep 5
    done

    # Phase 2: create a warm-up session to ensure Spark is fully initialized
    echo "Warming up Livy (creating test session)..."
    # Clean stale session file before warmup
    rm -f /tmp/dbt-fabricspark-integration/livy-session-id.txt
    local session_id=""
    session_id=$(curl -sf -X POST "${LIVY_URL}/sessions" \
        -H "Content-Type: application/json" \
        -d '{"kind": "spark"}' 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || true)

    if [[ -n "$session_id" ]]; then
        for i in $(seq 1 60); do
            local state
            state=$(curl -sf "${LIVY_URL}/sessions/${session_id}" 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || true)
            if [[ "$state" == "idle" ]]; then
                echo "Livy session ${session_id} is idle — Spark is warm"
                # Delete the warm-up session so tests start clean
                curl -sf -X DELETE "${LIVY_URL}/sessions/${session_id}" >/dev/null 2>&1 || true
                sleep 2
                break
            fi
            if [[ "$state" == "dead" || "$state" == "error" ]]; then
                echo "WARNING: Warm-up session failed (state=$state), proceeding anyway"
                curl -sf -X DELETE "${LIVY_URL}/sessions/${session_id}" >/dev/null 2>&1 || true
                break
            fi
            echo "  Warming up Spark... ($i/60, state=$state)"
            sleep 3
        done
    else
        echo "WARNING: Could not create warm-up session, proceeding anyway"
    fi

    echo "Livy is ready at ${LIVY_URL}"
    return 0
}

spark_logs() {
    docker compose -f "${DOCKER_COMPOSE_FILE}" \
        -p "${DOCKER_PROJECT}" logs --tail=200
}

stop_spark() {
    nuke_docker
}

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------

run_venv()             { create_venv; }
run_build()            { ensure_venv; cd "${PROJECT_DIR}"; rm -rf dist/; uv build --wheel 2>&1 | tail -5; echo "  Built: $(ls dist/*.whl)"; }
run_fix()              { ensure_venv; cd "${PROJECT_DIR}"; uv run ruff check --fix src/ tests/; uv run ruff format src/ tests/; }
run_lint()             { ensure_venv; cd "${PROJECT_DIR}"; uv run ruff check src/ tests/; uv run ruff format --check src/ tests/; }
run_unit_test()        { ensure_venv; cd "${PROJECT_DIR}"; uv run pytest tests/unit -vv; }

run_seed_ci() {
    ensure_venv
    cd "${PROJECT_DIR}"
    echo "Downloading seed data from GitHub Gist..."
    uv run python integration_tests/scripts/download_seeds.py --manifest "$SEED_MANIFEST" "$DBT_PROJECT"
}

run_integration_test() {
    ensure_venv
    cd "${PROJECT_DIR}"

    run_seed_ci

    local skip_docker="${SPARK_SKIP_DOCKER:-}"

    if [[ -z "$skip_docker" ]]; then
        echo "Starting Spark+Livy via Docker..."
        start_spark
        wait_for_livy
    fi

    echo "Using LIVY_URL=${LIVY_URL:-http://localhost:8998}"

    local rc=0
    SPARK_SKIP_DOCKER=1 uv run pytest integration_tests/test_dbt_fabricspark.py -vv --timeout=1800 -m integration || rc=$?

    if [[ -z "$skip_docker" ]]; then
        if [[ $rc -ne 0 ]]; then
            echo ""; echo "── Spark+Livy logs (last 200 lines) ──"
            spark_logs || true
        fi
        echo "Stopping Spark+Livy..."
        stop_spark || true
    fi

    return $rc
}

run_e2e() {
    ensure_venv
    cd "${PROJECT_DIR}"
    run_seed_ci
    SPARK_SKIP_DOCKER="${SPARK_SKIP_DOCKER:-}" bash integration_tests/scripts/run-dbt-local.sh
}

# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

echo "=== dbt-fabricspark: ${TARGET} ==="

case "$TARGET" in
    "all")
        # Non-Docker targets first
        for t in "venv" "build" "fix" "lint" "unit-test" "seed-ci"; do
            echo ""; echo "── ${t} ──"
            "run_${t//-/_}"
        done

        # Docker targets share a single stack with guaranteed cleanup
        echo ""; echo "── docker-start ──"
        start_spark
        wait_for_livy
        trap 'echo ""; echo "── docker-teardown ──"; stop_spark || true' EXIT

        echo ""; echo "── integration-test ──"
        cd "${PROJECT_DIR}"
        SPARK_SKIP_DOCKER=1 uv run pytest integration_tests/test_dbt_fabricspark.py -vv --timeout=1800 -m integration

        # Reset dbt state between integration and e2e
        clean_dbt_state

        echo ""; echo "── e2e ──"
        SPARK_SKIP_DOCKER=1 bash integration_tests/scripts/run-dbt-local.sh

        echo ""; echo "=== All targets completed. ==="
        ;;
    *) "run_${TARGET//-/_}" ;;
esac
