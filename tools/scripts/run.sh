#!/bin/bash
#
# dbt-fabricspark dev/test script. Requires Python 3.10+ and uv.
#
# Usage: tools/scripts/run.sh <target>
#
# Targets: venv | clean | lint | fix | build | test | publish | all
#
# Each target is idempotent — auto-creates the venv if missing.
# publish runs `twine check dist/*` unconditionally, and `uv publish`
# only when UV_PUBLISH_TOKEN is set (no-op otherwise).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"

declare -A TARGETS=(
    ["venv"]="Fresh venv + install deps"
    ["clean"]="Remove build artifacts"
    ["lint"]="ruff check + format --check"
    ["fix"]="ruff auto-fix + format"
    ["build"]="Build wheel to dist/ + twine check"
    ["test"]="pytest unit tests"
    ["publish"]="twine check + uv publish (no-op if UV_PUBLISH_TOKEN unset)"
    ["all"]="Run clean, lint, build, test, publish in sequence"
)
TARGET_ORDER=("venv" "clean" "lint" "fix" "build" "test" "publish")
ALL_ORDER=("clean" "lint" "build" "test" "publish")

print_usage() {
    echo
    printf "  %-20s %s\n" "TARGET" "DESCRIPTION"
    printf "  %-20s %s\n" "all" "${TARGETS[all]}"
    for t in "${TARGET_ORDER[@]}"; do printf "  %-20s %s\n" "$t" "${TARGETS[$t]}"; done
    echo
    echo "Usage: tools/scripts/run.sh <target>"
    echo
}

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then echo "ERROR: No target provided."; print_usage; exit 1; fi
if [[ "$TARGET" != "all" && -z "${TARGETS[$TARGET]:-}" ]]; then
    echo "ERROR: Unknown target '$TARGET'"; print_usage; exit 1
fi

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

run_venv()  { create_venv; }

run_clean() {
    cd "${PROJECT_DIR}"
    rm -rf dist/ build/ .pytest_cache .ruff_cache
    find . -type d -name '*.egg-info' -not -path './.venv/*' -not -path './node_modules/*' -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name '__pycache__' -not -path './.venv/*' -not -path './node_modules/*' -exec rm -rf {} + 2>/dev/null || true
    echo "  Cleaned build artifacts."
}

run_lint() {
    ensure_venv
    cd "${PROJECT_DIR}"
    uv run ruff check src/ tests/
    uv run ruff format --check src/ tests/
}

run_fix() {
    ensure_venv
    cd "${PROJECT_DIR}"
    uv run ruff check --fix src/ tests/
    uv run ruff format src/ tests/
}

run_build() {
    ensure_venv
    cd "${PROJECT_DIR}"
    rm -rf dist/
    uv build
    ls -lh dist/
    uv tool install twine >/dev/null
    uv tool run twine check dist/*
}

run_test() {
    ensure_venv
    cd "${PROJECT_DIR}"
    uv run pytest tests/unit -vv
}

run_publish() {
    ensure_venv
    cd "${PROJECT_DIR}"
    if [[ ! -d dist ]] || [[ -z "$(ls -A dist/ 2>/dev/null)" ]]; then
        echo "  dist/ is empty — running build first..."
        run_build
    fi
    uv tool install twine >/dev/null
    uv tool run twine check dist/*
    if [[ -n "${UV_PUBLISH_TOKEN:-}" ]]; then
        echo "  UV_PUBLISH_TOKEN detected — publishing to PyPI..."
        uv publish
    else
        echo "  UV_PUBLISH_TOKEN not set — skipping uv publish (no-op)."
    fi
}

echo "=== dbt-fabricspark: ${TARGET} ==="

case "$TARGET" in
    "all")
        for t in "${ALL_ORDER[@]}"; do
            echo ""; echo "── ${t} ──"
            "run_${t}"
        done
        echo ""; echo "=== All targets completed. ==="
        ;;
    *) "run_${TARGET}" ;;
esac
