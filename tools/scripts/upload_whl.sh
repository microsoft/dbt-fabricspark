#!/usr/bin/env bash
#
# Upload the built dbt-fabricspark wheel to Azure Blob Storage, overwriting
# any existing blob at the same path. Expects STORAGE_KEY in the environment
# (load from test.env first: `set -a; source test.env; set +a`).
#
# Mirrors .temp/privy/scripts/upload_whl.sh, pointed at this repo's wheel
# instead of privy's.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."

ACCOUNT_NAME="${PRIVY_STORAGE_ACCOUNT:-rakirahman}"
CONTAINER="${PRIVY_STORAGE_CONTAINER:-public}"
WHL_GLOB="${DBT_FABRICSPARK_WHL_GLOB:-dist/dbt_fabricspark-*-py3-none-any.whl}"

if [[ -z "${STORAGE_KEY:-}" ]]; then
  echo "STORAGE_KEY is not set. Run:  set -a; source test.env; set +a" >&2
  exit 1
fi

# shellcheck disable=SC2206 # intentional word-splitting to expand the glob
whls=($WHL_GLOB)
if [[ ! -f "${whls[0]:-}" ]]; then
  echo "wheel not found matching $WHL_GLOB — run 'uv build' first" >&2
  exit 1
fi
if [[ ${#whls[@]} -gt 1 ]]; then
  echo "multiple wheels match $WHL_GLOB, refusing to guess: ${whls[*]}" >&2
  exit 1
fi
WHL_PATH="${whls[0]}"
BLOB_NAME="${DBT_FABRICSPARK_BLOB_NAME:-whls/$(basename "$WHL_PATH")}"

echo ">> uploading $WHL_PATH → https://${ACCOUNT_NAME}.blob.core.windows.net/${CONTAINER}/${BLOB_NAME}"
az storage blob upload \
  --account-name "$ACCOUNT_NAME" \
  --account-key "$STORAGE_KEY" \
  --container-name "$CONTAINER" \
  --name "$BLOB_NAME" \
  --file "$WHL_PATH" \
  --overwrite \
  --only-show-errors

echo ">> done: https://${ACCOUNT_NAME}.blob.core.windows.net/${CONTAINER}/${BLOB_NAME}"
