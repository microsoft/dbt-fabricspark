#!/bin/bash
#
#
#       Common functions.
#
# ---------------------------------------------------------------------------------------
#

retry() {
    local retries=10
    local delay=30
    local rc=1
    local attempt=1

    while [[ $retries -ge $attempt && $rc -ne 0 ]]; do
        set +e
        "$@"
        rc=$?
        set -e

        if [[ $rc -ne 0 ]]; then
            echo "$@ rc is non-zero"
            if [[ $attempt -ne $retries ]]; then
                sleep "$delay"
            fi
            attempt=$((attempt + 1))
        fi
    done

    if [[ $rc -ne 0 ]]; then
        false
    else
        true
    fi
}
