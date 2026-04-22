#!/usr/bin/env bash

#
# Lifted from https://github.com/pypa/hatch/tree/install with the following
# changes:
#

VERSION="1.13.0" # "latest"
INSTALL_PATH="/usr/local/bin"
RUNNER_OS="Linux"
RUNNER_ARCH="X64"

set -euo pipefail
IFS=$'\n\t'

PURPLE="\033[1;35m"
RESET="\033[0m"

# -- install_hatch --
install_hatch() {
    mkdir -p "${INSTALL_PATH}"
    archive="${INSTALL_PATH}/$1"

    echo -e "${PURPLE}Downloading Hatch ${VERSION}${RESET}\n"
    if [[ "${VERSION}" == "latest" ]]; then
        curl -sSLo "${archive}" "https://github.com/pypa/hatch/releases/latest/download/$1"
    else
        curl -sSLo "${archive}" "https://github.com/pypa/hatch/releases/download/hatch-v${VERSION}/$1"
    fi

    tar -xzf "${archive}" -C "${INSTALL_PATH}"
    rm "${archive}"

    echo -e "${PURPLE}Installing Hatch ${VERSION}${RESET}"
    "${INSTALL_PATH}/hatch" --version
    "${INSTALL_PATH}/hatch" self cache dist --remove
}

# X64
install_hatch "hatch-x86_64-unknown-linux-gnu.tar.gz"
curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="${INSTALL_PATH}" sh