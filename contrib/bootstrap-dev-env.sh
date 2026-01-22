#!/bin/bash
#
#
#       Sets up a dev env with all pre-reqs. 
#
#       This script is idempotent, it will only attempt to install 
#       dependencies if not exists.
#
# ---------------------------------------------------------------------------------------
#
set -e

export REPO_ROOT=$(git rev-parse --show-toplevel)
export DEBIAN_FRONTEND=noninteractive

PACKAGES=""
command -v python &>/dev/null || PACKAGES="python3 python-is-python3 python3-venv"
command -v pip &>/dev/null || PACKAGES="$PACKAGES python3-pip"
command -v curl &>/dev/null || PACKAGES="$PACKAGES curl"

[ -n "$PACKAGES" ] && sudo apt-get update -qq && sudo apt-get install -yqq $PACKAGES

command -v uv &>/dev/null || { curl -LsSf https://astral.sh/uv/install.sh | sh; source "$HOME/.local/bin/env" 2>/dev/null || true; }
command -v az &>/dev/null || curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

[[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && export PATH="$HOME/.local/bin:$PATH"

cd "$REPO_ROOT"
uv pip install -e . --group dev
[ ! -f "$REPO_ROOT/test.env" ] && cp "$REPO_ROOT/test.env.example" "$REPO_ROOT/test.env"

code --install-extension donjayamanne.python-extension-pack

echo "Done. Python: $(python --version), uv: $(uv --version), az: $(az version -o tsv 2>/dev/null | head -1)"
