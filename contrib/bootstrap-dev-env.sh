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
set -euo pipefail

export REPO_ROOT=$(git rev-parse --show-toplevel)
export DEBIAN_FRONTEND=noninteractive
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')

DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"
PACKAGES=""
command -v python &>/dev/null || PACKAGES="python3 python-is-python3 python3-venv"
command -v pip &>/dev/null || PACKAGES="$PACKAGES python3-pip"
command -v curl &>/dev/null || PACKAGES="$PACKAGES curl"
command -v gh &>/dev/null || PACKAGES="$PACKAGES gh"

[ -n "$PACKAGES" ] && sudo apt-get update -qq && sudo apt-get install -yqq $PACKAGES

command -v uv &>/dev/null || { curl -LsSf https://astral.sh/uv/install.sh | sh; source "$HOME/.local/bin/env" 2>/dev/null || true; }

AZ_PATH=$(which az 2>/dev/null || true)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi
if ! az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
fi

sudo mkdir -p /etc/docker
echo '{"max-concurrent-downloads": 32}' | sudo tee /etc/docker/daemon.json > /dev/null

echo "docker is installed, restarting..."
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
docker container ls
docker ps -q | xargs -r docker kill

[[ ":$PATH:" != *":$HOME/.local/bin:"* ]] && export PATH="$HOME/.local/bin:$PATH"

cd "$REPO_ROOT"
[ ! -d "$REPO_ROOT/.venv" ] && uv venv "$REPO_ROOT/.venv"
source "$REPO_ROOT/.venv/bin/activate"
uv pip install -e . --group dev
[ ! -f "$REPO_ROOT/test.env" ] && cp "$REPO_ROOT/test.env.example" "$REPO_ROOT/test.env"

code --install-extension donjayamanne.python-extension-pack

echo "Done. Python: $(python --version), uv: $(uv --version), az: $(az version -o tsv 2>/dev/null | head -1), docker: $(docker --version)"
