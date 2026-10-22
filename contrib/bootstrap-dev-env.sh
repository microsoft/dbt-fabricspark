#!/bin/bash
#
#
#       Bootstraps a Linux Ubuntu host for the VS Code devcontainer idempotently,
#       with the minimal set of dependencies.
#
#       If your host restarts, rerun this script.
#
# ---------------------------------------------------------------------------------------
#

set -eo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)

ACR_NAME="dbtfabric"
ACR_URL="${ACR_NAME}.azurecr.io"
DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"

command -v jq &>/dev/null || PACKAGES="jq"
command -v gh &>/dev/null || PACKAGES="$PACKAGES gh"
command -v wslu &>/dev/null || PACKAGES="$PACKAGES wslu"
[ -n "$PACKAGES" ] && sudo apt-get update -qq && sudo apt-get install -yqq $PACKAGES

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
fi

sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json > /dev/null <<'EOF'
{
  "max-concurrent-downloads": 32,
  "max-concurrent-uploads": 32,
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 1048576, "Soft": 1048576 },
    "nproc":  { "Name": "nproc",  "Hard": 1048576, "Soft": 1048576 },
    "memlock": { "Name": "memlock", "Hard": -1, "Soft": -1 }
  },
  "features": { "buildkit": true },
  "log-driver": "json-file",
  "log-opts": { "max-size": "50m", "max-file": "3" }
}
EOF

echo "docker is installed, restarting..."
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
sudo usermod -aG docker "$USER"
docker container ls
docker ps -q | xargs -r docker kill

if grep -q "$ACR_URL" ~/.docker/config.json 2>/dev/null; then
     echo "Already logged in to ${ACR_URL}"
elif [ -z "$ACR_PASSWORD" ]; then
    echo "ACR_PASSWORD is not set, skipping docker login; only devcontainer image publishers need it"
else
    echo "$ACR_PASSWORD" | docker login "$ACR_URL" --username "$ACR_NAME" --password-stdin
fi
docker pull "$(jq -r .image "$REPO_ROOT/.devcontainer/devcontainer.json")"

export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
if ! [ -x "$(command -v npm)" ]; then
  echo "Installing Node.js and npm for WSL..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get update 2>&1 > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs
else
  echo "WSL npm is available."
fi

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
    # prompt=none lets the signed-in Windows browser complete the login with single sign-on, without clicks.
    # Login experience v2 off skips the subscription picker that waits for input in a terminal.
    BROWSER='bash -c "exec wslview \"${0/prompt=select_account/prompt=none}\"" %s' AZURE_CORE_LOGIN_EXPERIENCE_V2=off \
        az login --output none
fi

cd "$REPO_ROOT"
sudo npm install
sudo chmod -R 777 ${REPO_ROOT}/node_modules

echo "Docker: $(docker --version)"
echo "npm: $(npm version)"
echo "nx: $(npx nx --version)"
echo "devcontainer: $(npx devcontainer --version)"