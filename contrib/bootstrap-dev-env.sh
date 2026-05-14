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

REPO_ROOT=$(git rev-parse --show-toplevel)

ACR_NAME="dbtfabric"
ACR_URL="${ACR_NAME}.azurecr.io"
DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"

command -v jq &>/dev/null || PACKAGES="jq"
command -v gh &>/dev/null || PACKAGES="$PACKAGES gh"
[ -n "$PACKAGES" ] && sudo apt-get update -qq && sudo apt-get install -yqq $PACKAGES

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
else
  echo "docker is already installed."
fi

sudo chmod 666 /var/run/docker.sock
docker container ls
docker ps -q | xargs -r docker kill

if grep -q "$ACR_URL" ~/.docker/config.json 2>/dev/null; then
     echo "Already logged in to ${ACR_URL}"
 else
    if [ -n "$ACR_PASSWORD" ]; then
        docker_password="$ACR_PASSWORD"
    else
        read -sp "If you are a Microsoft Employee and you plan on contributing to the Devcontainer image, please ping @mdrrahman and enter Docker Admin password for ${ACR_URL} - otherwise, leave blank and press [ENTER]: " docker_password
        echo
    fi
    if [ -z "$docker_password" ]; then
        echo "You left the password empty, skipping docker login"
    else
        echo "$docker_password" | docker login "$ACR_URL" --username "$ACR_NAME" --password-stdin
    fi
fi

export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
if ! [ -x "$(command -v npm)" ]; then
  echo "Installing Node.js and npm for WSL..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get update 2>&1 > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs
else
  echo "WSL npm is available."
fi

AZ_PATH=$(which az 2>/dev/null)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi
az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

cd "$REPO_ROOT"
sudo npm install
sudo chmod -R 777 ${REPO_ROOT}/node_modules

echo "Docker: $(docker --version)"
echo "npm: $(npm version)"
echo "nx: $(npx nx --version)"
echo "devcontainer: $(npx devcontainer --version)"