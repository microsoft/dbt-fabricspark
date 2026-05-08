# Contributing

[![Deep-dive Walkthrough](https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-fabricspark-contrib.png)](https://rakirahman.blob.core.windows.net/public/videos/dbt-fabricspark-local-development-deep-dive.mp4)


## How to use, on a Linux machine

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

1. Get a fresh new WSL machine up:

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\contrib\bootstrap-dev-env.ps1"
   ```

1. Clone the repo, and open VSCode in it:

   ```bash
   sudo mkdir -p /workspaces && sudo chmod 777 /workspaces && cd /workspaces

   read -p "Enter your name (e.g. 'FirstName LastName'): " user_name
   read -p "Enter your email (e.g. 'your-alias@foo.com'): " user_email
   read -p "Enter your git fork (e.g. 'https://github.com/microsoft/dbt-fabricspark.git'): " git_fork_url
   read -p "Enter the existing branch to switch to: (e.g. 'main'): " branch_name
   
   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   git clone "$git_fork_url"

   cd /workspaces/dbt-fabricspark
   git checkout "$branch_name"

   code .
   ```

1. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

1. Launch the devcontainer:

   ```bash
   cd /workspaces/dbt-fabricspark
   HEX=$(printf '%s' "$(wslpath -w .)" | xxd -ps -c 256)
   code --folder-uri "vscode-remote://dev-container+${HEX}/workspaces/dbt-fabricspark"
   ```

1. Install recommended developer tooling (optional):

  ```bash
  curl -fsSL https://gh.io/copilot-install | bash
  $HOME/.local/bin/copilot -i /login
  ```

1. Login to github and ensure to authorize `Microsoft` if you're an employee (optional):

   ```bash
   gh auth login
   ```

1. Create a `test.env` file with the two Fabric workspace IDs and display names — you need `Contributor` on both. The functional test suite uses **two** workspaces:

    - **Workspace 1 (`WORKSPACE_ID_1` / `WORKSPACE_NAME_1`)** — the *primary* workspace where dbt models are materialized during tests.
    - **Workspace 2 (`WORKSPACE_ID_2` / `WORKSPACE_NAME_2`)** — the *secondary* workspace that hosts a seeded fixture lakehouse, used to verify cross-workspace 4-part naming end-to-end.

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   read -p "Enter Fabric workspace 1 ID: " ws1_id
   read -p "Enter Fabric workspace 1 display name: " ws1_name
   read -p "Enter Fabric workspace 2 ID: " ws2_id
   read -p "Enter Fabric workspace 2 display name: " ws2_name
   {
       echo "WORKSPACE_ID_1=${ws1_id}"
       echo "WORKSPACE_NAME_1=${ws1_name}"
       echo "WORKSPACE_ID_2=${ws2_id}"
       echo "WORKSPACE_NAME_2=${ws2_name}"
   } > "${GIT_ROOT}/test.env"
   ```