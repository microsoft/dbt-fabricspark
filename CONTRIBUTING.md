# Contributing

[![Deep-dive Walkthrough](https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-fabricspark-contrib.png)](https://rakirahman.blob.core.windows.net/public/videos/dbt-fabricspark-local-development-deep-dive.mp4)

If you're a windows user, we only use windows to get into WSL - everything is Linux from there.

Spark and Livy are notoriously hard to run on Windows.
Therefore, the development environment we support is Linux, using [VSCode Devcontainer](https://code.visualstudio.com/docs/devcontainers/containers).
This repo's CI runs the exact same devcontainer.

Therefore, if the tests pass locally, they are highly likely to pass in CI as well.

## How to use, on a Linux machine

1. Windows pre-reqs

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   ```

1. Clone your fork on Windows, then check out and push the branch you want to work on:

   ```powershell
   git clone https://github.com/<your-github-user>/dbt-fabricspark.git
   ```

1. From an **elevated PowerShell 7** prompt in the checkout, install [Task](https://taskfile.dev) and run [`contrib/bootstrap-dev-env.yaml`](contrib/bootstrap-dev-env.yaml):

   > ⚠️ This **deletes every WSL distribution** on the machine before recreating `Ubuntu-24.04`. Push any work inside WSL first.

   ```powershell
   winget install Task.Task
   $env:PATH = [Environment]::GetEnvironmentVariable('PATH', 'Machine') + ';' + [Environment]::GetEnvironmentVariable('PATH', 'User')
   task --taskfile contrib/bootstrap-dev-env.yaml
   ```

   Without any prompts, this recreates WSL on the drive with the most free space and clones your current branch into `/workspaces/dbt-fabricspark`. It then runs [`contrib/bootstrap-dev-env.sh`](contrib/bootstrap-dev-env.sh), which signs in the Azure CLI through your signed-in browser with single sign-on (no clicks), then starts the devcontainer and opens it in VS Code. After a restart, run `task --taskfile contrib/bootstrap-dev-env.yaml up` instead; it keeps WSL and reopens the devcontainer.

1. Install recommended developer tooling (optional):

   ```bash
   curl -fsSL https://gh.io/copilot-install | bash
   $HOME/.local/bin/copilot --yolo
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

1. All build and tests should now run green:

   ```bash
   npx nx run dbt-fabricspark:build
   npx nx run dbt-fabricspark:test --output-style=stream
   ```