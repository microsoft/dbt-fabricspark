# Contributing

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

2. Clone the repo, and open VSCode in it:

   ```bash
   cd ~/

   read -p "Enter your name (e.g. 'FirstName LastName'): " user_name
   read -p "Enter your email (e.g. 'your-alias@foo.com'): " user_email
   read -p "Enter your git fork (e.g. 'https://github.com/<your-git-alias>/dbt-fabricspark.git'): " git_fork_url
   read -p "Enter the existing branch to switch to: (e.g. 'main'): " branch_name
   
   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   git clone "$git_fork_url"

   cd dbt-fabricspark/
   git checkout "$branch_name"

   code .
   ```

3. Run the bootstrapper script, that installs all tools idempotently:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x ${GIT_ROOT}/contrib/bootstrap-dev-env.sh && ${GIT_ROOT}/contrib/bootstrap-dev-env.sh
   ```

4. Source the path to apply environment changes:

   ```bash
   source ~/.bashrc
   ```

5. Dev loop (`TODO` get rid of this and replace with run.sh):

   ```bash
   # Build wheel
   rm -rf /home/mdrrahman/dbt-fabricspark/dist
   uv build

   # Run unit tests
   uv run pytest -v
   ```