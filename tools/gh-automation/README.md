# Github automation

GitHub Action automations for repo maintenance.

Discovers Copilot PRs via `gh` CLI, then re-runs any `action_required` workflow
runs using `POST /actions/runs/{id}/rerun` — this is what the GitHub UI's
"Approve and run" button does under the hood.

## Quick start

```bash
# Long-running watch loop — approves workflows every 60s (default)
npx tsx tools/gh-automation/approve-workflow.ts
```

## Examples

```bash
# Watch a specific PR
npx tsx tools/gh-automation/approve-workflow.ts 98

# One-shot: approve all Copilot PRs, then exit
npx tsx tools/gh-automation/approve-workflow.ts --no-watch

# One-shot: specific PR
npx tsx tools/gh-automation/approve-workflow.ts 98 --no-watch

# Dry run — see what would happen without approving anything
npx tsx tools/gh-automation/approve-workflow.ts --dry-run

# Dry run + specific PR
npx tsx tools/gh-automation/approve-workflow.ts 98 --dry-run --no-watch

# Different repo
npx tsx tools/gh-automation/approve-workflow.ts --repo myorg/myrepo
```

## npm script

```bash
# Runs in watch mode by default
npm run approve-copilot-gh-workflow

# Pass flags after --
npm run approve-copilot-gh-workflow -- 98 --no-watch --dry-run
```

## Flags

| Flag                     | Default                     | Description                     |
| ------------------------ | --------------------------- | ------------------------------- |
| `[pr-number]`            | _(all Copilot PRs)_         | Target a specific PR            |
| `--watch` / `--no-watch` | `--watch`                   | Reconcile loop every 60s        |
| `--dry-run`              | `false`                     | Print actions without executing |
| `--repo <owner/repo>`    | `microsoft/dbt-fabricspark` | Target repository               |

## How it works

1. `gh pr list` discovers open Copilot PRs (author: `copilot-swe-agent`)
2. `gh api` fetches workflow runs per branch, filters `conclusion === "action_required"`
3. `gh api --method POST .../actions/runs/{id}/rerun` re-runs them as you (authorized user)
4. In watch mode, repeats every 60 seconds