# Github automation

GitHub Action automations for repo maintenance.

## Tools

### approve-workflow

Discovers Copilot PRs via `gh` CLI, then re-runs any `action_required` workflow
runs using `POST /actions/runs/{id}/rerun` — this is what the GitHub UI's
"Approve and run" button does under the hood.

### sync-main

Discovers Copilot PRs via `gh` CLI, checks if their branches are behind `main`,
and posts a comment telling the Copilot agent to merge main and re-test.

## Quick start

```bash
# Approve workflows — watch loop every 60s (default)
npx tsx tools/gh-automation/approve-workflow.ts

# Sync branches with main — watch loop every 60s (default)
npx tsx tools/gh-automation/sync-main.ts
```

## Examples

### approve-workflow

```bash
# Watch a specific PR
npx tsx tools/gh-automation/approve-workflow.ts 98 --sleep 360

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

### sync-main

```bash
# Watch a specific PR
npx tsx tools/gh-automation/sync-main.ts 98 --sleep 360

# One-shot: sync all Copilot PRs, then exit
npx tsx tools/gh-automation/sync-main.ts --no-watch

# One-shot: specific PR
npx tsx tools/gh-automation/sync-main.ts 98 --no-watch

# Dry run — see what would happen without commenting
npx tsx tools/gh-automation/sync-main.ts --dry-run

# Dry run + specific PR
npx tsx tools/gh-automation/sync-main.ts 98 --dry-run --no-watch

# Different repo
npx tsx tools/gh-automation/sync-main.ts --repo myorg/myrepo
```

## npm scripts

```bash
# approve-workflow — runs in watch mode by default
npm run approve-copilot-gh-workflow

# sync-main — runs in watch mode by default
npm run sync-copilot-main

# Pass flags after --
npm run approve-copilot-gh-workflow -- 98 --no-watch --dry-run
npm run sync-copilot-main -- 98 --no-watch --dry-run
```

## Flags

Both tools share the same flag interface:

| Flag                     | Default                     | Description                     |
| ------------------------ | --------------------------- | ------------------------------- |
| `[pr-number]`            | _(all Copilot PRs)_         | Target a specific PR            |
| `--watch` / `--no-watch` | `--watch`                   | Reconcile loop every 60s        |
| `--dry-run`              | `false`                     | Print actions without executing |
| `--repo <owner/repo>`    | `microsoft/dbt-fabricspark` | Target repository               |

## How it works

### approve-workflow

1. `gh pr list` discovers open Copilot PRs (author: `copilot-swe-agent`)
2. `gh api` fetches workflow runs per branch, filters `conclusion === "action_required"`
3. `gh api --method POST .../actions/runs/{id}/rerun` re-runs them as you (authorized user)
4. In watch mode, repeats every 60 seconds

### sync-main

1. `gh pr list` discovers open Copilot PRs (author: `copilot-swe-agent`)
2. `gh api repos/{owner}/{repo}/compare/main...{branch}` checks if the branch is behind main
3. If behind, `gh pr comment` posts a message telling the Copilot agent to merge and re-test
4. In watch mode, repeats every 60 seconds