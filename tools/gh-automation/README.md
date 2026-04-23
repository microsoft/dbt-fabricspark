# Github automation

GitHub Action automations for repo maintenance. All tools discover open Copilot PRs (author: `copilot-swe-agent`) and run in a reconcile loop by default.

| Tool | What it does |
|------|-------------|
| **approve-workflow** | Re-runs `action_required` workflow runs (GitHub UI's "Approve and run") |
| **sync-main** | Comments on PRs whose branches are behind `main`, telling Copilot to merge and re-test |
| **nudge** | Detects failed/cancelled/timed-out CI runs and comments telling Copilot to read logs, fix, and retry |

## Usage

```bash
# Watch mode (reconcile every 60s) — via npm
npm run approve-copilot-gh-workflow
npm run sync-copilot-main
npm run nudge-copilot

# Or directly
npx tsx tools/gh-automation/approve-workflow.ts
npx tsx tools/gh-automation/sync-main.ts
npx tsx tools/gh-automation/nudge.ts

# One-shot, dry-run, specific PR — flags work on all tools
npx tsx tools/gh-automation/nudge.ts 98 --no-watch --dry-run
```

## Flags

All tools share the same interface:

| Flag                     | Default                     | Description                     |
| ------------------------ | --------------------------- | ------------------------------- |
| `[pr-number]`            | _(all Copilot PRs)_         | Target a specific PR            |
| `--watch` / `--no-watch` | `--watch`                   | Reconcile loop every 60s        |
| `--dry-run`              | `false`                     | Print actions without executing |
| `--repo <owner/repo>`    | `microsoft/dbt-fabricspark` | Target repository               |