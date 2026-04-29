# Github automation

GitHub Action automations for repo maintenance. All tools discover open Copilot PRs (author: `copilot-swe-agent`) and run in a reconcile loop by default.

| Tool                   | What it does                                                                                         |
| ---------------------- | ---------------------------------------------------------------------------------------------------- |
| **approve-workflow**   | Re-runs `action_required` workflow runs (GitHub UI's "Approve and run")                              |
| **sync-main**          | Comments on PRs whose branches are behind `main`, telling Copilot to merge and re-test               |
| **nudge**              | Detects failed/cancelled/timed-out CI runs and comments telling Copilot to read logs, fix, and retry |
| **compact-dependabot** | Fetches diffs from open Dependabot PRs and writes a combined patch file for review and application   |
| **get-version-diffs**  | Generates a changelog of PR descriptions between the last version bump and HEAD of main              |

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
npx tsx tools/gh-automation/compact-dependabot.ts

# One-shot, dry-run, specific PR — flags work on all tools
npx tsx tools/gh-automation/nudge.ts 98 --no-watch --dry-run

# Compact dependabot (always one-shot, no watch mode)
npm run compact-dependabot
npm run compact-dependabot -- --dry-run
npm run compact-dependabot -- --output my-patch.patch

# Generate version changelog (one-shot)
npm run get-version-diffs
npm run get-version-diffs -- --dry-run
npm run get-version-diffs -- --output custom-changelog.md
```

## Flags

All tools share the same interface:

| Flag                     | Default                     | Description                     |
| ------------------------ | --------------------------- | ------------------------------- |
| `[pr-number]`            | _(all Copilot PRs)_         | Target a specific PR            |
| `--watch` / `--no-watch` | `--watch`                   | Reconcile loop                  |
| `--sleep <seconds>`      | `60`                        | Seconds between reconcile loops |
| `--dry-run`              | `false`                     | Print actions without executing |
| `--repo <owner/repo>`    | `microsoft/dbt-fabricspark` | Target repository               |
