#!/usr/bin/env tsx
/**
 * CLI entry point: approve pending workflow runs on Copilot-authored PRs.
 *
 * Discovers PRs via `gh` CLI, then re-runs any `action_required` workflow runs
 * using `POST /actions/runs/{id}/rerun` — this is what the GitHub UI's
 * "Approve and run" button does under the hood.
 *
 * Usage:
 *   npx tsx tools/gh-automation/approve-workflow.ts           # watch mode (default)
 *   npx tsx tools/gh-automation/approve-workflow.ts --no-watch
 *   npx tsx tools/gh-automation/approve-workflow.ts --dry-run
 *   npx tsx tools/gh-automation/approve-workflow.ts 98        # specific PR
 */

import { Command } from 'commander';
import { Logger } from './core/logger.js';
import { GhClient } from './core/gh-client.js';
import { PullRequestService } from './core/pull-request-service.js';
import { WorkflowService } from './core/workflow-service.js';
import { WatchRunner } from './core/watch-runner.js';
import type { GhAutomationConfig, PullRequest } from './core/types.js';

const program = new Command();

program
    .name('approve-workflow')
    .description('Approve pending workflow runs on Copilot-authored PRs')
    .argument('[pr-number]', 'Specific PR number (omit to process all Copilot PRs)')
    .option('--dry-run', 'Print actions without executing them', false)
    .option('--watch', 'Run in an infinite reconcile loop (default: true)', true)
    .option('--no-watch', 'Run once and exit')
    .option('--sleep <seconds>', 'Seconds between reconcile loops', '60')
    .option('--repo <owner/repo>', 'GitHub repository', 'microsoft/dbt-fabricspark')
    .action(async (prNumber: string | undefined, opts: GhAutomationConfig) => {
        const logger = new Logger();
        const client = new GhClient(opts, logger);
        const prService = new PullRequestService(client);
        const workflowService = new WorkflowService(client, logger);
        const runner = new WatchRunner(opts.watch, logger, Number(opts.sleep));

        await runner.run(() => {
            let prs: PullRequest[] | null;

            if (prNumber !== undefined) {
                const pr = prService.getByNumber(prNumber);
                prs = pr ? [pr] : null;
            } else {
                prs = prService.listCopilotPRs();
            }

            if (client.isDryRun && prs === null) {
                logger.dryRun('Would fetch workflow runs for each PR and approve action_required runs.');
                return;
            }

            if (!prs || prs.length === 0) {
                logger.info('No PRs to process.');
                return;
            }

            const results = workflowService.approveAll(prs);
            const totalApproved = results.reduce((sum, r) => sum + r.approved, 0);
            logger.summary(totalApproved);
        });
    });

program.parse();
