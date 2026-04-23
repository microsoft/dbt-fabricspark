#!/usr/bin/env tsx
/**
 * CLI entry point: nudge Copilot agents on failed CI runs.
 *
 * Discovers Copilot PRs via `gh` CLI, checks for failed/cancelled/timed-out
 * workflow runs, and posts a comment telling the agent to read the logs and fix.
 *
 * Usage:
 *   npx tsx tools/gh-automation/nudge.ts              # watch mode (default)
 *   npx tsx tools/gh-automation/nudge.ts --no-watch
 *   npx tsx tools/gh-automation/nudge.ts --dry-run
 *   npx tsx tools/gh-automation/nudge.ts 98            # specific PR
 */

import { Command } from 'commander';
import { Logger } from './core/logger.js';
import { GhClient } from './core/gh-client.js';
import { PullRequestService } from './core/pull-request-service.js';
import { NudgeService } from './core/nudge-service.js';
import { WatchRunner } from './core/watch-runner.js';
import type { GhAutomationConfig, PullRequest } from './core/types.js';

const program = new Command();

program
    .name('nudge')
    .description('Nudge Copilot agents to fix failed CI runs')
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
        const nudgeService = new NudgeService(client, logger);
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
                logger.dryRun('Would check each Copilot PR for failed CI runs and nudge.');
                return;
            }

            if (!prs || prs.length === 0) {
                logger.info('No PRs to process.');
                return;
            }

            nudgeService.nudgeAll(prs);
        });
    });

program.parse();
