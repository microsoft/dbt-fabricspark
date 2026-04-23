#!/usr/bin/env tsx
/**
 * CLI entry point: sync Copilot PR branches with main.
 *
 * Discovers Copilot PRs via `gh` CLI, checks if their branches are behind
 * main, and posts a comment telling the Copilot agent to merge and re-test.
 *
 * Usage:
 *   npx tsx tools/gh-automation/sync-main.ts              # watch mode (default)
 *   npx tsx tools/gh-automation/sync-main.ts --no-watch
 *   npx tsx tools/gh-automation/sync-main.ts --dry-run
 *   npx tsx tools/gh-automation/sync-main.ts 98            # specific PR
 */

import { Command } from 'commander';
import { Logger } from './core/logger.js';
import { GhClient } from './core/gh-client.js';
import { PullRequestService } from './core/pull-request-service.js';
import { SyncService } from './core/sync-service.js';
import { WatchRunner } from './core/watch-runner.js';
import type { GhAutomationConfig, PullRequest } from './core/types.js';

const program = new Command();

program
    .name('sync-main')
    .description('Sync Copilot PR branches with main by posting merge comments')
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
        const syncService = new SyncService(client, logger);
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
                logger.dryRun('Would check each Copilot PR branch against main and comment if behind.');
                return;
            }

            if (!prs || prs.length === 0) {
                logger.info('No PRs to process.');
                return;
            }

            syncService.syncAll(prs);
        });
    });

program.parse();
