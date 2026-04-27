#!/usr/bin/env tsx
/**
 * CLI entry point: compact open Dependabot PRs into a single patch file.
 *
 * Fetches diffs from all open Dependabot PRs and writes them to a local
 * patch file for review and manual application.
 *
 * Usage:
 *   npx tsx tools/gh-automation/compact-dependabot.ts
 *   npx tsx tools/gh-automation/compact-dependabot.ts --dry-run
 *   npx tsx tools/gh-automation/compact-dependabot.ts --output my-patch.patch
 */

import { Command } from 'commander';
import { Logger } from './core/logger.js';
import { GhClient } from './core/gh-client.js';
import { PullRequestService } from './core/pull-request-service.js';
import { DependabotService } from './core/dependabot-service.js';

interface CompactDependabotOptions {
    readonly repo: string;
    readonly dryRun: boolean;
    readonly output: string;
    // Required by GhAutomationConfig shape used in GhClient
    readonly watch: boolean;
    readonly sleep: string;
}

const program = new Command();

program
    .name('compact-dependabot')
    .description('Fetch open Dependabot PR diffs and write a combined patch file')
    .option('--dry-run', 'Print actions without executing them', false)
    .option('--repo <owner/repo>', 'GitHub repository', 'microsoft/dbt-fabricspark')
    .option('--output <path>', 'Output patch file path', 'dependabot-diffs.patch')
    .action((opts: CompactDependabotOptions) => {
        const logger = new Logger();
        const client = new GhClient(opts, logger);
        const prService = new PullRequestService(client);
        const dependabotService = new DependabotService(client, prService, logger);

        dependabotService.compact(opts.output);
    });

program.parse();
